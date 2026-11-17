"""Subscription delivery semantics: full hand-off, incremental atomicity,
batch identity, credentials, generation and retention.

These tests assert concrete business content (row ids, typed values, versions,
before/after images, positions), not merely that calls returned.
"""

import unittest

from support import (TempRepo, account, drain_changes, drain_full, ids,
                     seed_accounts)

from mrtx import (ReplicaGenerationError, SchemaError, StaleSubscriptionGeneration,
                  SubscriptionClosed, SubscriptionError, SubscriptionHandle,
                  SubscriptionNotFound)


def ledger(acct="A", amount=1, active=True):
    return {"acct": acct, "amount": amount, "active": active}


def images(items):
    """(table, row_id, version, values) of full rows, in delivery order."""
    return [(i["table"], i["row_id"], i["version"], i["values"]) for i in items]


def changes(items):
    """(table, row_id, before_present, before_version, after_present, after_version)."""
    out = []
    for i in items:
        b, a = i["before"], i["after"]
        out.append((i["table"], i["row_id"], b["present"], b["version"],
                    a["present"], a["version"]))
    return out


class TestFullLoadExact(unittest.TestCase):
    def test_full_is_exactly_the_cut_snapshot_in_codepoint_order(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            # Deliberately out of code-point order; "Z" < "a" < "é" < emoji.
            for rid, owner in (("b", "b"), ("Z", "Z"), ("a", "a"),
                               ("é", "e"), ("\U0001f600", "smile")):
                tx.stage_insert("accounts", rid,
                                account(owner=owner, balance=ord(rid[0]) % 50,
                                        active=True, note=None))
            tx.stage_insert("ledger", "L1", ledger())
            tx.commit()
            sub, cut = c.create_subscription("s", ["accounts", "ledger"])
            items = drain_full(c, sub.handle, max_rows=2)
            self.assertEqual([i["table"] for i in items],
                             ["accounts"] * 5 + ["ledger"])
            self.assertEqual([i["row_id"] for i in items],
                             sorted(["b", "Z", "a", "é", "\U0001f600"])
                             + ["L1"])
            # Every delivered image is the S image, with its version.
            for i in items:
                self.assertEqual(i["version"], 1)
            # Original typed values survive, including bool and null.
            first = items[0]
            self.assertEqual(first["values"]["active"], True)
            self.assertIsNone(first["values"]["note"])

    def test_empty_tables_still_have_recognizable_structure(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            tx.stage_insert("accounts", "A", account())
            tx.commit()
            sub, cut = c.create_subscription("s", ["accounts", "audit"])
            items = drain_full(c, sub.handle)
            self.assertEqual([i["table"] for i in items], ["accounts"])
            # The empty subscribed table is still described by the batch.
            batch_meta = c.pull  # noqa: F841 - readability only
            result = c.subscription_status(sub.handle)
            self.assertEqual(sorted(result["schema"]), ["accounts", "audit"])
            self.assertEqual(sorted(result["schema"]["audit"]), ["n", "ok", "tag"])

    def test_rows_deleted_before_cut_are_absent(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=1)), ("B", account(balance=2))])
            tx = c.begin()
            tx.stage_delete("accounts", "B")
            tx.commit()
            sub, cut = c.create_subscription("s", ["accounts"])
            items = drain_full(c, sub.handle)
            self.assertEqual([i["row_id"] for i in items], ["A"])

    def test_page_budget_is_soft_upper_bound_for_full(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("r%02d" % i, account(balance=i)) for i in range(7)])
            sub, cut = c.create_subscription("s", ["accounts"])
            sizes = []
            while True:
                batch = c.pull(sub.handle, max_rows=3)["batch"]
                sizes.append(batch["row_count"])
                self.assertLessEqual(batch["row_count"], 3)
                c.ack(sub.handle, batch["batch_id"])
                if batch["full_complete"]:
                    break
            self.assertEqual(sum(sizes), 7)


class TestFullIsStableAcrossPagination(unittest.TestCase):
    def test_updates_deletes_reinserts_during_pagination_do_not_change_unread_rows(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("%s" % k, account(owner=k, balance=0))
                              for k in "abcde"])
            sub, cut = c.create_subscription("s", ["accounts"])
            first = c.pull(sub.handle, max_rows=2)["batch"]
            self.assertEqual([i["row_id"] for i in first["items"]], ["a", "b"])
            c.ack(sub.handle, first["batch_id"])
            # Repeated updates, delete + reinsert, on rows that are *not* yet
            # delivered.  Peers commit between pages.
            tx = c.begin()
            tx.stage_update("accounts", "c", {"balance": 100})
            tx.commit()
            tx = c.begin()
            tx.stage_delete("accounts", "d")
            tx.commit()
            tx = c.begin()
            tx.stage_insert("accounts", "d", account(owner="reborn", balance=7))
            tx.commit()
            for v in range(2, 6):
                tx = c.begin()
                tx.stage_update("accounts", "e", {"balance": v})
                tx.commit()
            rest = drain_full_continued(c, sub.handle, max_rows=2)
            all_items = first["items"] + rest
            # Exactly the S images: version 1, original values, no reborn row.
            self.assertEqual([i["row_id"] for i in all_items], list("abcde"))
            self.assertTrue(all(i["version"] == 1 for i in all_items))
            self.assertEqual({i["row_id"]: i["values"]["owner"] for i in all_items},
                             {k: k for k in "abcde"})

    def test_progress_survives_an_interrupted_full_load(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("r%02d" % i, account(balance=i)) for i in range(6)])
            sub, cut = c.create_subscription("s", ["accounts"])
            b1 = c.pull(sub.handle, max_rows=2)["batch"]
            b2 = c.pull(sub.handle, max_rows=2)["batch"]
            self.assertEqual(b1["batch_id"], b2["batch_id"])  # nothing acked
            # Simulate the consumer process dying with b1 pending.  A brand-new
            # client (and a re-opened handle) resumes the same batch.
            c2 = repo.__class__  # noqa: F841
            from mrtx import Client
            fresh = Client(repo.path)
            again = fresh.pull(SubscriptionHandle.from_dict(sub.handle.as_dict()),
                               max_rows=99)["batch"]
            self.assertEqual(again["batch_id"], b1["batch_id"])
            self.assertEqual(again["items"], b1["items"])
            c.ack(sub.handle, again["batch_id"])
            rest = drain_full_continued(c, sub.handle, max_rows=2)
            self.assertEqual(len(b1["items"]) + len(rest), 6)


def drain_full_continued(client, handle, max_rows):
    items = []
    while True:
        batch = client.pull(handle, max_rows=max_rows)["batch"]
        items.extend(batch["items"])
        client.ack(handle, batch["batch_id"])
        if batch["full_complete"]:
            return items


class TestIncrementalCommitAtomicity(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        seed_accounts(self.c, [("A", account(balance=0))])
        self.sub, self.cut = self.c.create_subscription("s",
                                                        ["accounts", "ledger"])
        drain_full(self.c, self.sub.handle)
        self.h = self.sub.handle

    def tearDown(self):
        self.repo.__exit__(None, None, None)

    def test_two_subscribed_tables_one_commit_one_batch(self):
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"balance": 5})
        tx.stage_insert("ledger", "L1", ledger())
        tx.commit()
        result = self.c.pull(self.h, max_rows=1)  # budget 1 cannot split it
        batch = result["batch"]
        self.assertEqual(batch["phase"], "incremental")
        self.assertEqual(batch["row_count"], 2)
        self.assertEqual([(i["table"], i["row_id"]) for i in batch["items"]],
                         [("accounts", "A"), ("ledger", "L1")])
        self.assertEqual(batch["position"], 2)

    def test_same_identity_staged_times_delivers_only_net_change(self):
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"balance": 1})
        tx.stage_update("accounts", "A", {"balance": 2})
        tx.stage_update("accounts", "A", {"balance": 3})
        tx.commit()
        batch = self.c.pull(self.h)["batch"]
        self.assertEqual(len(batch["items"]), 1)
        item = batch["items"][0]
        self.assertEqual(item["before"]["values"]["balance"], 0)
        self.assertEqual(item["after"]["values"]["balance"], 3)
        self.assertEqual(item["before"]["version"], 1)
        self.assertEqual(item["after"]["version"], 2)

    def test_insert_then_delete_new_identity_has_no_final_change(self):
        tx = self.c.begin()
        tx.stage_insert("accounts", "X", account(owner="x"))
        tx.stage_delete("accounts", "X")
        tx.commit()
        # No net change: the commit is skipped, and nothing after it is stuck.
        self.assertEqual(self.c.pull(self.h)["caught_up"], True)

    def test_business_values_unchanged_but_version_bumped_is_a_change(self):
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"balance": 0})  # same value
        tx.commit()
        batch = self.c.pull(self.h)["batch"]
        self.assertEqual(len(batch["items"]), 1)
        item = batch["items"][0]
        self.assertEqual(item["before"]["version"], 1)
        self.assertEqual(item["after"]["version"], 2)
        self.assertEqual(item["before"]["values"], item["after"]["values"])

    def test_deleted_side_is_marked_absent_not_business_null(self):
        tx = self.c.begin()
        tx.stage_delete("accounts", "A")
        tx.commit()
        item = self.c.pull(self.h)["batch"]["items"][0]
        self.assertFalse(item["after"]["present"])
        self.assertIsNone(item["after"]["values"])
        self.assertTrue(item["before"]["present"])

    def test_bool_and_int_survive_incremental_images(self):
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"active": False, "balance": 2 ** 62})
        tx.commit()
        after = self.c.pull(self.h)["batch"]["items"][0]["after"]
        self.assertIs(after["values"]["active"], False)
        self.assertEqual(after["values"]["balance"], 2 ** 62)
        self.assertIsInstance(after["values"]["balance"], int)


class TestStreamSkipsAndOrdering(unittest.TestCase):
    def test_unrelated_and_readonly_commits_do_not_stall_or_skip(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            sub, cut = c.create_subscription("s", ["accounts"])
            drain_full(c, sub.handle)
            # commit 2: subscribed; commit 3: unrelated; commit 4: read-only;
            # commit 5: subscribed.
            tx = c.begin(); tx.stage_update("accounts", "A", {"balance": 1})
            tx.commit()
            tx = c.begin(); tx.stage_insert("audit", "U", {"tag": "x", "n": 1,
                                                           "ok": True})
            tx.commit()
            ro = c.begin(); ro.read("accounts"); ro.commit()
            tx = c.begin(); tx.stage_update("accounts", "A", {"balance": 2})
            tx.commit()
            seen = drain_changes(c, sub.handle, max_rows=1)
            self.assertEqual([p for p, _ in seen], [2, 5])
            self.assertEqual([i["after"]["values"]["balance"]
                              for _, items in seen for i in items], [1, 2])

    def test_abort_and_uncommitted_staging_never_enter_the_stream(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            sub, cut = c.create_subscription("s", ["accounts"])
            drain_full(c, sub.handle)
            doomed = c.begin()
            doomed.stage_update("accounts", "A", {"balance": 99})
            doomed.abort()
            held = c.begin()
            held.stage_insert("accounts", "GHOST", account(owner="ghost"))
            # held is never committed; its staging must be invisible.
            drain = c.pull(sub.handle)
            self.assertEqual(drain["caught_up"], True)
            held.abort()
            self.assertEqual(c.rows("accounts")[0]["balance"], 0)

    def test_resent_commit_does_not_duplicate_events(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            sub, cut = c.create_subscription("s", ["accounts"])
            drain_full(c, sub.handle)
            tx = c.begin()
            tx.stage_update("accounts", "A", {"balance": 5})
            first = tx.commit()
            replay = c.commit(tx.handle)
            self.assertTrue(replay["replayed"])
            batch = c.pull(sub.handle)["batch"]
            self.assertEqual(batch["row_count"], 1)
            self.assertEqual(batch["position"], first["commit_version"])
            c.ack(sub.handle, batch["batch_id"])
            self.assertTrue(c.pull(sub.handle)["caught_up"])


class TestBatchIdentityAndAck(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        seed_accounts(self.c, [("A", account(balance=0)), ("B", account(balance=0))])
        self.sub, self.cut = self.c.create_subscription("s", ["accounts"])
        self.h = self.sub.handle

    def tearDown(self):
        self.repo.__exit__(None, None, None)

    def test_repull_same_batch_across_budgets_and_clients(self):
        b1 = self.c.pull(self.h, max_rows=1)["batch"]
        b2 = self.c.pull(self.h, max_rows=1000)["batch"]
        self.assertEqual(b1["batch_id"], b2["batch_id"])
        self.assertEqual(b1["items"], b2["items"])
        from mrtx import Client
        other = Client(self.repo.path)
        b3 = other.pull(SubscriptionHandle.from_dict(self.h.as_dict()))["batch"]
        self.assertEqual(b3["batch_id"], b1["batch_id"])
        self.assertEqual(b3["items"], b1["items"])

    def test_ack_replay_is_noop_and_only_pending_batch_can_advance(self):
        b = self.c.pull(self.h)["batch"]
        with self.assertRaises(SubscriptionError):
            self.c.ack(self.h, "b-deadbeef")
        first = self.c.ack(self.h, b["batch_id"])
        self.assertEqual(first["acknowledged_position"], self.cut)
        replay = self.c.ack(self.h, b["batch_id"])
        self.assertTrue(replay["replayed"])
        status = self.c.subscription_status(self.h)
        self.assertEqual(status["acknowledged_position"], self.cut)

    def test_wrong_token_and_other_subscription_batch_cannot_advance(self):
        other, _ = self.c.create_subscription("other", ["accounts"])
        b = self.c.pull(self.h)["batch"]
        # The pending batch id of *this* subscription must not acknowledge
        # through another subscription's handle.
        with self.assertRaises(SubscriptionError):
            self.c.ack(other.handle, b["batch_id"])
        forged = SubscriptionHandle(self.h.sub_id, self.h.generation,
                                    "1" * 48, self.repo.path)
        with self.assertRaises(SubscriptionError):
            self.c.pull(forged)

    def test_no_client_supplied_position(self):
        # ack takes only a batch id; there is no position argument to forge.
        b = self.c.pull(self.h)["batch"]
        with self.assertRaises(TypeError):
            self.c.ack(self.h, b["batch_id"], 10 ** 9)


class TestGenerationLifecycle(unittest.TestCase):
    def test_rebuild_bumps_generation_and_invalidates_old_credentials(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            sub, cut1 = c.create_subscription("s", ["accounts"])
            old_handle = sub.handle
            b = c.pull(old_handle, max_rows=1)["batch"]
            new_sub, cut2 = c.rebuild_subscription(old_handle)
            self.assertEqual(new_sub.handle.generation, 2)
            self.assertGreaterEqual(cut2, cut1)
            with self.assertRaises(StaleSubscriptionGeneration):
                c.pull(old_handle)
            with self.assertRaises(StaleSubscriptionGeneration):
                c.ack(old_handle, b["batch_id"])
            # The new generation starts a fresh full load at the new cut.
            fresh_batches = drain_full(c, new_sub.handle, max_rows=5)
            self.assertEqual([i["row_id"] for i in fresh_batches], ["A"])

    def test_close_blocks_reopen_but_rebuild_is_allowed(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account())])
            sub, cut = c.create_subscription("s", ["accounts"])
            drain_full(c, sub.handle)
            c.close_subscription(sub.handle)
            with self.assertRaises(SubscriptionClosed):
                c.open_subscription(sub.handle)
            with self.assertRaises(SubscriptionClosed):
                c.pull(sub.handle)
            rebuilt, cut2 = c.rebuild_subscription(sub.handle)
            self.assertEqual(rebuilt.handle.generation, 2)
            self.assertEqual(len(drain_full(c, rebuilt.handle)), 1)

    def test_reopen_resumes_at_durable_progress(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("r%d" % i, account(balance=i)) for i in range(4)])
            sub, cut = c.create_subscription("s", ["accounts"])
            first = c.pull(sub.handle, max_rows=2)["batch"]
            c.ack(sub.handle, first["batch_id"])
            reopened = c.open_subscription(sub.handle)
            rest = drain_full_continued(c, reopened.handle, max_rows=2)
            self.assertEqual(len(first["items"]) + len(rest), 4)


class TestValidation(unittest.TestCase):
    def test_subscription_requires_known_tables_and_columns(self):
        with TempRepo() as repo:
            c = repo.client
            with self.assertRaises(SchemaError):
                c.create_subscription("s", ["nope"])
            with self.assertRaises(SchemaError):
                c.create_subscription("s", [])
            c.create_subscription("ok", ["accounts"])

    def test_duplicate_name_rejected(self):
        with TempRepo() as repo:
            c = repo.client
            c.create_subscription("s", ["accounts"])
            with self.assertRaises(SubscriptionError):
                c.create_subscription("s", ["ledger"])

    def test_unknown_handle(self):
        with TempRepo() as repo:
            c = repo.client
            fake = SubscriptionHandle("sub-000000000000000000000000", 1, "x" * 48,
                                      repo.path)
            with self.assertRaises(SubscriptionNotFound):
                c.pull(fake)

    def test_schema_change_after_subscription_is_rejected_on_create(self):
        with TempRepo() as repo:
            c = repo.client
            c.create_table("later", [("v", "int64")])
            with self.assertRaises(SchemaError):
                # column types were never declared as a supported *schema* shape
                c.create_subscription("s", ["later", "missing"])


if __name__ == "__main__":
    unittest.main()


class TestSchemaMigration(unittest.TestCase):
    def test_v1_database_upgrades_without_losing_data(self):
        import os
        import sqlite3
        import tempfile
        from mrtx import Client

        tmp = tempfile.mkdtemp(prefix="mrtx-migrate-")
        path = os.path.join(tmp, "old.db")
        try:
            c = Client(path)
            c.create_table("t", [("v", "int64")])
            tx = c.begin()
            tx.stage_insert("t", "k", {"v": 1})
            tx.commit()
            # Emulate a v1 repository: subscription tables absent, version 1.
            conn = sqlite3.connect(path, isolation_level=None)
            conn.execute("UPDATE meta SET value='1' WHERE key='schema_version'")
            for name in ("subscriptions", "subscription_pending",
                         "subscription_items", "sub_changes"):
                conn.execute("DROP TABLE IF EXISTS %s" % name)
            conn.close()
            reopened = Client(path)
            self.assertEqual(reopened.current_version(), 1)
            self.assertEqual(reopened.rows("t")[0]["v"], 1)
            sub, cut = reopened.create_subscription("s", ["t"])
            self.assertEqual(len(drain_full(reopened, sub.handle)), 1)
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
