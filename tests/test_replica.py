"""Replica consumer tests: readiness gating, atomic per-commit apply,
idempotent replay, crash between local apply and source ack, generation reset.

The final-state assertions compare every row's typed values and version
against an independently computed expectation, never by calling the product's
incremental generator.
"""

import os
import sqlite3
import unittest

from support import TempRepo, account, drain_full, seed_accounts

from mrtx import (Client, Replica, ReplicaGenerationError, ReplicaNotReady,
                  StaleSubscriptionGeneration, SubscriptionHandle)


def ledger(acct="A", amount=1, active=True):
    return {"acct": acct, "amount": amount, "active": active}


class ReplicaCase(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = repo_client = self.repo.client
        self.replica_path = os.path.join(self.repo.dir, "replica.db")
        seed_accounts(self.c, [("A", account(owner="a", balance=0)),
                               ("B", account(owner="b", balance=0))])
        self.sub, self.cut = self.c.create_subscription("s", ["accounts", "ledger"])
        self.replica = Replica(self.replica_path, self.sub.handle)

    def tearDown(self):
        self.repo.__exit__(None, None, None)

    def expected_rows(self):
        """Independently computed current source rows at its current version."""
        return {r["row_id"]: r for r in self.c.rows("accounts")}


class TestReadiness(ReplicaCase):
    def test_business_reads_are_refused_until_full_end(self):
        first = self.replica.apply_local(self.c, max_rows=1)
        self.assertEqual(first["state"], "receiving")
        with self.assertRaises(ReplicaNotReady):
            self.replica.rows("accounts")
        with self.assertRaises(ReplicaNotReady):
            self.replica.tables()
        self.replica.acknowledge(self.c)
        self.replica.run(self.c, max_rows=10)
        self.assertEqual(self.replica.state()["state"], "ready")
        self.assertEqual([r["row_id"] for r in self.replica.rows("accounts")],
                         ["A", "B"])

    def test_empty_subscribed_table_exists_on_the_replica(self):
        self.replica.run(self.c)
        self.assertEqual(self.replica.rows("ledger"), [])
        self.assertIn("ledger", self.replica.tables())


class TestAtomicReplicaApply(ReplicaCase):
    def test_each_source_commit_visible_once_on_replica(self):
        self.replica.run(self.c)
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"balance": 10})
        tx.stage_insert("ledger", "L1", ledger())
        tx.commit()
        report = self.replica.consume(self.c, max_rows=1)
        self.assertEqual(report["row_count"], 2)
        self.assertTrue(report["applied"])
        accounts = {r["row_id"]: r for r in self.replica.rows("accounts")}
        self.assertEqual(accounts["A"]["balance"], 10)
        self.assertEqual(accounts["A"]["version"], 2)
        self.assertEqual(len(self.replica.rows("ledger")), 1)

    def test_replica_matches_source_row_by_row_type_by_type_version_by_version(self):
        self.replica.run(self.c)
        # A hand-built sequence of source changes across both tables.
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"active": False, "balance": 7})
        tx.commit()
        tx = self.c.begin()
        tx.stage_delete("accounts", "B")
        tx.commit()
        tx = self.c.begin()
        tx.stage_insert("accounts", "B", account(owner="reborn", balance=3,
                                                 active=True, note=None))
        tx.commit()
        # An attempted cross-table transaction that fails at staging: it must
        # leave no trace on either source or replica.
        bad = self.c.begin()
        try:
            bad.stage_update("ledger", "missing", {"acct": "X"})
            self.fail("staging an absent identity should fail")
        except Exception:
            bad.abort()
        tx = self.c.begin()
        tx.stage_insert("ledger", "L2", ledger(acct="B", amount=9, active=False))
        tx.commit()
        self.replica.run(self.c)
        got = {r["row_id"]: r for r in self.replica.rows("accounts")}
        want = self.expected_rows()
        self.assertEqual(set(got), set(want))
        for rid in want:
            self.assertEqual(got[rid]["version"], want[rid]["version"])
            for col in ("owner", "balance", "active", "note"):
                self.assertEqual(got[rid][col], want[rid][col])
                self.assertIs(type(got[rid][col]), type(want[rid][col]))
        ledger_rows = {r["row_id"]: r for r in self.replica.rows("ledger")}
        self.assertIs(ledger_rows["L2"]["active"], False)
        self.assertEqual(ledger_rows["L2"]["amount"], 9)


class TestReplayAndCrash(ReplicaCase):
    def test_crash_after_local_apply_before_ack_recovers_same_batch(self):
        self.replica.run(self.c)
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"balance": 50})
        tx.commit()
        # Apply locally, then "crash": never acknowledge.  The local data and
        # the batch-id record are already durable.
        applied = self.replica.apply_local(self.c, max_rows=10)
        self.assertTrue(applied["applied"])
        batch_id = applied["batch_id"]
        # A brand new replica object and a brand new client in effect.
        rep2 = Replica(self.replica_path, self.sub.handle)
        fresh = Client(self.repo.path)
        report = rep2.consume(fresh, max_rows=10)
        self.assertEqual(report["batch_id"], batch_id)
        self.assertTrue(report["replayed"])
        self.assertFalse(report["applied"])
        # The value is exactly the source's; not double-applied or rolled back.
        self.assertEqual(rep2.rows("accounts")[0]["balance"], 50)
        self.assertEqual(rep2.rows("accounts")[0]["version"], 2)

    def test_reapplying_an_old_batch_never_regresses_a_newer_row(self):
        self.replica.run(self.c)
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"balance": 1})
        tx.commit()
        b1 = self.replica.apply_local(self.c)
        self.replica.acknowledge(self.c)
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"balance": 2})
        tx.commit()
        b2 = self.replica.apply_local(self.c)
        self.replica.acknowledge(self.c)
        self.assertEqual(self.replica.rows("accounts")[0]["balance"], 2)
        # Manually force-apply the old batch payload through the internal row
        # writer (simulating a pathological duplicate delivery).
        conn = sqlite3.connect(self.replica_path, isolation_level=None)
        try:
            conn.execute("BEGIN IMMEDIATE")
            self.replica._apply_row(conn, "accounts",
                                    self.sub.handle and
                                    {"owner": "text", "balance": "int64",
                                     "active": "bool", "note": "null"},
                                    "A", 1,
                                    {"owner": "a", "balance": 0, "active": False,
                                     "note": None})
            conn.execute("COMMIT")
        finally:
            conn.close()
        row = self.replica.rows("accounts")[0]
        self.assertEqual(row["balance"], 2)  # never regressed to the old value
        self.assertEqual(row["version"], 3)

    def test_counting_effect_is_not_duplicated_by_replay(self):
        self.replica.run(self.c)
        source_counter = 0
        for _ in range(3):
            tx = self.c.begin()
            row = tx.read("accounts", columns=["row_id", "balance"])[0]
            source_counter += 1
            tx.stage_update("accounts", row["row_id"],
                            {"balance": row["balance"] + 1},
                            expected_version=row["version"])
            tx.commit()
            applied = self.replica.apply_local(self.c)
            # Crash pattern: first apply is acknowledged, but re-deliver the
            # same batch once before moving on.
            rep = Replica(self.replica_path, self.sub.handle)
            rep.consume(Client(self.repo.path))
        self.assertEqual(self.replica.rows("accounts")[0]["balance"], source_counter)
        self.assertEqual(self.replica.rows("accounts")[0]["balance"], 3)


class TestReplicaGeneration(ReplicaCase):
    def test_old_full_load_is_not_mixed_with_new_generation(self):
        # Complete generation 1.
        self.replica.run(self.c)
        self.assertEqual(len(self.replica.rows("accounts")), 2)
        # Rebuild the source subscription: generation 2 with a new cut.
        new_sub, new_cut = self.c.rebuild_subscription(self.sub.handle)
        # A replica still bound to generation 1 sees the new generation's
        # batch.  It must refuse to mix it unless explicitly reinitialised.
        self.replica.handle = new_sub.handle
        with self.assertRaises(ReplicaGenerationError):
            self.replica.consume(self.c, reinitialize=False)
        # Explicit reinitialisation throws away the old load and starts clean.
        report = self.replica.consume(self.c, reinitialize=True)
        self.assertEqual(report["generation"], 2)
        self.replica.run(self.c)
        state = self.replica.state()
        self.assertEqual(int(state["generation"]), 2)
        got = {r["row_id"]: r for r in self.replica.rows("accounts")}
        want = {r["row_id"]: r for r in self.c.rows("accounts")}
        self.assertEqual(set(got), set(want))

    def test_old_handle_cannot_drive_a_new_generation(self):
        self.replica.run(self.c)
        old = self.replica.handle
        self.c.rebuild_subscription(old)
        with self.assertRaises(StaleSubscriptionGeneration):
            self.replica.consume(self.c)


if __name__ == "__main__":
    unittest.main()
