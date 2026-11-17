"""Online add-column migration tests.

These cover the migration itself (validation, atomicity, untouched business
versions), the snapshot/structural-conflict rules for transactions that began
before a migration, the ordered schema-change stream through subscriptions,
and the replica's schema switch including the crash/replay seam.

Every business-state assertion is computed from the public ``history``/
``rows`` APIs or hand-written literals -- never from the product's own
schema-event or incremental machinery.
"""

import os
import shutil
import sqlite3
import tempfile
import unittest

from support import TempRepo, account, seed_accounts

from mrtx import Client, Replica, SchemaConflictError, SchemaError


class TestAddColumnValidation(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        seed_accounts(self.c, [("A", account(balance=1))])

    def tearDown(self):
        self.repo.__exit__(None, None, None)

    def test_unknown_table_is_rejected_without_side_effects(self):
        before = self.c.tables()
        with self.assertRaises(SchemaError):
            self.c.add_column("nope", "g", "text", "x")
        self.assertEqual(self.c.tables(), before)
        self.assertEqual(self.c.schema_history(), [])

    def test_column_name_conflict_is_rejected(self):
        with self.assertRaises(SchemaError):
            self.c.add_column("accounts", "owner", "text", "x")
        with self.assertRaises(SchemaError):
            self.c.add_column("accounts", "row_id", "text", "x")

    def test_unsupported_type_rejected(self):
        with self.assertRaises(SchemaError):
            self.c.add_column("accounts", "g", "decimal", 1)

    def test_default_must_match_domain(self):
        with self.assertRaises(SchemaError):
            self.c.add_column("accounts", "g", "int64", "nope")
        with self.assertRaises(SchemaError):
            self.c.add_column("accounts", "g", "bool", 1)  # int is not bool
        with self.assertRaises(SchemaError):
            self.c.add_column("accounts", "g", "text", 5)
        # null-only columns accept only None.
        with self.assertRaises(SchemaError):
            self.c.add_column("accounts", "g", "null", "x")

    def test_failed_migration_leaves_no_half_state(self):
        # A bad default never opens a write transaction; verify the physical
        # table, the catalog and the change log are all untouched.
        hist_before = self._physical_columns("accounts")
        with self.assertRaises(SchemaError):
            self.c.add_column("accounts", "g", "int64", "bad")
        self.assertEqual(self._physical_columns("accounts"), hist_before)
        con = sqlite3.connect(self.repo.path)
        try:
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM schema_changes").fetchone()[0], 0)
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM table_schemas "
                            "WHERE table_name='accounts'").fetchone()[0], 1)
        finally:
            con.close()

    def _physical_columns(self, table):
        con = sqlite3.connect(self.repo.path)
        try:
            return [r[1] for r in con.execute(
                'PRAGMA table_info("%s_rows")' % table)]
        finally:
            con.close()


class TestAddColumnSemantics(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        seed_accounts(self.c, [("A", account(owner="a", balance=1)),
                               ("B", account(owner="b", balance=2))])

    def tearDown(self):
        self.repo.__exit__(None, None, None)

    def test_old_rows_read_default_without_version_change(self):
        versions_before = {r["row_id"]: r["version"]
                           for r in self.c.rows("accounts")}
        out = self.c.add_column("accounts", "source_group", "text", "organic")
        self.assertIsInstance(out["commit_version"], int)
        rows = {r["row_id"]: r for r in self.c.rows("accounts")}
        for rid in ("A", "B"):
            self.assertEqual(rows[rid]["source_group"], "organic")
            # The business version is unchanged: the migration is not a row
            # update and must not pretend every row was rewritten.
            self.assertEqual(rows[rid]["version"], versions_before[rid])

    def test_history_images_keep_their_original_versions(self):
        self.c.add_column("accounts", "source_group", "text", "organic")
        for rid in ("A", "B"):
            hist = self.c.history("accounts", rid)
            self.assertEqual([h["version"] for h in hist], [1])
            self.assertFalse(hist[0]["deleted"])
            self.assertEqual(hist[0]["source_group"], "organic")

    def test_omitted_insert_and_update_use_default(self):
        self.c.add_column("accounts", "tier", "int64", 7)
        tx = self.c.begin()
        tx.stage_insert("accounts", "C",
                        {"owner": "c", "balance": 3, "active": True, "note": None})
        tx.commit()
        rows = {r["row_id"]: r for r in self.c.rows("accounts")}
        self.assertEqual(rows["C"]["tier"], 7)
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"balance": 99})  # tier omitted
        tx.commit()
        rows = {r["row_id"]: r for r in self.c.rows("accounts")}
        self.assertEqual(rows["A"]["tier"], 7)
        self.assertEqual(rows["A"]["balance"], 99)

    def test_explicit_value_is_validated_and_kept(self):
        self.c.add_column("accounts", "tier", "int64", 7)
        tx = self.c.begin()
        tx.stage_insert("accounts", "D",
                        {"owner": "d", "balance": 4, "active": False,
                         "note": None, "tier": 42})
        tx.commit()
        rows = {r["row_id"]: r for r in self.c.rows("accounts")}
        self.assertEqual(rows["D"]["tier"], 42)
        # An ill-typed explicit value is still rejected.
        bad = self.c.begin()
        with self.assertRaises(SchemaError):
            bad.stage_insert("accounts", "E",
                             {"owner": "e", "balance": 5, "active": True,
                              "note": None, "tier": "not-an-int"})
        bad.abort()

    def test_bool_default_preserves_type_disjointness(self):
        self.c.add_column("accounts", "flag", "bool", False)
        row = [r for r in self.c.rows("accounts") if r["row_id"] == "A"][0]
        # The stored value round-trips as an exact bool, never a bare int.
        self.assertIs(type(row["flag"]), bool)
        self.assertIs(row["flag"], False)

    def test_multiple_migrations_accumulate_in_order(self):
        v1 = self.c.add_column("accounts", "g1", "text", "a")["commit_version"]
        v2 = self.c.add_column("accounts", "g2", "int64", 0)["commit_version"]
        self.assertLess(v1, v2)
        history = self.c.schema_history("accounts")
        self.assertEqual([h["column"] for h in history], ["g1", "g2"])
        self.assertEqual([h["schema_version"] for h in history], [2, 3])
        self.assertEqual([h["commit_version"] for h in history], [v1, v2])
        row = [r for r in self.c.rows("accounts") if r["row_id"] == "A"][0]
        self.assertEqual(row["g1"], "a")
        self.assertEqual(row["g2"], 0)

    def test_migration_shares_the_commit_version_stream_with_data(self):
        before = self.c.current_version()
        out = self.c.add_column("accounts", "g", "text", "x")
        self.assertEqual(out["commit_version"], before + 1)
        self.assertEqual(self.c.current_version(), before + 1)
        tx = self.c.begin()
        tx.stage_update("accounts", "A", {"balance": 50})
        data_commit = tx.commit()["commit_version"]
        self.assertEqual(data_commit, before + 2)


class TestSnapshotAndStructuralConflict(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        seed_accounts(self.c, [("A", account(owner="a", balance=1))])

    def tearDown(self):
        self.repo.__exit__(None, None, None)

    def test_transaction_started_before_migration_keeps_old_columns_on_read(self):
        old = self.c.begin()
        self.c.add_column("accounts", "source_group", "text", "organic")
        row = old.read("accounts")[0]
        self.assertNotIn("source_group", row)
        self.assertEqual(set(row), {"row_id", "version", "owner", "balance",
                                    "active", "note"})

    def test_old_transaction_cannot_reference_the_new_column(self):
        old = self.c.begin()
        self.c.add_column("accounts", "source_group", "text", "organic")
        with self.assertRaises(SchemaError):
            old.read("accounts", columns=["source_group"])

    def test_old_transaction_write_is_refused_with_no_effect(self):
        old = self.c.begin()
        self.c.add_column("accounts", "source_group", "text", "organic")
        with self.assertRaises(SchemaConflictError) as caught:
            old.stage_update("accounts", "A", {"balance": 99})
        ev = caught.exception.evidence
        self.assertEqual(ev["code"], "schema_conflict")
        self.assertEqual(ev["added_columns"], ["source_group"])
        self.assertIn("source_group", ev["columns_now"])
        self.assertNotIn("source_group", ev["columns_at_snapshot"])
        # The transaction is still usable for reads and can close cleanly.
        self.assertEqual(old.read("accounts")[0]["balance"], 1)
        old.abort()
        # Nothing was written.
        self.assertEqual(
            [r for r in self.c.rows("accounts") if r["row_id"] == "A"][0]["balance"],
            1)

    def test_old_transaction_insert_is_refused(self):
        old = self.c.begin()
        self.c.add_column("accounts", "source_group", "text", "organic")
        with self.assertRaises(SchemaConflictError):
            old.stage_insert("accounts", "Z", account(owner="z"))
        self.assertNotIn("Z", {r["row_id"] for r in self.c.rows("accounts")})
        old.abort()

    def test_read_only_old_transaction_still_commits(self):
        old = self.c.begin()
        old.read("accounts")
        self.c.add_column("accounts", "g", "text", "x")
        result = old.commit()  # serializes at its start view; no write conflict
        self.assertEqual(result["status"], "committed")

    def test_new_transaction_uses_new_definition(self):
        self.c.add_column("accounts", "source_group", "text", "organic")
        tx = self.c.begin()
        row = tx.read("accounts")[0]
        self.assertIn("source_group", row)
        self.assertEqual(row["source_group"], "organic")
        tx.abort()

    def test_concurrent_non_conflicting_commit_proceeds_during_migration_window(self):
        # A second table's writer is unaffected by a migration on accounts.
        self.c.create_table("other", [("n", "int64")])
        tx = self.c.begin()
        tx.stage_insert("other", "o1", {"n": 1})
        self.c.add_column("accounts", "g", "text", "x")
        out = tx.commit()
        self.assertEqual(out["status"], "committed")
        self.assertEqual(self.c.rows("other")[0]["n"], 1)


class TestSubscriptionSchemaStream(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        seed_accounts(self.c, [("r%d" % i, account(balance=i))
                               for i in range(6)])
        self.sub, self.cut = self.c.create_subscription("s", ["accounts"])
        self.h = self.sub.handle

    def tearDown(self):
        self.repo.__exit__(None, None, None)

    def _drain(self, max_rows=2):
        order = []
        while True:
            result = self.c.pull(self.h, max_rows=max_rows)
            batch = result["batch"]
            if batch is None:
                return order
            order.append(batch)
            self.c.ack(self.h, batch["batch_id"])

    def test_full_pages_keep_old_schema_when_migration_lands_midway(self):
        first = self.c.pull(self.h, max_rows=2)["batch"]
        self.assertEqual([i["row_id"] for i in first["items"]],
                         ["r0", "r1"])
        # The not-yet-finished full load describes only the original columns.
        self.assertEqual(
            set(first["tables"][0]["columns"]),
            {"owner", "balance", "active", "note"})
        self.c.ack(self.h, first["batch_id"])
        migration = self.c.add_column("accounts", "source_group", "text",
                                      "organic")
        # Remaining full pages must not gain the new column or change values.
        rest = []
        while True:
            b = self.c.pull(self.h, max_rows=2)["batch"]
            rest.append(b)
            self.c.ack(self.h, b["batch_id"])
            if b["full_complete"]:
                break
        full_items = first["items"] + [i for b in rest for i in b["items"]]
        self.assertEqual([i["row_id"] for i in full_items],
                         ["r%d" % i for i in range(6)])
        for item in full_items:
            self.assertNotIn("source_group", item["values"])
            self.assertEqual(item["version"], 1)

    def test_schema_event_precedes_later_row_changes_in_stream(self):
        # Finish the full load first.
        self._drain()
        migration = self.c.add_column("accounts", "source_group", "text",
                                      "organic")
        tx = self.c.begin()
        tx.stage_update("accounts", "r4",
                        {"balance": 404, "source_group": "ads"})
        tx.commit()
        batches = self._drain(max_rows=1)
        kinds = [b.get("kind") for b in batches]
        self.assertEqual(kinds, ["schema_change", "row_change"])
        schema_batch = batches[0]
        self.assertEqual(schema_batch["position"], migration["commit_version"])
        event = schema_batch["schema_events"][0]
        self.assertEqual(event["column"], "source_group")
        self.assertEqual(event["column_type"], "text")
        self.assertEqual(event["default"], "organic")
        # The schema batch's table descriptor already includes the new column.
        self.assertIn("source_group",
                      schema_batch["tables"][0]["columns"])
        self.assertEqual(schema_batch["tables"][0]["defaults"],
                         {"source_group": "organic"})
        row_batch = batches[1]
        item = row_batch["items"][0]
        self.assertEqual(item["row_id"], "r4")
        self.assertEqual(item["after"]["values"]["source_group"], "ads")

    def test_cleanup_keeps_schema_event_until_acknowledged(self):
        self._drain()
        self.c.add_column("accounts", "g", "text", "x")
        # Nothing acknowledged past the cut yet: the event is retained.
        report = self.c.cleanup_sync_history()
        self.assertEqual(report["remaining_schema_changes"], 1)
        self._drain()
        report = self.c.cleanup_sync_history()
        self.assertEqual(report["remaining_schema_changes"], 0)
        self.assertEqual(report["reclaimed_schema_changes"], 1)


class TestReplicaSchemaSwitch(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        seed_accounts(self.c, [("r%d" % i, account(balance=i))
                               for i in range(6)])
        self.replica_path = os.path.join(self.repo.dir, "replica.db")
        self.sub, self.cut = self.c.create_subscription("s", ["accounts"])
        self.h = self.sub.handle
        self.replica = Replica(self.replica_path, self.sub.handle)

    def tearDown(self):
        self.repo.__exit__(None, None, None)

    def _independent_expectation(self, boundary):
        """Hand/history-computed expected rows, never using sync machinery."""
        ids = ["r%d" % i for i in range(6)]
        out = {}
        for rid in ids:
            hist = [h for h in self.c.history("accounts", rid)
                    if h["version"] <= boundary]
            if not hist:
                continue
            newest = max(hist, key=lambda h: h["version"])
            if newest.get("deleted"):
                continue
            out[rid] = newest
        return out

    def test_mid_full_migration_replica_converges_value_type_version(self):
        # One full page applied locally then acknowledged.
        self.replica.apply_local(self.c, max_rows=2)
        self.replica.acknowledge(self.c)
        migration = self.c.add_column("accounts", "source_group", "text",
                                      "organic")
        tx = self.c.begin()
        tx.stage_update("accounts", "r4",
                        {"balance": 404, "source_group": "ads"})
        tx.commit()
        tx = self.c.begin()
        tx.stage_update("accounts", "r5", {"balance": 505})  # omitted new col
        boundary = tx.commit()["commit_version"]

        while True:
            report = self.replica.apply_local(self.c, max_rows=2)
            if report is None:
                break
            self.replica.acknowledge(self.c)

        self.assertEqual(self.replica.state()["position"], boundary)
        got = {r["row_id"]: r for r in self.replica.rows("accounts")}
        want = self._independent_expectation(boundary)
        self.assertEqual(set(got), set(want))
        cols = ("owner", "balance", "active", "note", "source_group")
        for rid, w in want.items():
            g = got[rid]
            self.assertEqual(g["version"], w["version"], rid)
            for col in cols:
                self.assertEqual(g[col], w[col], "%s.%s" % (rid, col))
                self.assertIs(type(g[col]), type(w[col]),
                              "%s.%s type" % (rid, col))
        # Hand-checked facts.
        self.assertEqual(got["r4"]["source_group"], "ads")
        self.assertEqual(got["r4"]["version"], migration["commit_version"] + 1)
        self.assertEqual(got["r5"]["source_group"], "organic")  # default
        self.assertEqual(got["r0"]["version"], 1)               # untouched
        # The column was physically added exactly once.
        con = sqlite3.connect(self.replica_path)
        try:
            names = [r[1] for r in con.execute(
                'PRAGMA table_info("accounts")')]
        finally:
            con.close()
        self.assertEqual(names.count("source_group"), 1)

    def test_crash_after_local_switch_before_ack_replays_without_duplicate(self):
        self.replica.run(self.c)  # full load complete, ready
        migration = self.c.add_column("accounts", "source_group", "text",
                                      "organic")
        tx = self.c.begin()
        tx.stage_update("accounts", "r1", {"source_group": "ads"})
        tx.commit()
        # Local schema-switch commit, but no source acknowledgement: crash.
        applied = self.replica.apply_local(self.c)
        self.assertEqual(applied["kind"], "schema_change")
        batch_id = applied["batch_id"]
        con = sqlite3.connect(self.replica_path)
        try:
            local_cols = [r[1] for r in con.execute(
                'PRAGMA table_info("accounts")')]
            recorded = [r[0] for r in con.execute(
                "SELECT batch_id FROM replica_applied")]
            position = con.execute(
                "SELECT value FROM replica_meta WHERE key='position'").fetchone()[0]
        finally:
            con.close()
        self.assertIn("source_group", local_cols)
        self.assertIn(batch_id, recorded)
        self.assertEqual(int(position), migration["commit_version"])
        # The source still holds the batch pending.
        self.assertEqual(self.c.subscription_status(self.h)["pending_batch"],
                         batch_id)

        # Reopen with brand-new objects (simulated process restart).
        reopened = Replica(self.replica_path, self.sub.handle)
        fresh = Client(self.repo.path)
        redelivered = reopened.consume(fresh)
        self.assertEqual(redelivered["batch_id"], batch_id)
        # The local ALTER/apply was replayed (batch id already recorded), while
        # the source acknowledgement is the first real one -- it had never been
        # sent before the crash.
        self.assertTrue(redelivered["replayed"])
        self.assertFalse(redelivered["applied"])
        self.assertFalse(redelivered["ack_replayed"])
        # Drain the rest; later rows must use the NEW structure.
        reopened.run(fresh)
        rows = {r["row_id"]: r for r in reopened.rows("accounts")}
        self.assertEqual(rows["r1"]["source_group"], "ads")
        self.assertEqual(rows["r0"]["source_group"], "organic")
        con = sqlite3.connect(self.replica_path)
        try:
            names = [r[1] for r in con.execute(
                'PRAGMA table_info("accounts")')]
        finally:
            con.close()
        self.assertEqual(names.count("source_group"), 1)
        # The schema batch id is recorded exactly once (no duplicate apply).
        con = sqlite3.connect(self.replica_path)
        try:
            schema_records = con.execute(
                "SELECT COUNT(*) FROM replica_applied WHERE batch_id=?",
                (batch_id,)).fetchone()[0]
        finally:
            con.close()
        self.assertEqual(schema_records, 1)

    def test_new_subscription_after_migration_uses_new_schema_directly(self):
        self.replica.run(self.c)
        self.c.add_column("accounts", "source_group", "text", "organic")
        sub2, _ = self.c.create_subscription("later", ["accounts"])
        path2 = os.path.join(self.repo.dir, "replica2.db")
        rep2 = Replica(path2, sub2.handle)
        rep2.run(self.c)
        rows = {r["row_id"]: r for r in rep2.rows("accounts")}
        self.assertIn("source_group", rows["r0"])
        self.assertEqual(rows["r0"]["source_group"], "organic")


class TestLegacyUpgrade(unittest.TestCase):
    def _v2_database(self, path):
        """Build a realistic v2 repository (no v3 catalog or schema log)."""
        c = Client(path)
        c.create_table("t", [("v", "int64"), ("name", "text")])
        tx = c.begin()
        tx.stage_insert("t", "k", {"v": 1, "name": "a"})
        tx.commit()
        sub, _ = c.create_subscription("s", ["t"])
        # Downgrade to the v2 physical layout.
        con = sqlite3.connect(path, isolation_level=None)
        con.execute("UPDATE meta SET value='2' WHERE key='schema_version'")
        con.execute("DROP TABLE IF EXISTS table_schemas")
        con.execute("DROP TABLE IF EXISTS schema_changes")
        con.close()
        return sub

    def test_v2_database_backfills_catalog_and_preserves_data(self):
        tmp = tempfile.mkdtemp(prefix="mrtx-v3-upgrade-")
        path = os.path.join(tmp, "old.db")
        try:
            self._v2_database(path)
            c = Client(path)  # triggers the v2 -> v3 upgrade
            self.assertEqual(c.tables(), {"t": {"v": "int64", "name": "text"}})
            self.assertEqual(c.rows("t")[0]["name"], "a")
            schemas = c.table_schemas("t")
            self.assertEqual(len(schemas), 1)
            self.assertEqual(schemas[0]["schema_version"], 1)
            self.assertEqual(schemas[0]["effective_version"], 0)
            # Online migration works on the upgraded legacy repository.
            out = c.add_column("t", "g", "text", "org")
            self.assertEqual(c.rows("t")[0]["g"], "org")
            self.assertEqual(c.schema_history("t")[0]["commit_version"],
                             out["commit_version"])
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_v2_subscription_keeps_working_and_receives_schema_event(self):
        tmp = tempfile.mkdtemp(prefix="mrtx-v3-upgrade-sub-")
        path = os.path.join(tmp, "old.db")
        rep_path = os.path.join(tmp, "rep.db")
        try:
            sub = self._v2_database(path)
            c = Client(path)
            # The existing subscription row gained the defaults column.
            status = c.subscription_status(sub.handle)
            self.assertEqual(status["schema_defaults"], {"t": {}})
            c.add_column("t", "g", "text", "org")
            rep = Replica(rep_path, sub.handle)
            rep.run(c)
            rows = {r["row_id"]: r for r in rep.rows("t")}
            self.assertEqual(rows["k"]["g"], "org")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
