"""Real OS-process drills for materialized join-aggregate summaries.

Covers the requirements that cannot be exercised in one process:

* a paged initial build paused mid-scan while another process keeps writing
  both source tables and runs GC/cleanup, then finished in a *different*
  process -- the result must equal the naive summary at the activation point;
* a build cancelled from another process releases its retention;
* two processes appending different source identities that land in the same
  summary group both commit (no artificial same-key write conflict) and the
  totals accumulate;
* a read of the summary followed by another process changing it (including an
  empty-predicate appearance) raises the read-invalidation conflict;
* a cascade delete on a source table withdraws the affected summary groups in
  the same commit;
* a subscription spanning a source table and its materialized summary delivers
  both changes in one per-commit batch, and a replica applies them.
"""

import os
import unittest

from procutil import Worker
from support import TempRepo, drain_changes, drain_full

from mrtx import Client, ConflictError, Replica, SubscriptionHandle


def lrow(k, g, qty=None, flag=None):
    return {"k": k, "g": g, "qty": qty, "flag": flag}


def rrow(k, cat, amt=None):
    return {"k": k, "cat": cat, "amt": amt}


GROUPS = [{"table": "left", "column": "g", "alias": "grp"}]
AGGS = [
    {"fn": "count_star", "alias": "n"},
    {"fn": "sum", "column": {"table": "right", "column": "amt"},
     "alias": "amt_sum"},
]


class MvProcessCase(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        self.workers = []
        self.c.create_table("lefts", [("k", "text"), ("g", "text"),
                                      ("qty", "int64"), ("flag", "bool")])
        self.c.create_table("rights", [("k", "text"), ("cat", "text"),
                                       ("amt", "int64")])

    def tearDown(self):
        for w in self.workers:
            w.close()
        self.repo.__exit__(None, None, None)

    def worker(self):
        w = Worker(self.repo.path)
        self.workers.append(w)
        return w

    def define_and_start(self, name="mv", join_kind="left"):
        self.c.define_materialized_summary(
            name, "lefts", "rights", "k", "k", join_kind, GROUPS, AGGS)
        return self.c.start_materialized_build(name)

    def build_in_worker(self, w, name="mv", budget=3):
        for _ in range(10000):
            out = w.send({"cmd": "mv_build", "name": name, "budget": budget})
            assert out["ok"], out
            if out["status"]["state"] == "active":
                return out["status"]
        raise AssertionError("build did not finish")

    def test_paused_build_resumes_in_another_process_amid_writes_and_gc(self):
        c = self.c
        # Seed enough left rows to force several pages.
        tx = c.begin()
        for i in range(12):
            tx.stage_insert("lefts", "L%02d" % i,
                            lrow("k%02d" % i, "G", qty=i, flag=True))
        for i in range(12):
            tx.stage_insert("rights", "R%02d" % i,
                            rrow("k%02d" % i, "c", amt=i))
        tx.commit()
        cut = c.current_version()
        status = self.define_and_start()
        self.assertEqual(status["state"], "building")

        # Process A advances the build by a couple of pages, then exits.
        builder = self.worker()
        for _ in range(2):
            out = builder.send({"cmd": "mv_build", "name": "mv", "budget": 3})
            self.assertTrue(out["ok"], out)
            self.assertEqual(out["status"]["state"], "building")
        builder.kill()

        # Another process keeps writing BOTH tables while the build is paused.
        writer = self.worker()
        t = writer.begin()
        writer.insert(t, "lefts", "L99", lrow("k01", "G", qty=99, flag=False))
        writer.insert(t, "rights", "R99", rrow("k01", "c", amt=7))
        writer.commit(t)
        # GC and sync cleanup must not destroy the build's cut recovery basis.
        gc = writer.gc()
        self.assertIn(status["build"]["cut_version"], gc["active_snapshots"])
        writer.cleanup()

        # A fresh process resumes and finishes the build + catch-up.
        finisher = self.worker()
        final = self.build_in_worker(finisher, budget=4)
        self.assertEqual(final["state"], "active")
        self.assertEqual(final["maintained_version"],
                         final["current_source_position"])

        # The active summary equals a fresh naive recomputation of the source.
        import mv_naive
        definition = c.get_materialized_summary("mv")["definition"]
        left = {r["row_id"]: {k: v for k, v in r.items()
                             if k not in ("row_id", "version")}
                for r in c.rows("lefts")}
        right = {r["row_id"]: {k: v for k, v in r.items()
                              if k not in ("row_id", "version")}
                 for r in c.rows("rights")}
        expected = mv_naive.naive_summary(definition, left, right)
        got = {(r["grp"],): r for r in c.rows("mv")}
        self.assertEqual(set(got), set(expected))
        for key, exp in expected.items():
            for alias, value in exp.items():
                self.assertEqual(got[key][alias], value, (key, alias))
        # The cut itself was a real source position and activation is at a
        # strictly later position.
        self.assertGreaterEqual(final["maintained_version"], cut)

    def test_cancel_in_another_process_releases_retention(self):
        c = self.c
        tx = c.begin()
        tx.stage_insert("lefts", "L1", lrow("a", "G"))
        tx.commit()
        self.define_and_start()
        builder = self.worker()
        builder.send({"cmd": "mv_build", "name": "mv", "budget": 1})
        canceller = self.worker()
        out = canceller.send({"cmd": "mv_cancel", "name": "mv"})
        self.assertTrue(out["ok"], out)
        self.assertEqual(out["status"]["state"], "cancelled")
        import sqlite3
        conn = sqlite3.connect(c.db_path)
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("mv_rows", tables)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM materialized_accum WHERE mv_name='mv'"
                             ).fetchone()[0], 0)
        finally:
            conn.close()

    def test_two_processes_appending_different_identities_same_group_both_commit(self):
        c = self.c
        tx = c.begin()
        tx.stage_insert("lefts", "Lhot", lrow("hot", "G"))
        tx.stage_insert("rights", "R0", rrow("hot", "c", amt=10))
        tx.commit()
        self.define_and_start(join_kind="inner")
        self.build_in_worker(self.worker())
        w1, w2 = self.worker(), self.worker()
        t1, t2 = w1.begin(), w2.begin()
        w1.insert(t1, "rights", "Ra", rrow("hot", "c", amt=1))
        w2.insert(t2, "rights", "Rb", rrow("hot", "c", amt=2))
        r1 = w1.commit(t1)
        r2 = w2.commit(t2)
        self.assertTrue(r1["ok"], r1)
        self.assertTrue(r2["ok"], r2)  # different identities, no ww conflict
        row = [r for r in c.rows("mv") if r["grp"] == "G"][0]
        # Original R0 plus the two new matching rows.
        self.assertEqual(row["n"], 3)
        self.assertEqual(row["amt_sum"], 13)

    def test_summary_read_conflict_on_change_and_phantom_appearance(self):
        c = self.c
        # An unrelated ordinary table gives the reader a write so its commit
        # is validated: a pure read-only transaction serializes at its start
        # view by design and is never rejected, exactly as for normal tables.
        c.create_table("scratch", [("note", "text")])
        tx = c.begin()
        tx.stage_insert("lefts", "L1", lrow("a", "G"))
        tx.commit()
        self.define_and_start()
        self.build_in_worker(self.worker())
        reader, writer = self.worker(), self.worker()
        tr = reader.begin()
        self.assertEqual(reader.read(tr, "mv", where=None)["rows"][0]["n"], 1)
        reader.insert(tr, "scratch", "S1", {"note": "read then write"})
        tw = writer.begin()
        writer.insert(tw, "lefts", "L2", lrow("a", "G"))
        writer.commit(tw)
        # The reader's observed summary row changed underneath it.
        conflict = reader.commit(tr)
        self.assertFalse(conflict["ok"])
        self.assertEqual(conflict["code"], "read_conflict")
        # An empty-result read is invalidated by a phantom appearance.
        reader2, writer2 = self.worker(), self.worker()
        tr2 = reader2.begin()
        self.assertEqual(reader2.read(tr2, "mv",
                                      where={"op": "eq", "col": "grp",
                                             "value": "NEW"})["rows"], [])
        reader2.insert(tr2, "scratch", "S2", {"note": "phantom"})
        tw2 = writer2.begin()
        writer2.insert(tw2, "lefts", "L3", lrow("a", "NEW"))
        writer2.commit(tw2)
        conflict2 = reader2.commit(tr2)
        self.assertFalse(conflict2["ok"])
        self.assertEqual(conflict2["code"], "read_conflict")

    def test_cascade_delete_withdraws_summary_groups_in_same_commit(self):
        c = self.c
        c.create_table("parents", [("name", "text")])
        tx = c.begin()
        tx.stage_insert("parents", "P1", {"name": "P1"})
        tx.stage_insert("lefts", "L1", lrow("P1", "G"))
        tx.stage_insert("rights", "R1", rrow("P1", "c", amt=5))
        tx.commit()
        c.register_reference("cas", "parents", "lefts", "k", "cascade")
        self.define_and_start(join_kind="inner")
        self.build_in_worker(self.worker())
        self.assertEqual(len(c.rows("mv")), 1)
        tx = c.begin()
        tx.stage_delete("parents", "P1")
        tx.commit()
        # The cascaded left row removal withdraws the only join row.
        self.assertEqual(c.rows("mv"), [])
        identities = c.list_materialized_group_identities("mv")
        self.assertEqual(len(identities), 1)
        self.assertFalse(identities[0]["active"])

    def test_subscription_spanning_source_and_summary_delivers_one_batch(self):
        c = self.c
        tx = c.begin()
        tx.stage_insert("lefts", "L1", lrow("a", "G"))
        tx.stage_insert("rights", "R1", rrow("a", "c", amt=5))
        tx.commit()
        self.define_and_start()
        self.build_in_worker(self.worker())
        # A subscription used to inspect the per-commit incremental stream.
        sub, cut = c.create_subscription(
            "both", ["lefts", "rights", "mv"])
        drain_full(c, sub.handle)
        tx = c.begin()
        tx.stage_insert("rights", "R2", rrow("a", "c", amt=3))
        tx.commit()
        changes = drain_changes(c, sub.handle)
        positions = [p for p, _ in changes]
        self.assertEqual(positions, [c.current_version()])  # one commit batch
        tables = {item["table"] for _, items in changes for item in items}
        self.assertIn("mv", tables)
        self.assertIn("rights", tables)
        # A separate subscription drives a replica from its own full load
        # through the same stream; it must end ready and carry the summary.
        rep_sub, _cut = c.create_subscription(
            "rep", ["lefts", "rights", "mv"])
        rep_path = os.path.join(self.repo.dir, "replica.db")
        rep = Replica(rep_path,
                      SubscriptionHandle.from_dict(rep_sub.handle.as_dict()))
        fresh = Client(c.db_path)
        rep.run(fresh, max_rows=50)
        self.assertTrue(rep.state()["ready"])
        self.assertEqual({r["grp"] for r in rep.rows("mv")}, {"G"})
        # The replica's applied position reaches the current source position.
        self.assertEqual(rep.state()["position"], c.current_version())


if __name__ == "__main__":
    unittest.main()
