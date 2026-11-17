"""Crash/replay and fan-out drills for materialized summaries.

* a build whose progress is on disk is resumed after the builder process is
  SIGKILLed (no in-memory state survives);
* a source commit whose decision is durable but whose response never reaches
  the caller (the worker exits inside commit) is resent from another process
  and must not double-count the maintained totals or reuse a source position;
* a high fan-out join (many right rows on one key) is maintained without ever
  materializing the full join-row list, and the disk accumulator stays bounded
  by the number of groups rather than the number of join rows.
"""

import os
import sqlite3
import unittest

from procutil import Worker
from support import TempRepo

from mrtx import Client


GROUPS = [{"table": "left", "column": "g", "alias": "grp"}]
AGGS = [
    {"fn": "count_star", "alias": "n"},
    {"fn": "sum", "column": {"table": "right", "column": "amt"},
     "alias": "amt_sum"},
]


class CrashFanoutCase(unittest.TestCase):
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

    def define(self, join_kind="left"):
        self.c.define_materialized_summary(
            "mv", "lefts", "rights", "k", "k", join_kind, GROUPS, AGGS)
        self.c.start_materialized_build("mv")

    def finish(self, w, budget=3):
        for _ in range(10000):
            if w.mv_build("mv", budget)["status"]["state"] == "active":
                return

    def test_killed_builder_resumes_from_durable_progress_in_new_process(self):
        c = self.c
        tx = c.begin()
        for i in range(10):
            tx.stage_insert("lefts", "L%02d" % i,
                           {"k": "k%02d" % i, "g": "G", "qty": i, "flag": True})
            tx.stage_insert("rights", "R%02d" % i,
                           {"k": "k%02d" % i, "cat": "c", "amt": i})
        tx.commit()
        self.define()
        builder = self.worker()
        for _ in range(2):
            builder.mv_build("mv", budget=2)
        builder.kill()  # SIGKILL: only on-disk build rows survive.
        # A brand-new process resumes the same build and reaches active.
        finisher = self.worker()
        status = finisher.mv_status("mv")
        self.assertEqual(status["state"], "building")
        self.finish(finisher, budget=3)
        self.assertEqual(finisher.mv_status("mv")["state"], "active")
        # The totals are exactly the seeded values.
        rows = c.rows("mv")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["n"], 10)
        self.assertEqual(rows[0]["amt_sum"], sum(range(10)))

    def test_commit_replay_after_process_exit_does_not_double_count(self):
        c = self.c
        tx = c.begin()
        tx.stage_insert("lefts", "L1", {"k": "hot", "g": "G", "qty": 1,
                                        "flag": True})
        tx.stage_insert("rights", "R1", {"k": "hot", "cat": "c", "amt": 10})
        tx.commit()
        self.define(join_kind="inner")
        self.finish(self.worker())
        position_before = c.current_version()
        row_before = c.rows("mv")[0]

        # Stage a commit in a worker, then crash after the durable decision.
        crasher = self.worker()
        txid = crasher.begin()
        crasher.insert(txid, "rights", "R2",
                       {"k": "hot", "cat": "c", "amt": 5})
        try:
            crasher.send({"cmd": "commit_crash", "txid": txid})
        except RuntimeError:
            pass
        crasher.proc.wait()
        self.workers.remove(crasher)

        # The decision is durable even though no response was returned.
        status = c.transaction_status(txid)
        self.assertTrue(status["committed"])
        crashed_version = status["commit_version"]
        self.assertEqual(crashed_version, position_before + 1)

        # Re-opening from another process returns the same durable decision
        # for that transaction id and cannot advance the position or
        # re-maintain the summary.
        other = Client(c.db_path)
        replay_status = other.transaction_status(txid)
        self.assertEqual(replay_status["commit_version"], crashed_version)
        self.assertEqual(other.current_version(), crashed_version)
        row_after = c.rows("mv")[0]
        self.assertEqual(row_after["n"], row_before["n"] + 1)
        self.assertEqual(row_after["amt_sum"],
                         row_before["amt_sum"] + 5)
        # No half-applied duplicate output version exists for the group.
        conn = sqlite3.connect(c.db_path)
        try:
            versions = [r[0] for r in conn.execute(
                "SELECT DISTINCT version FROM mv_rows WHERE deleted=0")]
            self.assertEqual(versions.count(crashed_version), 1)
            # The maintenance statistics record exactly one maintenance event
            # at the crashed position.
            events = [s for s in c.materialized_maintenance_stats("mv")
                      if s["commit_version"] == crashed_version
                      and s["phase"] == "maintenance"]
            self.assertEqual(len(events), 1)
        finally:
            conn.close()

    def test_high_fanout_keeps_disk_state_bounded_by_groups(self):
        c = self.c
        fanout = 200
        tx = c.begin()
        tx.stage_insert("lefts", "L1", {"k": "hot", "g": "G", "qty": 1,
                                        "flag": True})
        for i in range(fanout):
            tx.stage_insert("rights", "R%04d" % i,
                           {"k": "hot", "cat": "c", "amt": 1})
        tx.commit()
        self.define(join_kind="inner")
        # Build with a deliberately tiny budget so a naive in-memory join list
        # would have to hold all fanout rows at once; the disk accumulator must
        # not.
        self.finish(self.worker(), budget=2)
        row = c.rows("mv")[0]
        self.assertEqual(row["n"], fanout)
        self.assertEqual(row["amt_sum"], fanout)
        conn = sqlite3.connect(c.db_path)
        try:
            accum_groups = conn.execute(
                "SELECT COUNT(*) FROM materialized_accum WHERE mv_name='mv'"
            ).fetchone()[0]
            group_key_rows = conn.execute(
                "SELECT COUNT(*) FROM materialized_group_keys WHERE mv_name='mv'"
            ).fetchone()[0]
            # One group, and only the single join key 'hot' feeds it -- never
            # one auxiliary row per fan-out join result.
            self.assertEqual(accum_groups, 1)
            self.assertEqual(group_key_rows, 1)
        finally:
            conn.close()

    def test_maintenance_after_added_unrelated_column_is_unchanged(self):
        c = self.c
        tx = c.begin()
        tx.stage_insert("lefts", "L1", {"k": "a", "g": "G", "qty": 1,
                                        "flag": True})
        tx.stage_insert("rights", "R1", {"k": "a", "cat": "c", "amt": 4})
        tx.commit()
        self.define()
        self.finish(self.worker())
        # Add a column unrelated to the frozen definition on a source table.
        c.add_column("lefts", "extra", "text", "default-x")
        before = c.rows("mv")[0]
        tx = c.begin()
        tx.stage_update("lefts", "L1", {"extra": "changed"})
        tx.commit()
        after = c.rows("mv")[0]
        # Business aggregates and the row version are unaffected.
        self.assertEqual(after["n"], before["n"])
        self.assertEqual(after["amt_sum"], before["amt_sum"])
        self.assertEqual(after["version"], before["version"])


if __name__ == "__main__":
    unittest.main()
