"""Real OS-process drills for managed reference constraints.

Two worker processes overlap on the same source database and the parent
process orders the interleaving with :class:`threading.Barrier` so each
ordering is genuinely concurrent and reproducible.  The checks cover:

* deleting a parent while another process inserts a child -- both possible
  commit orders, neither ever leaving an orphan or silently deleting unseen
  data;
* a cascade delete racing an update of the child it would remove;
* two completely unrelated parent/child groups committing concurrently;
* a write transaction that began before a constraint was registered, in a
  different process, being checked when it finally commits;
* crash/replay: a source commit resent after the worker process exited, and a
  replica-local commit re-delivered after an unacknowledged apply.

Every final-state expectation is computed here from the public ``rows`` /
``history`` APIs, not from the product's cascade planner.
"""

import os
import threading
import unittest

from procutil import Worker
from support import TempRepo

from mrtx import Client, Replica


class ProcessRefCase(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        self.workers = []

    def tearDown(self):
        for w in self.workers:
            w.close()
        self.repo.__exit__(None, None, None)

    def worker(self):
        w = Worker(self.repo.path)
        self.workers.append(w)
        return w

    def schema(self):
        self.c.create_table("parents", [("name", "text")])
        self.c.create_table("children",
                            [("parent_id", "text"), ("note", "text")])

    def seed(self, parent_id, child_id):
        tx = self.c.begin()
        tx.stage_insert("parents", parent_id, {"name": parent_id})
        tx.stage_insert("children", child_id,
                        {"parent_id": parent_id, "note": "seed"})
        tx.commit()

    def orphans(self):
        parents = {r["row_id"] for r in self.c.rows("parents")}
        return [r["row_id"] for r in self.c.rows("children")
                if r["parent_id"] is not None and r["parent_id"] not in parents]

    def run_ordered(self, first, second):
        """Run two staged worker transactions in a deterministic commit order.

        Each argument is ``(worker, txid, commit_callable)``.  Both transactions
        are already staged by the caller.  A three-party barrier parks both
        workers after staging; the orchestrator then releases *first* alone,
        waits for it to finish, and only then releases *second*, so the two
        real-process interleavings are reproducible without scheduler luck.
        """
        staged = threading.Barrier(3)
        gate_first = threading.Event()
        gate_second = threading.Event()
        outcomes = {}

        def driver(slot, action, gate):
            staged.wait()
            gate.wait()
            outcomes[slot] = action()

        threads = [
            threading.Thread(target=driver, args=("first", first[2], gate_first)),
            threading.Thread(target=driver, args=("second", second[2], gate_second)),
        ]
        for t in threads:
            t.start()
        staged.wait()
        gate_first.set()
        threads[0].join()
        gate_second.set()
        threads[1].join()
        return outcomes["first"], outcomes["second"]


class TestDeleteInsertRace(ProcessRefCase):
    def _setup_pair(self, pid, cid):
        self.seed(pid, cid)
        wd, wi = self.worker(), self.worker()
        td, ti = wd.begin(), wi.begin()
        wd.delete(td, "parents", pid)
        wi.insert(ti, "children", cid + "-new",
                  {"parent_id": pid, "note": "new"})
        return (wd, td, lambda: wd.commit(td)), (wi, ti, lambda: wi.commit(ti))

    def test_cascade_delete_first_then_insert_loses(self):
        self.schema()
        self.c.register_reference("ref", "parents", "children",
                                  "parent_id", "cascade")
        pid, cid = "P1", "C1"
        delete_pair, insert_pair = self._setup_pair(pid, cid)
        first, second = self.run_ordered(delete_pair, insert_pair)
        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["evidence"]["code"], "reference_violation")
        self.assertEqual(self.orphans(), [])
        # The parent and its seed child are gone; the rejected insert never landed.
        self.assertNotIn(pid, {r["row_id"] for r in self.c.rows("parents")})
        self.assertEqual(
            {r["row_id"] for r in self.c.rows("children")}, set())

    def test_cascade_insert_first_then_cascade_conflict(self):
        self.schema()
        self.c.register_reference("ref", "parents", "children",
                                  "parent_id", "cascade")
        pid, cid = "P2", "C2"
        delete_pair, insert_pair = self._setup_pair(pid, cid)
        first, second = self.run_ordered(insert_pair, delete_pair)
        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["evidence"]["code"], "cascade_conflict")
        self.assertEqual(self.orphans(), [])
        # The inserted child survives and the parent was not deleted.
        child_ids = {r["row_id"] for r in self.c.rows("children")}
        self.assertIn(cid + "-new", child_ids)
        self.assertIn(pid, {r["row_id"] for r in self.c.rows("parents")})

    def test_restrict_delete_always_loses_insert_succeeds_no_orphan(self):
        self.schema()
        self.c.register_reference("ref", "parents", "children",
                                  "parent_id", "restrict")
        pid, cid = "P3", "C3"
        delete_pair, insert_pair = self._setup_pair(pid, cid)
        # The seed child C3 is a surviving restrict referrer, so the parent
        # delete can never commit in either ordering; the concurrent insert
        # points at a parent that therefore stays alive.
        first, second = self.run_ordered(delete_pair, insert_pair)
        self.assertFalse(first["ok"])
        self.assertEqual(first["evidence"]["code"], "restrict_violation")
        self.assertTrue(second["ok"])
        self.assertEqual(self.orphans(), [])
        # Both children remain and the parent was never removed.
        self.assertEqual(
            {r["row_id"] for r in self.c.rows("children")},
            {cid, cid + "-new"})
        self.assertIn(pid, {r["row_id"] for r in self.c.rows("parents")})

    def test_unrelated_groups_both_commit(self):
        self.schema()
        self.c.register_reference("ref", "parents", "children",
                                  "parent_id", "cascade")
        w1, w2 = self.worker(), self.worker()
        t1, t2 = w1.begin(), w2.begin()
        barrier = threading.Barrier(2)
        results = []

        def group_a():
            w1.insert(t1, "parents", "GA", {"name": "GA"})
            w1.insert(t1, "children", "GA-c",
                      {"parent_id": "GA", "note": "a"})
            barrier.wait()
            results.append(("A", w1.commit(t1)))

        def group_b():
            w2.insert(t2, "parents", "GB", {"name": "GB"})
            w2.insert(t2, "children", "GB-c",
                      {"parent_id": "GB", "note": "b"})
            barrier.wait()
            results.append(("B", w2.commit(t2)))

        th1, th2 = threading.Thread(target=group_a), threading.Thread(
            target=group_b)
        th1.start()
        th2.start()
        th1.join()
        th2.join()
        self.assertTrue(all(out["ok"] for _, out in results), results)
        self.assertEqual(
            {r["row_id"] for r in self.c.rows("children")},
            {"GA-c", "GB-c"})


class TestCascadeUpdateRace(ProcessRefCase):
    def _setup_pair(self, pid, cid):
        self.seed(pid, cid)
        wd, wu = self.worker(), self.worker()
        td, tu = wd.begin(), wu.begin()
        wd.delete(td, "parents", pid)
        wu.update(tu, "children", cid, {"note": "changed"})
        return (wd, td, lambda: wd.commit(td)), (wu, tu, lambda: wu.commit(tu))

    def test_cascade_first_child_update_gets_write_conflict(self):
        self.schema()
        self.c.register_reference("ref", "parents", "children",
                                  "parent_id", "cascade")
        pid, cid = "U1", "UC1"
        delete_pair, update_pair = self._setup_pair(pid, cid)
        first, second = self.run_ordered(delete_pair, update_pair)
        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["evidence"]["code"], "write_conflict")
        self.assertEqual(self.orphans(), [])

    def test_update_first_cascade_gets_cascade_conflict(self):
        self.schema()
        self.c.register_reference("ref", "parents", "children",
                                  "parent_id", "cascade")
        pid, cid = "U2", "UC2"
        delete_pair, update_pair = self._setup_pair(pid, cid)
        first, second = self.run_ordered(update_pair, delete_pair)
        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["evidence"]["code"], "cascade_conflict")
        self.assertEqual(self.orphans(), [])
        # The surviving child keeps the concurrent update; its parent remains.
        child = next(r for r in self.c.rows("children") if r["row_id"] == cid)
        self.assertEqual(child["note"], "changed")
        self.assertIn(pid, {r["row_id"] for r in self.c.rows("parents")})


class TestRegistrationRace(ProcessRefCase):
    def test_write_started_before_registration_in_other_process(self):
        self.schema()
        # No constraint yet.  A worker begins a transaction and stages an
        # orphan-shaped child.
        w = self.worker()
        txid = w.begin()
        w.insert(txid, "parents", "P1", {"name": "p"})
        w.insert(txid, "children", "C-bad",
                 {"parent_id": "GONE", "note": "x"})
        # Another process registers the rule while that transaction is open.
        registrar = self.worker()
        out = registrar.register_reference(
            "ref", "parents", "children", "parent_id", "cascade")
        self.assertTrue(out["ok"], out)
        # The in-flight transaction now commits under the new rule and loses.
        decision = w.commit(txid)
        self.assertFalse(decision["ok"])
        self.assertEqual(decision["evidence"]["code"], "reference_violation")
        self.assertEqual(
            {r["row_id"] for r in self.c.rows("children")}, set())

    def test_registration_orphan_rejection_is_process_local_and_clean(self):
        self.schema()
        tx = self.c.begin()
        tx.stage_insert("children", "orph",
                        {"parent_id": "GHOST", "note": "x"})
        tx.commit()
        w = self.worker()
        out = w.register_reference(
            "ref", "parents", "children", "parent_id", "cascade")
        self.assertFalse(out["ok"])
        self.assertEqual(out["error"], "ReferenceError")
        fresh = Client(self.repo.path)
        self.assertEqual(fresh.list_references(), [])


class TestReplayAcrossProcesses(ProcessRefCase):
    def test_source_commit_resend_after_process_exit(self):
        self.schema()
        self.c.register_reference("ref", "parents", "children",
                                  "parent_id", "cascade")
        self.seed("P1", "C1")
        w = self.worker()
        txid = w.begin()
        w.delete(txid, "parents", "P1")
        first = w.commit(txid)
        self.assertTrue(first["ok"])
        position = first["result"]["commit_version"]
        first_cascade = first["result"]["cascade_deletes"]
        # The worker exits; a brand new process re-sends the same commit using
        # only the serialized handle.
        handle = w.handles[txid]
        w.close()
        self.workers.remove(w)
        from mrtx.engine import TransactionHandle
        outcome = Client(self.repo.path).commit(
            TransactionHandle.from_dict(handle))
        self.assertTrue(outcome["replayed"])
        self.assertEqual(outcome["commit_version"], position)
        self.assertEqual(
            [(x["table"], x["row_id"]) for x in outcome["cascade_deletes"]],
            [(x["table"], x["row_id"]) for x in first_cascade])
        # Nothing was deleted a second time and the position did not advance.
        self.assertEqual(self.c.current_version(), position)
        self.assertEqual(self.c.rows("children"), [])
        self.assertEqual(self.c.rows("parents"), [])

    def test_replica_redelivery_after_unacknowledged_local_commit(self):
        from mrtx import SubscriptionHandle
        self.schema()
        self.c.register_reference("ref", "parents", "children",
                                  "parent_id", "cascade")
        self.seed("P1", "C1")
        sub, _cut = self.c.create_subscription("sub", ["parents", "children"])
        replica_path = os.path.join(self.repo.dir, "replica.db")
        rep = Replica(replica_path, sub.handle)
        rep.run(self.c)
        # A real cross-table cascade on the source.
        outcome = self.c.begin()
        outcome.stage_delete("parents", "P1")
        committed = outcome.commit()
        position = committed["commit_version"]
        # Apply locally but do not acknowledge, then simulate a crash by
        # dropping the replica object and reopening against the same files.
        batch = rep.apply_local(self.c)
        self.assertEqual(batch["position"], position)
        del rep
        reopened = Replica(replica_path, SubscriptionHandle.from_dict(
            sub.handle.as_dict()))
        # The source still has the same pending batch; re-pull is identical.
        pulled = self.c.pull(sub.handle)["batch"]
        self.assertEqual(pulled["position"], position)
        report = reopened.apply_local(self.c)
        self.assertTrue(report["replayed"])  # recognized, not applied twice
        reopened.acknowledge(self.c)
        self.assertEqual(reopened.rows("parents"), [])
        self.assertEqual(reopened.rows("children"), [])
        # The source position never advanced beyond the one real commit.
        self.assertEqual(self.c.current_version(), position)


if __name__ == "__main__":
    unittest.main()
