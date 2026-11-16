"""Process-level drills: real OS processes, barrier-controlled ordering.

Each scenario records the actual read results, the returned commit decisions
and the final rows, enumerates serial orders of the committed history with the
independent checker in ``serialcheck.py``, and asserts that every successful
history has a matching serial order.  The checker never calls product conflict
code.
"""

import json
import os
import time
import unittest

import serialcheck
from procutil import Worker
from support import TempRepo, account, seed_accounts

from mrtx import Client, ConflictError, eq, lt, not_


def pred_eq(col, value):
    return {"op": "eq", "col": col, "value": value}


class Drill(unittest.TestCase):
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

    def assert_serializable(self, history=None):
        history = history or serialcheck.load_history(self.repo.path)
        valid, counter = serialcheck.enumerate_serial_orders(history)
        self.assertTrue(valid, "no serial order explains history: %r" % (counter,))
        order = serialcheck.claimed_serial_order(history)
        ok, detail = serialcheck.serial_valid(history, order)
        self.assertTrue(ok, "claimed serial order is not a serial order: %r" % (detail,))
        return history, valid

    def final(self, table="accounts"):
        return {r["row_id"]: r for r in self.c.rows(table)}


class TestActives(Drill):
    def test_two_processes_deactivate_one_each(self):
        seed_accounts(self.c, [("A", account(active=True)), ("B", account(active=True))])
        w1, w2 = self.worker(), self.worker()
        t1, t2 = w1.begin(), w2.begin()
        r1 = w1.read(t1, "accounts", where=pred_eq("active", True))["rows"]
        r2 = w2.read(t2, "accounts", where=pred_eq("active", True))["rows"]
        # Both processes really saw both rows before either wrote.
        self.assertEqual([r["row_id"] for r in r1], ["A", "B"])
        self.assertEqual([r["row_id"] for r in r2], ["A", "B"])
        self.assertTrue(w1.update(t1, "accounts", "A", {"active": False})["ok"])
        self.assertTrue(w2.update(t2, "accounts", "B", {"active": False})["ok"])
        d1 = w1.commit(t1)
        d2 = w2.commit(t2)
        self.assertTrue(d1["ok"])
        self.assertFalse(d2["ok"])
        self.assertEqual(d2["error"], "conflict")
        self.assertEqual(d2["evidence"]["conflicts"][0]["dependency"], "rw")
        self.assertIn(d2["evidence"]["conflicts"][0]["row_id"], ("A", "B"))
        final = self.final()
        self.assertTrue(any(r["active"] for r in final.values()))
        self.assertEqual(len([r for r in final.values() if r["active"]]), 1)
        self.assert_serializable()

    def test_both_commits_recorded_as_decisions(self):
        seed_accounts(self.c, [("A", account(active=True)), ("B", account(active=True))])
        w1, w2 = self.worker(), self.worker()
        t1, t2 = w1.begin(), w2.begin()
        for w, t in ((w1, t1), (w2, t2)):
            w.read(t, "accounts", where=pred_eq("active", True))
        w1.update(t1, "accounts", "A", {"active": False})
        w2.update(t2, "accounts", "B", {"active": False})
        self.assertTrue(w1.commit(t1)["ok"])
        self.assertFalse(w2.commit(t2)["ok"])
        s1 = w1.status(t1)["status"]
        s2 = w2.status(t2)["status"]
        self.assertTrue(s1["committed"])
        self.assertEqual(s2["decision"], "rejected")


class TestPhantomAcrossProcesses(Drill):
    def test_empty_predicate_then_concurrent_insert(self):
        seed_accounts(self.c, [("A", account(owner="A", active=True))])
        w1, w2 = self.worker(), self.worker()
        t1, t2 = w1.begin(), w2.begin()
        empty = w1.read(t1, "accounts", where={"op": "eq", "col": "owner", "value": "ghost"})
        self.assertEqual(empty["rows"], [])
        # A reader that also writes must not have its empty-range observation
        # invalidated by a concurrent insert.
        w1.update(t1, "accounts", "A", {"balance": 2})
        self.assertTrue(w2.insert(t2, "accounts", "G", account(owner="ghost"))["ok"])
        self.assertTrue(w2.commit(t2)["ok"])
        d = w1.commit(t1)
        self.assertFalse(d["ok"])
        self.assertEqual(d["code"], "read_conflict")
        ev = d["evidence"]["conflicts"][0]
        self.assertEqual(ev["other_transaction"], t2)
        self.assertEqual(ev["row_id"], "G")
        self.assertTrue(ev["witness"]["predicate"].startswith("owner"))
        self.assertEqual(set(self.final()), {"A", "G"})
        self.assert_serializable()


class TestDisjointRangesAcrossProcesses(Drill):
    def test_two_disjoint_ranges_both_commit(self):
        rows = [("%03d" % i, account(owner="o%d" % i, balance=i)) for i in range(8)]
        seed_accounts(self.c, rows)
        w1, w2 = self.worker(), self.worker()
        t1, t2 = w1.begin(), w2.begin()
        left = w1.read(t1, "accounts", where={"op": "lt", "col": "row_id", "value": "004"},
                       columns=["row_id", "balance"])
        right = w2.read(t2, "accounts",
                        where={"op": "not", "part": {"op": "lt", "col": "row_id",
                                                     "value": "004"}},
                        columns=["row_id", "balance"])
        self.assertEqual(len(left["rows"]), 4)
        self.assertEqual(len(right["rows"]), 4)
        w1.update(t1, "accounts", "000", {"balance": 100})
        w2.update(t2, "accounts", "007", {"balance": 700})
        d1 = w1.commit(t1)
        d2 = w2.commit(t2)
        self.assertTrue(d1["ok"], d1)
        self.assertTrue(d2["ok"], d2)  # no replay, no conflict
        final = self.final()
        self.assertEqual(final["000"]["balance"], 100)
        self.assertEqual(final["007"]["balance"], 700)
        self.assert_serializable()

    def test_three_transactions_three_disjoint_ranges(self):
        rows = [("%02d" % i, account(owner="o", balance=i)) for i in range(9)]
        seed_accounts(self.c, rows)
        ws = [self.worker() for _ in range(3)]
        ts = [w.begin() for w in ws]
        specs = (
            (ws[0], ts[0], {"op": "lt", "col": "row_id", "value": "03"}),
            (ws[1], ts[1], {"op": "and", "parts": [
                {"op": "not", "part": {"op": "lt", "col": "row_id", "value": "03"}},
                {"op": "lt", "col": "row_id", "value": "06"}]}),
            (ws[2], ts[2], {"op": "not", "part": {"op": "lt", "col": "row_id",
                                                  "value": "06"}}),
        )
        for w, t, where in specs:
            w.read(t, "accounts", where=where, columns=["row_id", "balance"])
        ws[0].update(ts[0], "accounts", "01", {"balance": 901})
        ws[1].update(ts[1], "accounts", "04", {"balance": 904})
        ws[2].update(ts[2], "accounts", "07", {"balance": 907})
        decisions = [w.commit(t) for w, t in zip(ws, ts)]
        self.assertTrue(all(d["ok"] for d in decisions),
                        [d for d in decisions if not d["ok"]])
        self.assert_serializable()


class TestSameKeyAcrossProcesses(Drill):
    def test_same_row_update_one_wins(self):
        seed_accounts(self.c, [("A", account(balance=1))])
        w1, w2 = self.worker(), self.worker()
        t1, t2 = w1.begin(), w2.begin()
        r1 = w1.read(t1, "accounts")["rows"][0]
        r2 = w2.read(t2, "accounts")["rows"][0]
        self.assertEqual(r1["version"], r2["version"])
        w1.update(t1, "accounts", "A", {"balance": 2}, expected_version=r1["version"])
        w2.update(t2, "accounts", "A", {"balance": 3}, expected_version=r2["version"])
        self.assertTrue(w1.commit(t1)["ok"])
        d2 = w2.commit(t2)
        self.assertFalse(d2["ok"])
        self.assertEqual(d2["code"], "write_conflict")
        self.assertEqual(d2["evidence"]["conflicts"][0]["row_id"], "A")
        self.assertEqual(d2["evidence"]["conflicts"][0]["other_transaction"], t1)
        self.assertEqual(self.final()["A"]["balance"], 2)
        self.assert_serializable()


class TestReadYourWritesAcrossProcesses(Drill):
    def test_same_process_read_your_writes_and_second_process_isolation(self):
        seed_accounts(self.c, [("A", account(balance=1))])
        w1, w2 = self.worker(), self.worker()
        t1 = w1.begin()
        w1.update(t1, "accounts", "A", {"balance": 5})
        own = w1.read(t1, "accounts")["rows"][0]
        self.assertEqual(own["balance"], 5)
        self.assertLess(own["version"], 0)
        t2 = w2.begin()
        theirs = w2.read(t2, "accounts")["rows"][0]
        self.assertEqual(theirs["balance"], 1)  # uncommitted staging is invisible
        self.assertEqual(theirs["version"], 1)
        w1.commit(t1)
        # t2's fixed view still shows version 1
        self.assertEqual(w2.read(t2, "accounts")["rows"][0]["balance"], 1)
        w2.abort(t2)
        self.assert_serializable()


class TestAbortAcrossProcesses(Drill):
    def test_abort_leaves_no_writes_and_no_dependency(self):
        seed_accounts(self.c, [("A", account(balance=1))])
        w1, w2 = self.worker(), self.worker()
        t1, t2 = w1.begin(), w2.begin()
        w1.read(t1, "accounts")
        w2.read(t2, "accounts")
        w1.update(t1, "accounts", "A", {"balance": 99})
        w1.abort(t1)
        w2.update(t2, "accounts", "A", {"balance": 2})
        self.assertTrue(w2.commit(t2)["ok"])
        self.assertEqual(self.final()["A"]["balance"], 2)
        self.assertEqual(w1.status(t1)["status"]["decision"], "aborted")
        self.assert_serializable()


class TestCrashAfterCommitBeforeResponse(Drill):
    def test_new_process_recovers_committed_decision(self):
        seed_accounts(self.c, [("A", account(balance=1))])
        w1 = self.worker()
        t1 = w1.begin()
        token = w1.handles[t1]["token"]
        w1.read(t1, "accounts")
        w1.update(t1, "accounts", "A", {"balance": 42})
        # The worker commits and exits before writing any response.
        with self.assertRaises(RuntimeError):
            w1.send({"cmd": "commit_crash", "txid": t1})

        # A brand-new process (and a fresh Client) sees the durable decision.
        reader = self.worker()
        status = reader.status(t1)["status"]
        self.assertEqual(status["decision"], "committed")
        self.assertTrue(status["committed"])
        self.assertEqual(status["commit_version"], 2)
        self.assertEqual(self.final()["A"]["balance"], 42)

        # Re-sending commit from this process with the same id is idempotent.
        from mrtx.engine import TransactionHandle
        handle = TransactionHandle(t1, token, self.repo.path)
        replay = self.c.commit(handle)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["status"], "committed")
        self.assertEqual(replay["commit_version"], status["commit_version"])
        self.assertEqual(len(self.c.rows("accounts")), 1)
        self.assert_serializable()

    def test_crash_before_decision_reports_not_committed(self):
        seed_accounts(self.c, [("A", account(balance=1))])
        w1 = self.worker()
        t1 = w1.begin()
        token = w1.handles[t1]["token"]
        w1.update(t1, "accounts", "A", {"balance": 7})
        w1.kill()  # SIGKILL: no decision, no visible write
        reader = self.worker()
        self.assertEqual(reader.rows("accounts")[0]["balance"], 1)
        self.c.recover()
        status = reader.status(t1)["status"]
        self.assertEqual(status["decision"], "aborted_orphan")
        self.assertFalse(status["committed"])
        from mrtx.engine import TransactionHandle

        from mrtx import TransactionError
        with self.assertRaises(TransactionError):
            self.c.commit(TransactionHandle(t1, token, self.repo.path))
        # The staged write can never become visible.
        self.assertEqual(self.final()["A"]["balance"], 1)
        ok = self.c.begin()
        cur = ok.read("accounts")[0]
        self.assertEqual(cur["balance"], 1)
        ok.abort()


class TestCrossProcessReplay(Drill):
    def test_shared_handle_replay_is_idempotent(self):
        seed_accounts(self.c, [("A", account(balance=1))])
        w1 = self.worker()
        t1 = w1.begin()
        from mrtx.engine import TransactionHandle
        h = TransactionHandle.from_dict(w1.handles[t1])
        w1.update(t1, "accounts", "A", {"balance": 42})
        self.assertTrue(w1.commit(t1)["ok"])
        # A different process, holding only the serialized handle, replays.
        replay = Client(self.repo.path).commit(h)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["status"], "committed")
        self.assertEqual(len(self.c.rows("accounts")), 1)
        self.assertEqual(self.final()["A"]["balance"], 42)

    def test_handle_cannot_be_rebound_to_another_operation_set(self):
        seed_accounts(self.c, [("A", account(balance=1))])
        w1 = self.worker()
        t1 = w1.begin()
        from mrtx.engine import TransactionHandle
        h = TransactionHandle.from_dict(w1.handles[t1])
        w1.update(t1, "accounts", "A", {"balance": 2})
        w1.commit(t1)
        # The token from a different transaction must not work.
        bad = TransactionHandle(t1, "0" * 32, self.repo.path)
        from mrtx import TransactionError
        with self.assertRaises(TransactionError):
            self.c.commit(bad)
