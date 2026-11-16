"""Liveness and robustness: overlap without locks, busy is not a conflict."""

import os
import sqlite3
import tempfile
import threading
import time
import unittest

import serialcheck
from procutil import Worker
from support import TempRepo, account, ids, seed_accounts

from mrtx import BusyError, Client, ConflictError, eq


class TestNoWriteLockWhileThinking(unittest.TestCase):
    def test_other_writer_proceeds_while_transaction_is_open(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=1)), ("B", account(balance=1))])
            reader = c.begin()
            reader.read("accounts", where=eq("row_id", "A"),
                        columns=["row_id", "balance"])
            reader.stage_update("accounts", "A", {"balance": 5})
            # While reader is open and uncommitted, an independent process must
            # be able to complete a full write transaction promptly.
            with Worker(repo.path) as w:
                t = w.begin()
                w.read(t, "accounts")
                w.update(t, "accounts", "B", {"balance": 9})
                start = time.time()
                out = w.commit(t)
                elapsed = time.time() - start
            self.assertTrue(out["ok"], out)
            self.assertLess(elapsed, 2.0, "committing blocked on an open transaction")
            # reader can still commit (disjoint identity) afterwards
            self.assertEqual(reader.commit()["status"], "committed")
            final = {r["row_id"]: r for r in c.rows("accounts")}
            self.assertEqual(final["A"]["balance"], 5)
            self.assertEqual(final["B"]["balance"], 9)

    def test_begin_does_not_queue_on_open_transaction(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account())])
            held = c.begin()
            held.read("accounts")
            held.stage_update("accounts", "A", {"balance": 2})
            start = time.time()
            other = c.begin()
            other.read("accounts")
            elapsed = time.time() - start
            self.assertLess(elapsed, 1.0)
            other.abort()
            held.abort()

    def test_no_sqlite_write_lock_is_held_between_operations(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account())])
            tx = c.begin()
            tx.read("accounts")
            tx.stage_update("accounts", "A", {"balance": 2})
            # A separate raw connection can take an IMMEDIATE write lock.
            conn = sqlite3.connect(repo.path, timeout=1.0, isolation_level=None)
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("COMMIT")
            finally:
                conn.close()
            tx.commit()


class TestConcurrentThreads(unittest.TestCase):
    """Many overlapping transactions in threads; all must serialize correctly."""

    def test_threaded_mixed_workload(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [(rid, account(owner=rid.lower(), balance=i))
                              for i, rid in enumerate(["A", "B", "C", "D", "E", "F"])])
            errors = []
            decisions = {}

            def work(index):
                try:
                    client = Client(repo.path)
                    tx = client.begin()
                    tx.read("accounts", where=eq("owner", chr(ord("a") + index % 3)),
                            columns=["row_id", "balance"])
                    rid = "ABCDEF"[index % 6]
                    tx.stage_update("accounts", rid, {"balance": index})
                    decisions[index] = tx.commit()
                except ConflictError as exc:
                    decisions[index] = exc
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)

            threads = [threading.Thread(target=work, args=(i,)) for i in range(12)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(errors, [])
            history = serialcheck.load_history(repo.path)
            # 12 committed txns is too many for permutation enumeration; verify
            # the claimed serialization points instead, plus pairwise replay.
            order = serialcheck.claimed_serial_order(history)
            ok, detail = serialcheck.serial_valid(history, order)
            self.assertTrue(ok, detail)
            # final state is one of the per-identity last writers, no lost update
            committed = sum(1 for d in decisions.values() if isinstance(d, dict))
            final_versions = {r["row_id"]: r["version"] for r in c.rows("accounts")}
            self.assertEqual(max(final_versions.values()), committed + 1)


class TestBusyIsNotConflict(unittest.TestCase):
    def test_busy_error_is_typed_and_carries_no_evidence(self):
        with TempRepo() as repo:
            c = repo.client
            # Simulate an external long writer holding the SQLite write lock.
            hold = sqlite3.connect(repo.path, isolation_level=None)
            hold.execute("PRAGMA journal_mode=WAL")
            hold.execute("BEGIN IMMEDIATE")
            hold.execute("INSERT INTO meta(key, value) VALUES ('probe', '1')")
            try:
                fast = Client(repo.path, busy_timeout_ms=50, recover_on_start=False)
                fast.store.busy_retries = 2
                with self.assertRaises(BusyError) as info:
                    fast.begin()
                self.assertFalse(hasattr(info.exception, "evidence"))
            finally:
                hold.execute("ROLLBACK")
                hold.close()
            # the database is still usable and consistent
            tx = c.begin()
            tx.stage_insert("accounts", "Z", account())
            self.assertEqual(tx.commit()["status"], "committed")


class TestOverlapIsReal(unittest.TestCase):
    def test_two_transactions_read_same_rows_before_either_commits(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(active=True)), ("B", account(active=True))])
            with Worker(repo.path) as w1, Worker(repo.path) as w2:
                t1, t2 = w1.begin(), w2.begin()
                s1 = w1.status(t1)["status"]
                s2 = w2.status(t2)["status"]
                self.assertEqual(s1["state"], "active")
                self.assertEqual(s2["state"], "active")
                # both snapshots equal and both actually returned rows
                self.assertEqual(s1["snapshot_version"], s2["snapshot_version"])
                r1 = w1.read(t1, "accounts")["rows"]
                r2 = w2.read(t2, "accounts")["rows"]
                self.assertEqual(ids(r1), ids(r2))
                w1.abort(t1)
                w2.abort(t2)


if __name__ == "__main__":
    unittest.main()
