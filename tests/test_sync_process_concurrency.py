"""Concurrency and process-level subscription invariants.

* A source commit resent by a *different* process does not create a second
  event.
* Cleanup running concurrently with pulls never loses or duplicates changes.
* A pending batch built by one process is preserved for another process after
  the first dies, with identical identity and content.
"""

import os
import threading
import unittest

from procutil import Worker
from support import TempRepo, account, seed_accounts

from mrtx import Client, SubscriptionHandle


class TestConcurrentCleanup(unittest.TestCase):
    def test_no_lost_or_duplicated_changes_under_concurrent_cleanup(self):
        with TempRepo() as repo:
            c = repo.client
            c.create_table("counter", [("v", "int64")])
            tx = c.begin()
            tx.stage_insert("counter", "x", {"v": 0})
            tx.commit()
            sub, _ = c.create_subscription("s", ["counter"])
            while True:
                b = c.pull(sub.handle, max_rows=10)["batch"]
                c.ack(sub.handle, b["batch_id"])
                if b["full_complete"]:
                    break

            delivered = []
            received_all = threading.Event()
            errors = []

            def writer():
                cc = Client(repo.path)
                for _ in range(30):
                    while True:
                        try:
                            tx = cc.begin()
                            row = tx.read("counter")[0]
                            tx.stage_update("counter", "x", {"v": row["v"] + 1},
                                            expected_version=row["version"])
                            tx.commit()
                            break
                        except Exception:
                            continue
                    cc.cleanup_sync_history()
                    cc.gc_versions()

            thread = threading.Thread(target=writer)
            thread.start()
            try:
                while True:
                    result = c.pull(sub.handle, max_rows=100)
                    if result["batch"] is None:
                        if not thread.is_alive():
                            break
                        continue
                    delivered.extend(i["after"]["values"]["v"]
                                     for i in result["batch"]["items"])
                    c.ack(sub.handle, result["batch"]["batch_id"])
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                thread.join()
            self.assertEqual(errors, [])
            # Drain anything appended after the writer finished.
            while True:
                result = c.pull(sub.handle)
                if result["batch"] is None:
                    break
                delivered.extend(i["after"]["values"]["v"]
                                 for i in result["batch"]["items"])
                c.ack(sub.handle, result["batch"]["batch_id"])
            self.assertEqual(delivered, sorted(delivered), "commit order preserved")
            self.assertEqual(delivered, list(range(1, 31)))
            self.assertEqual(c.rows("counter")[0]["v"], 30)


class TestProcessResendAndPending(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        seed_accounts(self.c, [("A", account(balance=0))])
        self.sub, self.cut = self.c.create_subscription("s", ["accounts"])
        self.workers = []

    def tearDown(self):
        for w in self.workers:
            w.close()
        self.repo.__exit__(None, None, None)

    def worker(self):
        w = Worker(self.repo.path)
        self.workers.append(w)
        return w

    def test_resend_from_another_process_creates_one_event(self):
        c = self.c
        # Drain the full load in-process.
        while True:
            b = c.pull(self.sub.handle, max_rows=100)["batch"]
            c.ack(self.sub.handle, b["batch_id"])
            if b["full_complete"]:
                break
        w = self.worker()
        t = w.begin()
        w.update(t, "accounts", "A", {"balance": 5})
        first = w.commit(t)
        self.assertTrue(first["ok"])
        # Another process re-sends the same transaction id.
        from mrtx.engine import TransactionHandle
        ack_handle = SubscriptionHandle.from_dict(self.sub.handle.as_dict())
        again = c.commit(TransactionHandle.from_dict(w.handles[t]))
        self.assertTrue(again["replayed"])
        result = c.pull(ack_handle)["batch"]
        self.assertEqual(result["row_count"], 1)
        self.assertEqual(result["position"], first["result"]["commit_version"])
        c.ack(ack_handle, result["batch_id"])
        self.assertTrue(c.pull(ack_handle)["caught_up"])

    def test_pending_batch_survives_process_death(self):
        c = self.c
        consumer = self.worker()
        handle = self.sub.handle.as_dict()
        first = consumer.pull(handle, max_rows=1)["result"]["batch"]
        consumer.kill()
        fresh = self.worker()
        again = fresh.pull(handle, max_rows=500)["result"]["batch"]
        self.assertEqual(again["batch_id"], first["batch_id"])
        self.assertEqual(again["items"], first["items"])
        # A third process acknowledges the exact batch the first produced.
        out = fresh.send({"cmd": "ack", "handle": handle,
                          "batch_id": first["batch_id"]})
        self.assertTrue(out["ok"], out)
        self.assertEqual(out["result"]["acknowledged_position"], self.cut)


if __name__ == "__main__":
    unittest.main()
