"""Retention and cleanup: slow consumers, full-phase needs, closed releases.

The tests inspect concrete row counts and version sets, and prove that
cleanup reclaims exactly what no open subscription still needs.
"""

import sqlite3
import unittest

from support import TempRepo, account, drain_changes, drain_full, seed_accounts

from mrtx import Client


class TestFullPhaseRetention(unittest.TestCase):
    def test_gc_keeps_the_cut_version_of_an_unfinished_full_load(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            # The slow subscription fixes S, then never pulls its full load.
            slow, cut_slow = c.create_subscription("slow", ["accounts"])
            for i in range(1, 6):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            info = c.gc_versions()
            self.assertIn(cut_slow, info["full_phase_cuts"])
            self.assertEqual(info["safe_version"], cut_slow)
            kept = [h["version"] for h in c.history("accounts", "A")]
            # Every version above the cut plus the newest <= cut must survive.
            self.assertIn(cut_slow, kept)
            self.assertEqual(max(kept), 6)
            # The full load still yields the S-time image.
            items = drain_full(c, slow.handle)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["version"], cut_slow)
            self.assertEqual(items[0]["values"]["balance"], 0)

    def test_full_phase_need_is_released_when_full_load_finishes(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            sub, cut = c.create_subscription("s", ["accounts"])
            for i in range(1, 4):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            before = c.gc_versions()
            self.assertEqual(before["safe_version"], cut)
            drain_full(c, sub.handle)
            # Now in incremental phase; its need is expressed by ack_version,
            # not by a row-history snapshot.  GC may advance fully.
            after = c.gc_versions()
            self.assertEqual(after["full_phase_cuts"], [])
            self.assertEqual(after["safe_version"], 4)


class TestSlowConsumerRetention(unittest.TestCase):
    def _two_consumers(self, repo):
        c = repo.client
        seed_accounts(c, [("A", account(balance=0))])
        fast, _ = c.create_subscription("fast", ["accounts"])
        slow, _ = c.create_subscription("slow", ["accounts"])
        drain_full(c, fast.handle)
        drain_full(c, slow.handle)
        return c, fast, slow

    def test_cleanup_is_bounded_by_the_slowest_open_consumer(self):
        with TempRepo() as repo:
            c, fast, slow = self._two_consumers(repo)
            for i in range(1, 6):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            # fast drains everything.
            fast_changes = drain_changes(c, fast.handle)
            self.assertEqual(sum(len(items) for _, items in fast_changes), 5)
            # Cleanup while slow is still at position 1 must reclaim nothing.
            report = c.cleanup_sync_history()
            self.assertEqual(report["retention_boundary"], 1)
            self.assertEqual(report["reclaimed_changes"], 0)
            self.assertEqual(report["remaining_changes"], 5)
            # The slow consumer still receives every change.
            slow_changes = drain_changes(c, slow.handle)
            self.assertEqual([i["after"]["values"]["balance"]
                              for _, items in slow_changes for i in items],
                             [1, 2, 3, 4, 5])
            final = c.cleanup_sync_history()
            self.assertEqual(final["reclaimed_changes"], 5)
            self.assertEqual(final["remaining_changes"], 0)
            self.assertEqual(final["retention_boundary"], 6)

    def test_closing_a_slow_consumer_releases_its_retention(self):
        with TempRepo() as repo:
            c, fast, slow = self._two_consumers(repo)
            for i in range(1, 4):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            drain_changes(c, fast.handle)
            self.assertEqual(c.cleanup_sync_history()["reclaimed_changes"], 0)
            c.close_subscription(slow.handle)
            report = c.cleanup_sync_history()
            self.assertEqual(report["reclaimed_changes"], 3)
            self.assertEqual(report["remaining_changes"], 0)
            # fast's already-acknowledged position is unaffected.
            self.assertTrue(c.pull(fast.handle)["caught_up"])

    def test_cleanup_never_removes_a_pending_batch_payload(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            sub, cut = c.create_subscription("s", ["accounts"])
            drain_full(c, sub.handle)
            tx = c.begin()
            tx.stage_update("accounts", "A", {"balance": 3})
            tx.commit()
            pending = c.pull(sub.handle)["batch"]
            c.close_subscription(sub.handle)
            c.cleanup_sync_history()
            # A closed subscription's private copy is released, and the source
            # change log is reclaimable because no open subscription needs it.
            status = c.subscription_status(sub.handle)
            self.assertEqual(status["state"], "closed")
            self.assertIsNone(status["pending_batch"])

    def test_no_open_subscription_makes_the_whole_log_reclaimable(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            sub, cut = c.create_subscription("s", ["accounts"])
            drain_full(c, sub.handle)
            for i in range(1, 4):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            c.close_subscription(sub.handle)
            report = c.cleanup_sync_history()
            self.assertEqual(report["reclaimed_changes"], 3)
            self.assertEqual(report["open_subscriptions"], 0)


class TestNoLongLockDuringFullScan(unittest.TestCase):
    def test_other_process_can_commit_between_pages(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("r%02d" % i, account(balance=i)) for i in range(10)])
            sub, cut = c.create_subscription("s", ["accounts"])
            b = c.pull(sub.handle, max_rows=2)["batch"]
            c.ack(sub.handle, b["batch_id"])
            # A separate sqlite connection can take the write lock right now,
            # proving no read transaction or lock spans pages.
            conn = sqlite3.connect(repo.path, timeout=1.0, isolation_level=None)
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("COMMIT")
            finally:
                conn.close()
            # And an independent client commits a real transaction.
            other = Client(repo.path)
            tx = other.begin()
            tx.stage_update("accounts", "r05", {"balance": 500})
            self.assertEqual(tx.commit()["status"], "committed")
            # The full load still yields the S image, not 500.
            rest = []
            while True:
                batch = c.pull(sub.handle, max_rows=2)["batch"]
                rest.extend(batch["items"])
                c.ack(sub.handle, batch["batch_id"])
                if batch["full_complete"]:
                    break
            values = {i["row_id"]: i["values"]["balance"]
                      for i in b["items"] + rest}
            self.assertEqual(values["r05"], 5)


if __name__ == "__main__":
    unittest.main()
