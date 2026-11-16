"""Version cleanup, recovery boundaries, evidence and idempotency."""

import os
import unittest

from support import TempRepo, account, ids, seed_accounts

from mrtx import Client, ConflictError, TransactionError, eq


class TestGC(unittest.TestCase):
    def test_gc_prunes_unreachable_versions_only(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            for i in range(1, 6):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            before = c.history("accounts", "A")
            self.assertEqual([h["version"] for h in before], [1, 2, 3, 4, 5, 6])
            info = c.gc_versions()
            self.assertEqual(info["active_snapshots"], [])
            after = c.history("accounts", "A")
            # No live snapshot exists, so only the newest version is retained.
            self.assertEqual([h["version"] for h in after], [6])
            self.assertEqual(c.rows("accounts")[0]["balance"], 5)

    def test_gc_keeps_version_needed_by_live_snapshot(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            for i in range(1, 5):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            reader = c.begin()  # snapshot version 5 (seed + 4 updates)
            self.assertEqual(reader.read("accounts")[0]["balance"], 4)
            for i in range(5, 9):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            info = c.gc_versions()
            self.assertEqual(info["safe_version"], 5)
            kept = [h["version"] for h in c.history("accounts", "A")]
            self.assertIn(5, kept)      # newest <= safe
            self.assertIn(9, kept)      # newest overall
            # the live reader still sees exactly its origin version
            self.assertEqual(reader.read("accounts")[0]["balance"], 4)
            # and either commits (it is read-only) or aborts
            reader.abort()

    def test_gc_never_changes_commit_decisions(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0)), ("B", account(balance=0))])
            for i in range(1, 4):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            t1 = c.begin()
            t1.read("accounts", where=eq("row_id", "B"))
            t1.stage_update("accounts", "B", {"balance": 99})
            t2 = c.begin()
            t2.stage_update("accounts", "B", {"balance": 1})
            t2.commit()
            c.gc_versions()
            # evidence-based validation is independent of pruned history
            with self.assertRaises(ConflictError) as info:
                t1.commit()
            self.assertEqual(info.exception.code, "write_conflict")

    def test_gc_does_not_prune_versions_above_safe_point(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            reader = c.begin()  # snapshot 1
            reader.read("accounts")
            for i in range(1, 4):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            c.gc_versions()
            kept = [h["version"] for h in c.history("accounts", "A")]
            self.assertEqual(kept, [1, 2, 3, 4])
            reader.abort()


class TestEvidence(unittest.TestCase):
    def test_conflict_evidence_names_transactions_rows_and_predicate(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A")), ("B", account(owner="B"))])
            t1 = c.begin()
            t1.read("accounts", where=eq("owner", "A"))
            t1.stage_update("accounts", "B", {"balance": 5})
            t2 = c.begin()
            t2.stage_update("accounts", "A", {"owner": "Q"})
            t2.commit()
            with self.assertRaises(ConflictError) as info:
                t1.commit()
            ev = info.exception.evidence
            self.assertEqual(ev["transaction"], t1.txid)
            conflict = ev["conflicts"][0]
            self.assertEqual(conflict["other_transaction"], t2.txid)
            self.assertEqual(conflict["row_id"], "A")
            self.assertIn("owner", conflict["predicate_expression"])
            self.assertTrue(conflict["witness"]["identity"] == ["accounts", "A"])
            # and the evidence is durably retrievable
            self.assertEqual(c.rejection_evidence(t1.txid)["code"], "read_conflict")

    def test_write_conflict_evidence_reports_versions(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=1))])
            t1 = c.begin()
            t1.stage_update("accounts", "A", {"balance": 2})
            t2 = c.begin()
            t2.stage_update("accounts", "A", {"balance": 3})
            t1.commit()
            with self.assertRaises(ConflictError) as info:
                t2.commit()
            conflict = info.exception.evidence["conflicts"][0]
            self.assertEqual(conflict["dependency"], "ww")
            self.assertEqual(conflict["other_transaction"], t1.txid)
            self.assertEqual(conflict["row_version_at_snapshot"], 1)
            self.assertGreater(conflict["other_commit_version"], 1)


class TestRealTimeOrder(unittest.TestCase):
    def test_later_start_sees_earlier_commit(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=1))])
            t1 = c.begin()
            t1.stage_update("accounts", "A", {"balance": 2})
            t1.commit()
            t2 = c.begin()
            self.assertEqual(t2.read("accounts")[0]["balance"], 2)
            self.assertGreaterEqual(t2.read("accounts")[0]["version"], 2)
            t2.abort()

    def test_serial_order_follows_commit_not_start(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            early = c.begin()   # starts first
            late = c.begin()    # starts second
            late.stage_update("accounts", "A", {"balance": 1})
            late.commit()
            early.stage_update("accounts", "A", {"balance": 2})
            with self.assertRaises(ConflictError):
                early.commit()   # cannot reorder before the already-committed one
            self.assertEqual(c.rows("accounts")[0]["balance"], 1)


class TestIdentityAndReplay(unittest.TestCase):
    def test_resend_commit_is_idempotent(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=1))])
            tx = c.begin()
            tx.stage_update("accounts", "A", {"balance": 2})
            first = tx.commit()
            again = c.commit(tx.handle)
            self.assertTrue(again["replayed"])
            self.assertEqual(again["commit_version"], first["commit_version"])
            self.assertEqual(len(c.rows("accounts")), 1)
            self.assertEqual(c.rows("accounts")[0]["balance"], 2)

    def test_abort_then_commit_is_stable(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            tx.stage_insert("accounts", "A", account())
            tx.abort()
            again = c.commit(tx.handle)
            self.assertTrue(again["replayed"])
            self.assertEqual(again["status"], "aborted")
            self.assertEqual(c.rows("accounts"), [])

    def test_transaction_id_cannot_be_reused(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            with self.assertRaises(TransactionError):
                c.begin(txid=tx.txid)
            tx.abort()


if __name__ == "__main__":
    unittest.main()
