"""Edge cases around preconditions, typed reads, and evidence after GC."""

import unittest

import serialcheck
from support import TempRepo, account, ids, seed_accounts

from mrtx import (ConflictError, PredicateError, SchemaError, TransactionError,
                  and_, eq, is_null, le, lt, not_, or_)


class TestPreconditions(unittest.TestCase):
    def test_precondition_that_holds_at_stage_but_breaks_before_commit(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=1))])
            t1 = c.begin()
            row = t1.read("accounts", where=eq("row_id", "A"))[0]
            t1.stage_update("accounts", "A", {"balance": 2},
                            expected_version=row["version"])
            t2 = c.begin()
            t2.stage_update("accounts", "A", {"balance": 3})
            t2.commit()
            with self.assertRaises(ConflictError) as info:
                t1.commit()
            ev = info.exception.evidence
            self.assertEqual(ev["conflicts"][0]["dependency"], "ww")
            self.assertEqual(ev["conflicts"][0]["other_transaction"], t2.txid)
            self.assertEqual(ev["conflicts"][0]["row_id"], "A")

    def test_precondition_that_already_broke_is_refused_at_stage(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=1))])
            old = c.rows("accounts")[0]["version"]
            t2 = c.begin()
            t2.stage_update("accounts", "A", {"balance": 2})
            t2.commit()
            t1 = c.begin()
            with self.assertRaises(ConflictError) as info:
                t1.stage_update("accounts", "A", {"balance": 5},
                                expected_version=old)
            self.assertEqual(info.exception.code, "stale_expected_version")
            self.assertEqual(info.exception.evidence["observed_version_in_start_view"],
                             old + 1)
            t1.abort()

    def test_delete_precondition(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account())])
            tx = c.begin()
            row = tx.read("accounts")[0]
            res = tx.stage_delete("accounts", "A",
                                  expected_version=row["version"])
            self.assertEqual(res["op"], "delete")
            tx.commit()
            self.assertEqual(c.rows("accounts"), [])


class TestTypedReads(unittest.TestCase):
    def test_bool_is_not_int_in_predicates(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(active=True))])
            tx = c.begin()
            with self.assertRaises(SchemaError):
                tx.read("accounts", where=eq("active", 1))
            tx.abort()

    def test_null_column_predicate_must_use_is_null(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            with self.assertRaises(PredicateError):
                tx.read("accounts", where=eq("note", None))
            self.assertEqual(tx.read("accounts", where=is_null("note")), [])
            tx.abort()

    def test_untyped_and_or_are_rejected(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            with self.assertRaises(PredicateError):
                tx.read("accounts", where=and_())
            with self.assertRaises(PredicateError):
                tx.read("accounts", where=or_())
            with self.assertRaises(PredicateError):
                tx.read("accounts", where={"column": "owner"})
            tx.abort()

    def test_int64_bounds(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            with self.assertRaises(SchemaError):
                tx.stage_insert("accounts", "A", {"owner": "x", "balance": 2 ** 63,
                                                  "active": True, "note": None})
            tx.stage_insert("accounts", "B", {"owner": "x", "balance": -2 ** 63,
                                              "active": True, "note": None})
            tx.commit()
            self.assertEqual(c.rows("accounts")[0]["balance"], -(2 ** 63))


class TestEvidenceSurvivesGC(unittest.TestCase):
    def test_phantom_detected_after_gc(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A"))])
            # generate enough versions that GC has pruning work
            for i in range(4):
                tx = c.begin()
                tx.stage_update("accounts", "A", {"balance": i})
                tx.commit()
            reader = c.begin()
            self.assertEqual(reader.read("accounts", where=eq("owner", "ghost")), [])
            reader.stage_update("accounts", "A", {"balance": 99})
            writer = c.begin()
            writer.stage_insert("accounts", "G", account(owner="ghost"))
            writer.commit()
            c.gc_versions()
            with self.assertRaises(ConflictError) as info:
                reader.commit()
            ev = info.exception.evidence["conflicts"][0]
            self.assertEqual(ev["row_id"], "G")
            self.assertTrue(ev["witness"]["predicate"].startswith("owner"))


class TestAtomicMultiOp(unittest.TestCase):
    def test_many_ops_on_one_identity_commit_as_one_version(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            tx = c.begin()
            for i in range(1, 6):
                tx.stage_update("accounts", "A", {"balance": i})
            result = tx.commit()
            self.assertEqual(len(result["writes"]), 1)
            self.assertEqual(c.rows("accounts")[0]["balance"], 5)
            history = c.history("accounts", "A")
            self.assertEqual([h["version"] for h in history], [1, 2])

    def test_ops_apply_in_user_order(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            tx.stage_insert("accounts", "A", account(owner="one", balance=1))
            tx.stage_update("accounts", "A", {"owner": "two"})
            tx.stage_delete("accounts", "A")
            self.assertEqual(tx.read("accounts"), [])
            tx.commit()
            self.assertEqual(c.rows("accounts"), [])

    def test_reinsert_same_identity_after_delete(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="orig"))])
            tx = c.begin()
            tx.stage_delete("accounts", "A")
            tx.stage_insert("accounts", "A", account(owner="new", balance=8))
            rows = tx.read("accounts")
            self.assertEqual(rows[0]["owner"], "new")
            tx.commit()
            self.assertEqual(c.rows("accounts")[0]["owner"], "new")


if __name__ == "__main__":
    unittest.main()
