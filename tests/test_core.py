"""Core semantics: types, 3-valued predicates, snapshot, staging, conflicts."""

import unittest

from support import TempRepo, account, ids, seed_accounts

from mrtx import (ConflictError, PredicateError, SchemaError, TransactionError,
                  and_, eq, is_null, le, lt, not_, or_)


class TestTypes(unittest.TestCase):
    def test_bool_and_int_are_distinct(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            with self.assertRaises(SchemaError):
                tx.stage_insert("accounts", "a", {"owner": "x", "balance": True,
                                                  "active": True, "note": None})
            with self.assertRaises(SchemaError):
                tx.stage_insert("accounts", "a", {"owner": "x", "balance": 1,
                                                  "active": 1, "note": None})
            tx.abort()

    def test_text_compares_by_code_point(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("a", account(owner="Z")), ("b", account(owner="a")),
                              ("c", account(owner="é")),
                              ("d", account(owner="\U0001f600"))])
            tx = c.begin()
            self.assertEqual(ids(tx.read("accounts", where=lt("owner", "a"))), ["a"])
            self.assertEqual(ids(tx.read("accounts", where=le("owner", "é"))),
                             ["a", "b", "c"])
            tx.abort()

    def test_three_valued_logic_only_true_selects(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("a", account(owner="x")), ("b", account(owner="y"))])
            tx = c.begin()
            # note IS NULL is TRUE for both; NOT of it is FALSE, not UNKNOWN.
            self.assertEqual(tx.read("accounts", where=not_(is_null("note"))), [])
            self.assertEqual(ids(tx.read("accounts", where=is_null("note"))), ["a", "b"])
            # OR with a TRUE branch is TRUE even when the other branch is UNKNOWN.
            self.assertEqual(
                ids(tx.read("accounts", where=or_(eq("owner", "x"), eq("balance", 999)))),
                ["a"])
            # AND with a FALSE branch is FALSE regardless of UNKNOWN.
            self.assertEqual(
                tx.read("accounts", where=and_(eq("owner", "nobody"), eq("balance", 10))),
                [])
            tx.abort()

    def test_predicate_validation(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            with self.assertRaises(PredicateError):
                tx.read("accounts", where=eq("nope", 1))
            with self.assertRaises(SchemaError):
                tx.read("accounts", where=eq("balance", "10"))
            with self.assertRaises(SchemaError):
                tx.read("accounts", columns=["row_id", "nope"])
            tx.abort()

    def test_null_column_only_accepts_null(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            with self.assertRaises(SchemaError):
                tx.stage_insert("accounts", "a", {"owner": "x", "balance": 1,
                                                  "active": True, "note": 3})
            tx.abort()


class TestSnapshot(unittest.TestCase):
    def test_order_and_versions(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("z", account(owner="z")), ("a", account(owner="a")),
                              ("m", account(owner="m"))])
            tx = c.begin()
            rows = tx.read("accounts")
            self.assertEqual(ids(rows), ["a", "m", "z"])
            self.assertEqual({r["version"] for r in rows}, {1})
            tx.abort()

    def test_fixed_view_plus_own_writes(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("a", account(owner="orig"))])
            t1 = c.begin()
            self.assertEqual(t1.read("accounts")[0]["owner"], "orig")
            t1.stage_update("accounts", "a", {"owner": "mine"})
            self.assertEqual(t1.read("accounts")[0]["owner"], "mine")
            self.assertLess(t1.read("accounts")[0]["version"], 0)
            t2 = c.begin()
            t2.stage_update("accounts", "a", {"owner": "theirs"})
            t2.commit()
            # t1's snapshot is unchanged by t2's commit
            self.assertEqual(t1.read("accounts")[0]["owner"], "mine")
            with self.assertRaises(ConflictError) as info:
                t1.commit()
            self.assertEqual(info.exception.code, "write_conflict")
            ev = info.exception.evidence
            self.assertEqual(ev["conflicts"][0]["row_id"], "a")
            self.assertEqual(ev["conflicts"][0]["other_transaction"], t2.txid)

    def test_insert_then_delete_cancels(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            tx.stage_insert("accounts", "a", account())
            tx.stage_delete("accounts", "a")
            self.assertEqual(tx.read("accounts"), [])
            tx.commit()
            self.assertEqual(c.rows("accounts"), [])

    def test_abort_leaves_no_visible_write(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("a", account(owner="keep"))])
            tx = c.begin()
            tx.stage_update("accounts", "a", {"owner": "gone"})
            tx.stage_insert("accounts", "b", account(owner="new"))
            tx.abort()
            rows = c.rows("accounts")
            self.assertEqual(ids(rows), ["a"])
            self.assertEqual(rows[0]["owner"], "keep")
            status = c.transaction_status(tx.txid)
            self.assertEqual(status["decision"], "aborted")
            self.assertFalse(status["committed"])

    def test_closed_transaction_rejects_operations(self):
        with TempRepo() as repo:
            tx = repo.client.begin()
            tx.abort()
            with self.assertRaises(TransactionError):
                tx.read("accounts")
            with self.assertRaises(TransactionError):
                tx.commit()

    def test_expected_version_mismatch_at_stage(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("a", account())])
            tx = c.begin()
            with self.assertRaises(ConflictError) as info:
                tx.stage_update("accounts", "a", {"owner": "x"}, expected_version=99)
            self.assertEqual(info.exception.code, "stale_expected_version")
            self.assertEqual(info.exception.evidence["expected_version"], 99)
            tx.abort()

    def test_identity_cannot_be_rebound(self):
        with TempRepo() as repo:
            c = repo.client
            tx = c.begin()
            tx.stage_insert("accounts", "a", account(owner="first"))
            with self.assertRaises(TransactionError):
                tx.stage_insert("accounts", "a", account(owner="second"))
            tx.commit()
            self.assertEqual(c.rows("accounts")[0]["owner"], "first")


if __name__ == "__main__":
    unittest.main()
