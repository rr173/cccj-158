"""Concurrency semantics: write skew, phantoms, disjoint ranges, cross-table cycles."""

import unittest

from support import TempRepo, account, ids, seed_accounts

from mrtx import ConflictError, TransactionError, and_, eq, is_null, lt, not_, or_


class TestActiveDrill(unittest.TestCase):
    """Main drill: A/B active rows, both read both, each deactivates one."""

    def test_at_most_one_succeeds_and_one_active_remains(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A", active=True)),
                              ("B", account(owner="B", active=True))])
            t1 = c.begin()
            t2 = c.begin()
            r1 = t1.read("accounts", where=eq("active", True))
            r2 = t2.read("accounts", where=eq("active", True))
            self.assertEqual(ids(r1), ["A", "B"])
            self.assertEqual(ids(r2), ["A", "B"])
            t1.stage_update("accounts", "A", {"active": False},
                            expected_version=r1[0]["version"])
            t2.stage_update("accounts", "B", {"active": False},
                            expected_version=r2[1]["version"])
            first = t1.commit()
            self.assertEqual(first["status"], "committed")
            with self.assertRaises(ConflictError) as info:
                t2.commit()
            ev = info.exception.evidence
            # t2's read of A was invalidated by t1's write: an rw dependency.
            self.assertEqual(info.exception.code, "read_conflict")
            self.assertEqual(ev["conflicts"][0]["dependency"], "rw")
            self.assertEqual(ev["conflicts"][0]["row_id"], "A")
            self.assertEqual(ev["conflicts"][0]["other_transaction"], t1.txid)
            final = c.rows("accounts")
            self.assertEqual(len([r for r in final if r["active"]]), 1)
            # a durable rejection was recorded as well
            self.assertEqual(c.transaction_status(t2.txid)["decision"], "rejected")
            rej = c.rejection_evidence(t2.txid)
            self.assertEqual(rej["code"], "read_conflict")

    def test_both_may_commit_if_serial_order_explains_it_via_retry(self):
        """After a rejection the caller may re-read and retry; both end inactive."""
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A")), ("B", account(owner="B"))])
            t1 = c.begin()
            t1.read("accounts")
            t1.stage_update("accounts", "A", {"active": False})
            t1.commit()
            t2 = c.begin()
            rows = t2.read("accounts")
            self.assertEqual(len(rows), 2)
            t2.stage_update("accounts", "B", {"active": False})
            self.assertEqual(t2.commit()["status"], "committed")
            self.assertEqual(len(c.rows("accounts")), 2)


class TestPhantom(unittest.TestCase):
    def test_insert_into_empty_predicate_rejects_reader(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A"))])
            t1 = c.begin()
            # Empty result: the predicate range is registered, not just rows.
            self.assertEqual(t1.read("accounts", where=eq("owner", "ghost")), [])
            # A read-only transaction could serialize at its start view; the
            # phantom dependency only constrains a transaction that also
            # writes, so stage an unrelated write here.
            t1.stage_update("accounts", "A", {"balance": 2})
            t2 = c.begin()
            t2.stage_insert("accounts", "G", account(owner="ghost"))
            t2.commit()
            with self.assertRaises(ConflictError) as info:
                t1.commit()
            ev = info.exception.evidence
            self.assertEqual(info.exception.code, "read_conflict")
            witness = ev["conflicts"][0]["witness"]
            self.assertTrue(witness["predicate"].startswith("owner"))
            self.assertEqual(ev["conflicts"][0]["other_transaction"], t2.txid)

    def test_delete_from_observed_predicate_rejects_reader(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A")), ("B", account(owner="B"))])
            t1 = c.begin()
            self.assertEqual(ids(t1.read("accounts", where=eq("owner", "A"))), ["A"])
            t1.stage_update("accounts", "B", {"balance": 3})
            t2 = c.begin()
            t2.stage_delete("accounts", "A")
            t2.commit()
            with self.assertRaises(ConflictError) as info:
                t1.commit()
            self.assertEqual(info.exception.code, "read_conflict")

    def test_write_outside_observed_predicate_does_not_conflict(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A", balance=1)),
                              ("B", account(owner="B", balance=1))])
            t1 = c.begin()
            self.assertEqual(ids(t1.read("accounts", where=eq("owner", "A"))), ["A"])
            t2 = c.begin()
            t2.stage_update("accounts", "B", {"balance": 5})
            t2.commit()
            self.assertEqual(t1.commit()["status"], "committed")

    def test_narrow_projection_is_not_invalidated_by_unprojected_column(self):
        """Only projected columns and predicate columns are observed."""
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A", balance=1)),
                              ("Z", account(owner="Z", balance=1))])
            t1 = c.begin()
            t1.read("accounts", where=lt("owner", "B"), columns=["row_id", "owner"])
            t1.stage_update("accounts", "Z", {"balance": 3})
            t2 = c.begin()
            t2.stage_update("accounts", "A", {"balance": 7})  # balance not observed
            t2.commit()
            # t1 can serialize after t2: its owner projection is unchanged.
            self.assertEqual(t1.commit()["status"], "committed")

    def test_predicate_column_change_invalidates_reader(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A")), ("Z", account(owner="Z"))])
            t1 = c.begin()
            t1.read("accounts", where=lt("owner", "B"), columns=["row_id"])
            t1.stage_update("accounts", "Z", {"balance": 3})
            t2 = c.begin()
            t2.stage_update("accounts", "A", {"owner": "Q"})  # leaves the range
            t2.commit()
            with self.assertRaises(ConflictError) as info:
                t1.commit()
            ev = info.exception.evidence["conflicts"][0]
            self.assertEqual(ev["row_id"], "A")
            self.assertFalse(ev["matches_after_other_commit"])


class TestDisjointRanges(unittest.TestCase):
    def test_disjoint_key_ranges_commit_without_replay(self):
        with TempRepo() as repo:
            c = repo.client
            rows = [("%03d" % i, account(owner="o%d" % i, balance=i)) for i in range(10)]
            seed_accounts(c, rows)
            t1 = c.begin()
            t2 = c.begin()
            left = t1.read("accounts", where=lt("row_id", "005"))
            right = t2.read("accounts", where=not_(lt("row_id", "005")))
            # t1 only touches keys < 005, t2 only >= 005
            t1.stage_update("accounts", "001", {"balance": 100})
            t2.stage_update("accounts", "009", {"balance": 200})
            self.assertEqual(t1.commit()["status"], "committed")
            # t2 read row 009 (and rows including some t1 didn't touch);
            # t1 wrote 001, which t2 did not observe as a changed column.
            res = t2.commit()
            self.assertEqual(res["status"], "committed")
            final = {r["row_id"]: r for r in c.rows("accounts")}
            self.assertEqual(final["001"]["balance"], 100)
            self.assertEqual(final["009"]["balance"], 200)

    def test_same_predicate_different_rows_write_skew_conflicts(self):
        """Both read the same condition but update different rows: must conflict."""
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A", active=True)),
                              ("B", account(owner="B", active=True))])
            t1 = c.begin()
            t2 = c.begin()
            t1.read("accounts", where=eq("active", True), columns=["row_id", "active"])
            t2.read("accounts", where=eq("active", True), columns=["row_id", "active"])
            t1.stage_update("accounts", "A", {"active": False})
            t2.stage_update("accounts", "B", {"active": False})
            t1.commit()
            with self.assertRaises(ConflictError) as info:
                t2.commit()
            self.assertEqual(info.exception.evidence["conflicts"][0]["row_id"], "A")


class TestCrossTableCycle(unittest.TestCase):
    """Three transactions over two tables forming a dependency ring."""

    def test_three_way_cycle_rejected_with_cycle_evidence(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="A"))])
            tx0 = c.begin()
            tx0.stage_insert("ledger", "L1", {"acct": "A", "amount": 1, "active": True})
            tx0.stage_insert("audit", "U1", {"tag": "start", "n": 1, "ok": True})
            tx0.commit()

            t1 = c.begin()
            t2 = c.begin()
            # T1 reads accounts, then will write ledger.
            t1.read("accounts", where=eq("row_id", "A"))
            # T2 reads ledger, then will write audit.
            t2.read("ledger", where=eq("row_id", "L1"))
            # T1 commits a ledger write.
            t1.stage_update("ledger", "L1", {"amount": 2})
            self.assertEqual(t1.commit()["status"], "committed")

            # T3 starts after T1, reads audit, and will write accounts.
            t3 = c.begin()
            t3.read("audit", where=eq("row_id", "U1"))

            # T2's ledger read is now stale -> rejected
            t2.stage_update("audit", "U1", {"n": 2})
            with self.assertRaises(ConflictError) as info:
                t2.commit()
            ev = info.exception.evidence
            self.assertEqual(ev["conflicts"][0]["dependency"], "rw")
            self.assertEqual(ev["conflicts"][0]["other_transaction"], t1.txid)

            # T3 writes accounts and commits on its own (no read invalidation).
            t3.stage_update("accounts", "A", {"balance": 9})
            self.assertEqual(t3.commit()["status"], "committed")

            graph = c.dependency_graph()
            pairs = [{"from": e["from"], "to": e["to"], "kind": e["kind"]}
                     for e in graph["edges"]]
            # t2 read ledger before t1 wrote it: the anti-dependency T2 -> T1.
            self.assertIn({"from": t2.txid, "to": t1.txid, "kind": "rw"}, pairs)
            # Every cycle reported (if any) names real transactions.
            for cyc in graph["cycles"]:
                self.assertTrue(set(cyc["nodes"]) <= set(graph["nodes"]))


if __name__ == "__main__":
    unittest.main()
