"""Concurrency invariants: decision/visibility agreement under contention."""

import concurrent.futures
import unittest

import serialcheck
from support import TempRepo, account, seed_accounts

from mrtx import BusyError, Client, ConflictError, eq


class TestDecisionMatchesVisibility(unittest.TestCase):
    """Many clients contend on one identity; committed decisions must agree
    exactly with the versions actually visible afterwards."""

    def test_contention_on_one_identity(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])

            def work(i):
                client = Client(repo.path)
                tx = client.begin()
                try:
                    tx.read("accounts", where=eq("row_id", "A"),
                            columns=["row_id", "balance"])
                    tx.stage_update("accounts", "A", {"balance": i})
                    out = tx.commit()
                    return ("committed", out["commit_version"], tx.txid)
                except ConflictError:
                    return ("rejected", None, tx.txid)
                except BusyError:
                    return ("busy", None, tx.txid)

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(work, range(10)))

            committed = [r for r in results if r[0] == "committed"]
            busy = [r for r in results if r[0] == "busy"]
            self.assertEqual(busy, [], "busy must not appear in normal contention")
            versions = [r[1] for r in committed]
            self.assertEqual(len(versions), len(set(versions)),
                             "commit versions are unique")

            final = c.rows("accounts")[0]
            self.assertEqual(final["version"], max(versions))
            # Every committed transaction's decision is durable and visible.
            for status, version, txid in committed:
                st = c.transaction_status(txid)
                self.assertEqual(st["decision"], "committed")
                self.assertEqual(st["commit_version"], version)
                self.assertLessEqual(version, final["version"])
            # Every rejected transaction is durably rejected with evidence.
            for status, _, txid in results:
                if status == "rejected":
                    st = c.transaction_status(txid)
                    self.assertEqual(st["decision"], "rejected")
                    self.assertFalse(st["committed"])
            history = serialcheck.load_history(repo.path)
            order = serialcheck.claimed_serial_order(history)
            ok, detail = serialcheck.serial_valid(history, order)
            self.assertTrue(ok, detail)

    def test_committed_reads_see_only_earlier_commits(self):
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(balance=0))])
            tx = c.begin()
            tx.stage_update("accounts", "A", {"balance": 1})
            first = tx.commit()
            later = c.begin()
            later.stage_update("accounts", "A", {"balance": 2})
            second = later.commit()
            self.assertGreater(second["commit_version"], first["commit_version"])
            hist = c.history("accounts", "A")
            self.assertEqual(len(hist), 3)  # seed + two updates
            self.assertEqual(hist[-1]["balance"], 2)


if __name__ == "__main__":
    unittest.main()
