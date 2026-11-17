"""Real-process synchronization drill.

A consumer process pulls exactly one full page, then blocks (the parent holds
the next command).  While it is paused, another process updates rows it has
not yet read, deletes one row and re-inserts it, and commits a change that
spans two subscribed tables.  The source then runs cleanup and GC.  The
consumer is killed (client offline), then a fresh consumer process resumes.
The final replica must equal the source state at the requested commit
boundary, row by row, type by type, version by version.
"""

import os
import unittest

from procutil import Worker
from support import TempRepo, account

from mrtx import Client, Replica, SubscriptionHandle


def ledger(acct="A", amount=1, active=True):
    return {"acct": acct, "amount": amount, "active": active}


ACCOUNT_COLS = ("owner", "balance", "active", "note")
LEDGER_COLS = ("acct", "amount", "active")


class TestRealProcessFullThenIncremental(unittest.TestCase):
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

    def snapshot_at(self, table, boundary, row_ids):
        """Independent source image at *boundary* from public history()."""
        result = {}
        for rid in row_ids:
            hist = [h for h in self.c.history(table, rid)
                    if h["version"] <= boundary]
            if not hist:
                continue
            newest = max(hist, key=lambda h: h["version"])
            if newest.get("deleted"):
                continue
            row = {"row_id": rid, "version": newest["version"]}
            for col in (ACCOUNT_COLS if table == "accounts" else LEDGER_COLS):
                row[col] = newest[col]
            result[rid] = row
        return result

    def test_paused_full_load_then_offline_consumer_converges_at_boundary(self):
        c = self.c
        # Seed a source state in one commit (version 1).
        tx = c.begin()
        for i in range(12):
            tx.stage_insert("accounts", "r%02d" % i,
                            account(owner="o%d" % i, balance=i, active=True,
                                    note=None))
        tx.stage_insert("ledger", "L0", ledger(acct="seed", amount=100))
        tx.commit()
        cut = c.current_version()

        sub, _ = c.create_subscription("drill", ["accounts", "ledger"])
        handle = sub.handle.as_dict()

        # --- Consumer process pulls exactly one full page, then pauses. ---
        consumer = self.worker()
        first = consumer.pull(handle, max_rows=5)
        self.assertTrue(first["ok"], first)
        batch = first["result"]["batch"]
        self.assertEqual(batch["phase"], "full")
        self.assertFalse(batch["full_complete"])
        self.assertEqual(batch["row_count"], 5)
        self.assertEqual([i["row_id"] for i in batch["items"]],
                         ["r00", "r01", "r02", "r03", "r04"])
        consumer.kill()  # the client goes offline; batch still pending

        # --- Another process mutates rows not yet delivered. ---
        writer = self.worker()
        t = writer.begin()
        writer.update(t, "accounts", "r08", {"balance": 808})
        writer.delete(t, "accounts", "r09")
        writer.insert(t, "accounts", "r09",
                      account(owner="reborn", balance=909, active=False,
                              note=None))
        writer.insert(t, "ledger", "L1", ledger(acct="cross", amount=7,
                                                active=True))
        out = writer.commit(t)
        self.assertTrue(out["ok"], out)
        boundary = out["result"]["commit_version"]
        # Another commit afterwards, which the consumer will NOT include
        # because it is asked to stop at `boundary`.
        t2 = writer.begin()
        writer.update(t2, "accounts", "r02", {"balance": 202})
        writer.commit(t2)

        # --- Source runs cleanup and GC while the consumer is offline. ---
        writer.cleanup()
        gc = writer.gc()
        self.assertIn(cut, gc["full_phase_cuts"])
        self.assertIn(1, [h["version"] for h in c.history("accounts", "r00")])

        # --- A fresh consumer resumes the same pending batch. ---
        replica_path = os.path.join(self.repo.dir, "replica.db")
        fresh = self.worker()
        again = fresh.pull(handle, max_rows=1000)
        self.assertEqual(again["result"]["batch"]["batch_id"], batch["batch_id"])
        self.assertEqual(again["result"]["batch"]["items"], batch["items"])

        fresh_client = Client(self.repo.path)
        rep = Replica(replica_path, SubscriptionHandle.from_dict(handle))
        # Consume page by page until the replica's applied position reaches
        # the requested boundary (the following commit is not applied).
        for _ in range(100000):
            state = rep.state()
            if state["position"] >= boundary:
                break
            report = rep.apply_local(fresh_client, max_rows=4)
            if report is None:
                break
            rep.acknowledge(fresh_client)
        else:
            self.fail("replica did not reach the boundary")
        self.assertGreaterEqual(rep.state()["position"], boundary)

        # --- Independent comparison at the fixed boundary. ---
        account_ids = ["r%02d" % i for i in range(12)]
        want_accounts = self.snapshot_at("accounts", boundary, account_ids)
        want_ledger = self.snapshot_at("ledger", boundary, ["L0", "L1"])
        got_accounts = {r["row_id"]: r for r in rep.rows("accounts")}
        got_ledger = {r["row_id"]: r for r in rep.rows("ledger")}

        self.assertEqual(set(got_accounts), set(want_accounts))
        for rid, want in want_accounts.items():
            got = got_accounts[rid]
            self.assertEqual(got["version"], want["version"], rid)
            for col in ACCOUNT_COLS:
                self.assertEqual(got[col], want[col], "%s.%s" % (rid, col))
                self.assertIs(type(got[col]), type(want[col]),
                              "%s.%s type" % (rid, col))
        self.assertEqual(set(got_ledger), set(want_ledger))
        for rid, want in want_ledger.items():
            got = got_ledger[rid]
            self.assertEqual(got["version"], want["version"], rid)
            for col in LEDGER_COLS:
                self.assertEqual(got[col], want[col])
                self.assertIs(type(got[col]), type(want[col]))

        # Hand-checked facts: the reinserted identity is present with its new
        # typed values, the cross-table commit is atomic, and the *later*
        # commit past the boundary was not mixed in.
        self.assertEqual(got_accounts["r08"]["balance"], 808)
        self.assertEqual(got_accounts["r09"]["owner"], "reborn")
        self.assertIs(got_accounts["r09"]["active"], False)
        self.assertEqual(got_ledger["L1"]["amount"], 7)
        self.assertEqual(got_accounts["r02"]["balance"], 2)
        # Every account is at the S-or-later image <= boundary: r00..r07 are
        # still version 1, r08/r09 at the boundary commit.
        self.assertEqual(got_accounts["r00"]["version"], 1)
        self.assertEqual(got_accounts["r09"]["version"], boundary)


if __name__ == "__main__":
    unittest.main()
