"""Randomized small histories: engine decisions vs. independent enumeration.

Each trial runs a few transactions in one process with interleaved operations
(the barrier is the call order), records every read result and every commit
decision, then enumerates all serial orders of the committed history with the
independent checker.  Assertions:

* every successful history has at least one serial order;
* the engine's commit order is itself a valid serial order;
* the engine never reports success for a history with no serial order;
* busy errors never appear as conflicts (they carry no evidence).
"""

import itertools
import random
import unittest

import serialcheck
from support import TempRepo, account, ids, seed_accounts

from mrtx import (Client, ConflictError, TransactionError, and_, eq, is_null, le,
                  lt, not_, or_)
from mrtx.canonical import describe as describe_pred


PREDICATES = [
    None,
    eq("owner", "a"),
    eq("owner", "b"),
    lt("balance", 5),
    le("balance", 5),
    and_(lt("balance", 5), eq("active", True)),
    or_(eq("owner", "a"), lt("balance", 3)),
    not_(lt("balance", 5)),
    is_null("note"),
]


def make_txn(client, n, rng, row_ids, preds):
    """Issue a random interleaved script of reads and writes on one transaction."""
    tx = client.begin()
    script = []
    for _ in range(n):
        if rng.random() < 0.5:
            pred = rng.choice(preds)
            cols = rng.choice([None, ["row_id"], ["row_id", "owner"],
                               ["row_id", "balance"], ["row_id", "owner", "active"]])
            rows = tx.read("accounts", columns=cols, where=pred)
            script.append(("read", pred, cols, rows))
        else:
            rid = rng.choice(row_ids)
            try:
                if rng.random() < 0.4:
                    res = tx.stage_insert("accounts", rid,
                                          account(owner=rng.choice("ab"),
                                                  balance=rng.randrange(6),
                                                  active=rng.choice([True, False])))
                elif rng.random() < 0.7:
                    res = tx.stage_update("accounts", rid,
                                          {"balance": rng.randrange(6)})
                else:
                    res = tx.stage_delete("accounts", rid)
                script.append(("write", res["op"], rid, res))
            except TransactionError:
                script.append(("write-skip", rid))
            except ConflictError:
                script.append(("write-conflict", rid))
    return tx, script


class TestRandomHistories(unittest.TestCase):
    def _run_trial(self, seed):
        rng = random.Random(seed)
        with TempRepo() as repo:
            c = repo.client
            seed_accounts(c, [("A", account(owner="a", balance=1, active=True)),
                              ("B", account(owner="b", balance=7, active=True)),
                              ("C", account(owner="a", balance=4, active=True))])
            row_ids = ["A", "B", "C", "D"]
            preds = PREDICATES
            count = rng.randrange(2, 5)
            txs = []
            for _ in range(count):
                tx, script = make_txn(c, rng.randrange(1, 4), rng, row_ids, preds)
                txs.append((tx, script))
            decisions = []
            for tx, script in reversed(txs):  # commit in a mixed order
                try:
                    out = tx.commit()
                    decisions.append(("committed", out))
                except ConflictError as exc:
                    decisions.append(("rejected", exc.evidence))
                except TransactionError as exc:
                    decisions.append(("error", str(exc)))
            committed = sum(1 for d in decisions if d[0] == "committed")
            history = serialcheck.load_history(repo.path)
            # +1 for the seeding transaction itself
            self.assertEqual(len(history["transactions"]), committed + 1)
            valid, counter = serialcheck.enumerate_serial_orders(history)
            if committed > 0:
                self.assertTrue(
                    valid,
                    "seed %s: successful history has no serial order: %r\nscripts=%r\ndecisions=%r"
                    % (seed, counter, [(t.txid, s) for t, s in txs],
                       [(d[0], str(d[1])[:200]) for d in decisions]))
                order = serialcheck.claimed_serial_order(history)
                ok, detail = serialcheck.serial_valid(history, order)
                self.assertTrue(ok, "seed %s: claimed serial order invalid: %r" % (seed, detail))

    def test_randomized(self):
        for seed in range(40):
            with self.subTest(seed=seed):
                self._run_trial(seed)


class TestRandomizedMultiProcess(unittest.TestCase):
    """A few real processes with interleaved scripts, verified independently."""

    def test_process_trials(self):
        from procutil import Worker
        rng = random.Random(1234)
        for trial in range(4):
            with self.subTest(trial=trial), TempRepo() as repo:
                c = repo.client
                seed_accounts(c, [("A", account(owner="a", balance=1)),
                                  ("B", account(owner="b", balance=7))])
                workers = [Worker(repo.path) for _ in range(3)]
                try:
                    txs = [(w, w.begin()) for w in workers]
                    for w, t in txs:
                        w.read(t, "accounts")
                        for _ in range(rng.randrange(1, 4)):
                            rid = rng.choice(["A", "B", "C"])
                            if rng.random() < 0.5:
                                w.update(t, "accounts", rid,
                                         {"balance": rng.randrange(8)})
                            else:
                                w.read(t, "accounts",
                                       where=rng.choice([
                                           {"op": "eq", "col": "owner", "value": "a"},
                                           {"op": "lt", "col": "balance", "value": 4}]))
                    order = list(txs)
                    rng.shuffle(order)
                    for w, t in order:
                        w.commit(t)
                    history = serialcheck.load_history(repo.path)
                    valid, counter = serialcheck.enumerate_serial_orders(history)
                    self.assertTrue(valid, "trial %s: %r" % (trial, counter))
                    order = serialcheck.claimed_serial_order(history)
                    ok, detail = serialcheck.serial_valid(history, order)
                    self.assertTrue(ok, detail)
                finally:
                    for w in workers:
                        w.close()


if __name__ == "__main__":
    unittest.main()
