"""Randomized commit-by-commit comparison against an independent naive join.

The reference in :mod:`mv_naive` recomputes the whole join aggregate from plain
row mappings; it never imports the materializer or reads its auxiliary tables.
After every small random source commit we rebuild the expected summary and
compare it with the maintained materialized table row by row, value by value
and type by type, including the stable group identity mapping.

A separate test proves the maintenance cost is key-scoped rather than
proportional to the whole source table: many irrelevant join keys plus one hot
key, and the verifiable ``source_rows_read`` counter stays flat while the
irrelevant table grows.
"""

import random
import unittest

from support import TempRepo

from mrtx import Client, TransactionError

import mv_naive


GROUPS = [{"table": "left", "column": "g", "alias": "grp"},
          {"table": "right", "column": "cat", "alias": "cat"}]
AGGS = [
    {"fn": "count_star", "alias": "n"},
    {"fn": "count", "column": {"table": "right", "column": "amt"},
     "alias": "cnt_amt"},
    {"fn": "sum", "column": {"table": "left", "column": "qty"},
     "alias": "qty_sum"},
    {"fn": "sum", "column": {"table": "right", "column": "amt"},
     "alias": "amt_sum"},
]
KEYS = ["a", "b", "c", None]
GVALS = ["X", "Y", "", None]
CATS = ["p", "q", None]


def plain_rows(client, table):
    return {r["row_id"]: {k: v for k, v in r.items()
                          if k not in ("row_id", "version")}
            for r in client.rows(table)}


def build_fully(client, name, budget=4):
    client.start_materialized_build(name)
    for _ in range(10000):
        st = client.build_materialized_step(name, budget=budget)
        if st["state"] == "active":
            return st
    raise AssertionError("build stuck")


def read_mv(client, name):
    tx = client.begin()
    try:
        return tx.read(name)
    finally:
        tx.abort()


class TestRandomVsNaive(unittest.TestCase):
    def _trial(self, seed, join_kind):
        rng = random.Random(seed)
        with TempRepo() as repo:
            c = repo.client
            c.create_table("lefts", [("k", "text"), ("g", "text"),
                                     ("qty", "int64"), ("flag", "bool")])
            c.create_table("rights", [("k", "text"), ("cat", "text"),
                                      ("amt", "int64")])
            c.define_materialized_summary(
                "mv", "lefts", "rights", "k", "k", join_kind, GROUPS, AGGS)
            build_fully(c, "mv", budget=3)
            definition = c.get_materialized_summary("mv")["definition"]

            left_ids = ["L%d" % i for i in range(6)]
            right_ids = ["R%d" % i for i in range(6)]

            def mutate(table, ids):
                tx = c.begin()
                staged = False
                for _ in range(rng.randrange(1, 4)):
                    rid = rng.choice(ids)
                    live = {r["row_id"] for r in c.rows(table)}
                    action = rng.choice(["insert", "update", "delete"])
                    try:
                        if rid in live and action in ("update", "delete"):
                            if action == "delete":
                                tx.stage_delete(table, rid)
                            elif table == "lefts":
                                tx.stage_update(table, rid, {
                                    "k": rng.choice(KEYS),
                                    "g": rng.choice(GVALS),
                                    "qty": rng.randrange(-5, 6)})
                            else:
                                tx.stage_update(table, rid, {
                                    "k": rng.choice(KEYS),
                                    "cat": rng.choice(CATS),
                                    "amt": rng.randrange(-5, 6)})
                            staged = True
                        elif rid not in live and action == "insert":
                            if table == "lefts":
                                tx.stage_insert(table, rid, {
                                    "k": rng.choice(KEYS), "g": rng.choice(GVALS),
                                    "qty": rng.randrange(-5, 6),
                                    "flag": rng.choice([True, False])})
                            else:
                                tx.stage_insert(table, rid, {
                                    "k": rng.choice(KEYS),
                                    "cat": rng.choice(CATS),
                                    "amt": rng.randrange(-5, 6)})
                            staged = True
                    except TransactionError:
                        continue
                if staged:
                    tx.commit()
                else:
                    tx.abort()

            for step in range(30):
                mutate("lefts", left_ids)
                if rng.random() < 0.7:
                    mutate("rights", right_ids)
                self._assert_equal(c, definition, step)

    def _assert_equal(self, c, definition, step):
        left = plain_rows(c, "lefts")
        right = plain_rows(c, "rights")
        expected = mv_naive.naive_summary(definition, left, right)
        got_rows = read_mv(c, "mv")
        got = {(r["grp"], r["cat"]): r for r in got_rows}
        self.assertEqual(set(got), set(expected),
                         "step %s group set mismatch" % step)
        for key, exp in expected.items():
            row = got[key]
            for alias, value in exp.items():
                self.assertEqual(row[alias], value,
                                 "step %s %s.%s" % (step, key, alias))
                if value is not None:
                    self.assertIs(type(row[alias]), int,
                                  "step %s type of %s" % (step, alias))
                else:
                    self.assertIsNone(row[alias])
            # The stable identity must round-trip through the public mapping.
            rec = c.materialized_group_identity("mv", list(key))
            self.assertEqual(rec["group_identity"], row["row_id"])
            back = c.materialized_tuple_for_identity("mv", row["row_id"])
            self.assertEqual(back["group_tuple"], key)

    def test_inner_join_trials(self):
        for seed in range(12):
            self._trial(seed, "inner")

    def test_left_join_trials(self):
        for seed in range(12):
            self._trial(seed, "left")

    def test_deterministic_deep_reshuffle_matches_naive(self):
        # A fixed, aggressive sequence that moves rows between join keys and
        # groups so several different keys feed one group and groups repeatedly
        # disappear and reappear; the maintained table must match the naive
        # reference after every single commit.
        import random as _random
        rng = _random.Random(42)
        with TempRepo() as repo:
            c = repo.client
            c.create_table("lefts", [("k", "text"), ("g", "text"),
                                     ("qty", "int64"), ("flag", "bool")])
            c.create_table("rights", [("k", "text"), ("cat", "text"),
                                      ("amt", "int64")])
            keys = ["k%d" % i for i in range(8)]
            groups = ["G%d" % i for i in range(4)]
            cats = ["C", "D", None]
            c.define_materialized_summary(
                "mv", "lefts", "rights", "k", "k", "left",
                [{"table": "left", "column": "g", "alias": "grp"},
                 {"table": "right", "column": "cat", "alias": "cat"}],
                [{"fn": "count_star", "alias": "n"},
                 {"fn": "sum",
                  "column": {"table": "left", "column": "qty"},
                  "alias": "qs"},
                 {"fn": "sum",
                  "column": {"table": "right", "column": "amt"},
                  "alias": "as"}])
            build_fully(c, "mv", budget=4)
            definition = c.get_materialized_summary("mv")["definition"]
            tx = c.begin()
            for i in range(15):
                tx.stage_insert(
                    "lefts", "L%02d" % i,
                    {"k": rng.choice(keys), "g": rng.choice(groups),
                     "qty": rng.randrange(-9, 10),
                     "flag": rng.choice([True, False])})
            for i in range(15):
                tx.stage_insert(
                    "rights", "R%02d" % i,
                    {"k": rng.choice(keys), "cat": rng.choice(cats),
                     "amt": rng.randrange(-9, 10)})
            tx.commit()
            lids = ["L%02d" % i for i in range(15)]
            rids = ["R%02d" % i for i in range(15)]

            def check(label):
                exp = mv_naive.naive_summary(
                    definition, plain_rows(c, "lefts"),
                    plain_rows(c, "rights"))
                got = {(r["grp"], r["cat"]): r for r in read_mv(c, "mv")}
                self.assertEqual(set(got), set(exp), label)
                for key, e in exp.items():
                    for alias, value in e.items():
                        self.assertEqual(got[key][alias], value,
                                         (label, key, alias))

            for step in range(50):
                t = rng.choice(["lefts", "rights"])
                rid = rng.choice(lids if t == "lefts" else rids)
                live = {r["row_id"] for r in c.rows(t)}
                tx = c.begin()
                try:
                    if rid in live:
                        if rng.random() < 0.3:
                            tx.stage_delete(t, rid)
                        elif t == "lefts":
                            tx.stage_update(t, rid, {
                                "k": rng.choice(keys),
                                "g": rng.choice(groups),
                                "qty": rng.randrange(-9, 10)})
                        else:
                            tx.stage_update(t, rid, {
                                "k": rng.choice(keys),
                                "cat": rng.choice(cats),
                                "amt": rng.randrange(-9, 10)})
                    elif t == "lefts":
                        tx.stage_insert(t, rid, {
                            "k": rng.choice(keys), "g": rng.choice(groups),
                            "qty": rng.randrange(-9, 10), "flag": True})
                    else:
                        tx.stage_insert(t, rid, {
                            "k": rng.choice(keys),
                            "cat": rng.choice(cats),
                            "amt": rng.randrange(-9, 10)})
                    tx.commit()
                except TransactionError:
                    tx.abort()
                check("reshuffle step %s" % step)


class TestKeyScopedMaintenanceCost(unittest.TestCase):
    def test_hot_key_reads_do_not_grow_with_unrelated_data(self):
        with TempRepo() as repo:
            c = repo.client
            c.create_table("lefts", [("k", "text"), ("g", "text"),
                                     ("qty", "int64")])
            c.create_table("rights", [("k", "text"), ("cat", "text"),
                                      ("amt", "int64")])
            c.define_materialized_summary(
                "mv", "lefts", "rights", "k", "k", "inner",
                [{"table": "left", "column": "g"}],
                [{"fn": "count_star", "alias": "n"},
                 {"fn": "sum",
                  "column": {"table": "right", "column": "amt"},
                  "alias": "amt_sum"}])
            # One hot key plus a growing population of unrelated cold keys.
            tx = c.begin()
            tx.stage_insert("lefts", "hotL", {"k": "hot", "g": "H", "qty": 1})
            tx.stage_insert("rights", "hotR", {"k": "hot", "cat": "c",
                                               "amt": 1})
            for i in range(100):
                tx.stage_insert("lefts", "coldL%03d" % i,
                                {"k": "cold%03d" % i, "g": "C", "qty": 1})
                tx.stage_insert("rights", "coldR%03d" % i,
                                {"k": "cold%03d" % i, "cat": "c", "amt": 1})
            tx.commit()
            build_fully(c, "mv", budget=25)

            def last_stats():
                return c.materialized_maintenance_stats("mv", limit=1)[0]

            tx = c.begin()
            tx.stage_insert("rights", "hotR2", {"k": "hot", "cat": "c",
                                                "amt": 2})
            tx.commit()
            small = last_stats()["source_rows_read"]

            # Grow the irrelevant population substantially, then touch the hot
            # key again.  The read count for the hot update must not grow.
            tx = c.begin()
            for i in range(100, 400):
                tx.stage_insert("lefts", "coldL%03d" % i,
                                {"k": "cold%03d" % i, "g": "C", "qty": 1})
                tx.stage_insert("rights", "coldR%03d" % i,
                                {"k": "cold%03d" % i, "cat": "c", "amt": 1})
            tx.commit()
            tx = c.begin()
            tx.stage_insert("rights", "hotR3", {"k": "hot", "cat": "c",
                                                "amt": 3})
            tx.commit()
            large = last_stats()["source_rows_read"]
            self.assertLessEqual(large, small + 2,
                                 "hot-key maintenance scanned unrelated rows: "
                                 "%s vs %s" % (large, small))
            # And the aggregate itself is still correct (1+2+3 = 6 over 3 rows).
            rows = {r["g"]: r for r in read_mv(c, "mv") if r["g"] == "H"}
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows["H"]["n"], 3)
            self.assertEqual(rows["H"]["amt_sum"], 6)


if __name__ == "__main__":
    unittest.main()
