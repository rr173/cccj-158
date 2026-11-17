"""Unit tests for materialized join-aggregate summaries.

Covers the restricted declaration validation, hand-computed join/aggregate
semantics (duplicate keys, unmatched->matched->unmatched, nulls, two-sided
changes and group migration), stable group identity, row versioning, int64
overflow rejection, read-only enforcement and the key-scoped maintenance
statistics.  Real-process, replica and randomized cross-checks live in their
own modules.
"""

import unittest

from support import TempRepo

from mrtx import (Client, ConflictError, MaterializedError,
                  MaterializedNotReady, SchemaError, eq, is_null)
from mrtx.materialize import INT64_MAX

import mv_naive


def lrow(k, g, qty=None, flag=None):
    return {"k": k, "g": g, "qty": qty, "flag": flag}


def rrow(k, cat, amt=None):
    return {"k": k, "cat": cat, "amt": amt}


DEF_GROUPS = [{"table": "left", "column": "g", "alias": "grp"}]
DEF_AGGS = [
    {"fn": "count_star", "alias": "n"},
    {"fn": "count", "column": {"table": "right", "column": "amt"},
     "alias": "cnt_amt"},
    {"fn": "sum", "column": {"table": "left", "column": "qty"},
     "alias": "qty_sum"},
    {"fn": "sum", "column": {"table": "right", "column": "amt"},
     "alias": "amt_sum"},
]


def build_fully(client, name, budget=3):
    client.start_materialized_build(name)
    steps = 0
    while True:
        st = client.build_materialized_step(name, budget=budget)
        steps += 1
        if st["state"] == "active":
            return st, steps
        if steps > 10000:
            raise AssertionError("build did not finish")


def read_mv(client, name):
    tx = client.begin()
    try:
        return tx.read(name)
    finally:
        tx.abort()


class MvCase(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo().__enter__()
        self.c = self.repo.client
        self.c.create_table(
            "lefts", [("k", "text"), ("g", "text"), ("qty", "int64"),
                      ("flag", "bool")])
        self.c.create_table(
            "rights", [("k", "text"), ("cat", "text"), ("amt", "int64")])

    def tearDown(self):
        self.repo.__exit__(None, None, None)

    def insert(self, table, rows):
        tx = self.c.begin()
        for rid, vals in rows:
            tx.stage_insert(table, rid, vals)
        tx.commit()

    def define(self, name="mv", join_kind="left", groups=None, aggs=None):
        return self.c.define_materialized_summary(
            name, "lefts", "rights", "k", "k", join_kind,
            groups or DEF_GROUPS, aggs or DEF_AGGS)

    # -- declaration validation ---------------------------------------------

    def test_invalid_definitions_are_rejected_before_any_build(self):
        bad = [
            dict(left_table="nope"),
            dict(right_table="nope"),
            dict(left_join_column="flag"),    # bool, not a text join column
            dict(right_join_column="amt"),    # int64, not text
            dict(join_kind="outer"),
            dict(groups=[]),
            dict(groups=[{"table": "left", "column": "nope"}]),
            dict(groups=[{"table": "middle", "column": "k"}]),
            dict(groups=[{"table": "left", "column": "row_id"}]),
            dict(aggs=[{"fn": "avg", "alias": "x"}]),
            dict(aggs=[{"fn": "sum",
                        "column": {"table": "left", "column": "k"},
                        "alias": "x"}]),   # sum of text
            dict(aggs=[{"fn": "count",
                        "column": {"table": "left", "column": "missing"},
                        "alias": "x"}]),
            dict(aggs=[{"fn": "count_star"}]),                       # no alias
            dict(aggs=[{"fn": "count_star", "alias": "grp"}]),      # dup alias
        ]
        for overrides in bad:
            kwargs = dict(name="mvb", left_table="lefts",
                          right_table="rights", left_join_column="k",
                          right_join_column="k", join_kind="left",
                          groups=DEF_GROUPS, aggregates=DEF_AGGS)
            if "left_table" in overrides:
                kwargs["left_table"] = overrides["left_table"]
            if "right_table" in overrides:
                kwargs["right_table"] = overrides["right_table"]
            if "left_join_column" in overrides:
                kwargs["left_join_column"] = overrides["left_join_column"]
            if "right_join_column" in overrides:
                kwargs["right_join_column"] = overrides["right_join_column"]
            if "join_kind" in overrides:
                kwargs["join_kind"] = overrides["join_kind"]
            if "groups" in overrides:
                kwargs["groups"] = overrides["groups"]
            if "aggs" in overrides:
                kwargs["aggregates"] = overrides["aggs"]
            with self.assertRaises((MaterializedError, SchemaError),
                                   msg=overrides):
                self.c.define_materialized_summary(**kwargs)
        # Nothing was created.
        self.assertEqual(self.c.list_materialized_summaries(), [])

    def test_same_table_both_sides_and_duplicate_output_names_rejected(self):
        with self.assertRaises(MaterializedError):
            self.c.define_materialized_summary(
                "m1", "lefts", "lefts", "k", "k", "inner", DEF_GROUPS,
                [{"fn": "count_star", "alias": "n"}])
        with self.assertRaises(MaterializedError):
            self.c.define_materialized_summary(
                "m2", "lefts", "rights", "k", "k", "inner",
                [{"table": "left", "column": "g", "alias": "n"}],
                [{"fn": "count_star", "alias": "n"}])
        with self.assertRaises(MaterializedError):
            self.c.define_materialized_summary(
                "m3", "lefts", "rights", "k", "k", "inner",
                [{"table": "left", "column": "g"},
                 {"table": "left", "column": "g", "alias": "g2"}],
                [{"fn": "count_star", "alias": "n"}])

    def test_existing_name_cannot_silently_change_definition(self):
        self.define("mv")
        with self.assertRaises(MaterializedError):
            self.define("mv", join_kind="inner")
        # And an ordinary table may not take over the name.
        with self.assertRaises(SchemaError):
            self.c.create_table("mv", [("x", "text")])

    def test_inactive_summary_is_not_readable_or_subscribable(self):
        self.define("mv")
        with self.assertRaises(MaterializedNotReady):
            read_mv(self.c, "mv")
        with self.assertRaises(Exception):
            self.c.create_subscription("s", ["mv"])

    # -- hand-computed join semantics ----------------------------------------

    def test_hand_computed_left_join_with_duplicates_and_nulls(self):
        self.insert("lefts", [
            ("L1", lrow("a", "X", qty=1, flag=True)),
            ("L2", lrow("a", "X", qty=2, flag=False)),
            ("L3", lrow("b", "Z", qty=4, flag=True)),
            ("L4", lrow(None, "N", qty=8, flag=False)),
        ])
        self.insert("rights", [
            ("R1", rrow("a", "p", amt=5)),
            ("R2", rrow("a", "p", amt=7)),
            ("R3", rrow(None, "q", amt=99)),
        ])
        self.define("mv")
        build_fully(self.c, "mv", budget=2)
        rows = {r["grp"]: r for r in read_mv(self.c, "mv")}
        # a x a = 2x2 = 4 join rows, both in group X.
        self.assertEqual(rows["X"]["n"], 4)
        self.assertEqual(rows["X"]["cnt_amt"], 4)
        self.assertEqual(rows["X"]["amt_sum"], 24)
        # qty summed over the four fan-out rows: (1+2)*2 = 6.
        self.assertEqual(rows["X"]["qty_sum"], 6)
        # b unmatched left row: right side null.
        self.assertEqual(rows["Z"]["n"], 1)
        self.assertEqual(rows["Z"]["cnt_amt"], 0)
        self.assertIsNone(rows["Z"]["amt_sum"])
        self.assertEqual(rows["Z"]["qty_sum"], 4)
        # null-key left row kept in left mode with null right.
        self.assertEqual(rows["N"]["n"], 1)
        self.assertIsNone(rows["N"]["amt_sum"])
        # Types are preserved on the output columns.
        self.assertIsInstance(rows["X"]["n"], int)
        self.assertNotIsInstance(rows["X"]["n"], bool)

    def test_inner_join_drops_unmatched_and_null_keys(self):
        self.insert("lefts", [
            ("L1", lrow("a", "X", qty=1)),
            ("L2", lrow("b", "Z", qty=4)),
            ("L3", lrow(None, "N", qty=8)),
        ])
        self.insert("rights", [("R1", rrow("a", "p", amt=5))])
        self.define("mvi", join_kind="inner")
        build_fully(self.c, "mvi")
        rows = {r["grp"]: r for r in read_mv(self.c, "mvi")}
        self.assertEqual(set(rows), {"X"})
        self.assertEqual(rows["X"]["n"], 1)

    def test_unmatched_then_matched_then_unmatched_preserves_identity(self):
        self.insert("lefts", [("L1", lrow("a", "X", qty=3))])
        self.define("mv")
        build_fully(self.c, "mv")
        token_before = self.c.materialized_group_identity("mv", ("X",))[
            "group_identity"]
        # No match: left row survives with a null right side.
        rows = {r["grp"]: r for r in read_mv(self.c, "mv")}
        self.assertEqual(rows["X"]["n"], 1)
        self.assertIsNone(rows["X"]["amt_sum"])
        # First match appears.
        self.insert("rights", [("R1", rrow("a", "p", amt=5))])
        rows = {r["grp"]: r for r in read_mv(self.c, "mv")}
        self.assertEqual(rows["X"]["n"], 1)
        self.assertEqual(rows["X"]["amt_sum"], 5)
        token_matched = self.c.materialized_group_identity("mv", ("X",))[
            "group_identity"]
        self.assertEqual(token_before, token_matched)
        # Last match disappears again.
        tx = self.c.begin()
        tx.stage_delete("rights", "R1")
        tx.commit()
        rows = {r["grp"]: r for r in read_mv(self.c, "mv")}
        self.assertEqual(rows["X"]["n"], 1)
        self.assertIsNone(rows["X"]["amt_sum"])
        self.assertEqual(
            self.c.materialized_group_identity("mv", ("X",))["group_identity"],
            token_before)

    def test_group_migration_and_disappearing_group_keep_distinct_identities(self):
        self.insert("lefts", [
            ("L1", lrow("a", "X", qty=1)),
            ("L2", lrow("a", "Y", qty=2)),
        ])
        self.insert("rights", [("R1", rrow("a", "p", amt=5))])
        self.define("mv")
        build_fully(self.c, "mv")
        id_x = self.c.materialized_group_identity("mv", ("X",))["group_identity"]
        id_y = self.c.materialized_group_identity("mv", ("Y",))["group_identity"]
        self.assertNotEqual(id_x, id_y)
        # Move L1 from group X to group Y.
        tx = self.c.begin()
        tx.stage_update("lefts", "L1", {"g": "Y"})
        tx.commit()
        rows = {r["grp"]: r for r in read_mv(self.c, "mv")}
        self.assertNotIn("X", rows)           # group disappeared
        self.assertEqual(rows["Y"]["n"], 2)
        # The token for X is retained but inactive; Y keeps its own token.
        x_rec = self.c.materialized_group_identity("mv", ("X",))
        self.assertFalse(x_rec["active"])
        self.assertEqual(x_rec["group_identity"], id_x)
        self.assertEqual(
            self.c.materialized_group_identity("mv", ("Y",))["group_identity"],
            id_y)
        # Reappearance reuses the original identity, never a serial new one.
        tx = self.c.begin()
        tx.stage_update("lefts", "L1", {"g": "X"})
        tx.commit()
        self.assertEqual(
            self.c.materialized_group_identity("mv", ("X",))["group_identity"],
            id_x)
        reverse = self.c.materialized_tuple_for_identity("mv", id_x)
        self.assertEqual(reverse["group_tuple"], ("X",))

    def test_bool_int_and_null_empty_text_distinctions_in_group_tuples(self):
        self.c.add_column("lefts", "flag2", "bool", None)
        self.insert("lefts", [
            ("L1", lrow("a", "", qty=1, flag=True)),
            ("L2", lrow("a", None, qty=1, flag=False)),
        ])
        self.insert("rights", [("R1", rrow("a", "p", amt=0))])
        self.c.define_materialized_summary(
            "mvt", "lefts", "rights", "k", "k", "left",
            [{"table": "left", "column": "g"},
             {"table": "left", "column": "flag"}],
            [{"fn": "count_star", "alias": "n"}])
        build_fully(self.c, "mvt")
        # Two distinct groups: ("" , True) and (null, False); no key collision.
        rows = read_mv(self.c, "mvt")
        self.assertEqual(len(rows), 2)
        ids = self.c.list_materialized_group_identities("mvt")
        self.assertEqual(len(ids), 2)
        self.assertEqual({t["group_tuple"] for t in ids},
                         {("", True), (None, False)})

    # -- versioning -----------------------------------------------------------

    def test_unchanged_group_does_not_get_a_new_version_or_event(self):
        self.insert("lefts", [("L1", lrow("a", "X", qty=1))])
        self.insert("rights", [("R1", rrow("a", "p", amt=5))])
        self.define("mv")
        build_fully(self.c, "mv")
        first = {r["grp"]: r for r in read_mv(self.c, "mv")}["X"]
        # A commit that touches only an unrelated key.
        self.insert("lefts", [("L9", lrow("zzz", "ZZ", qty=1))])
        second = {r["grp"]: r for r in read_mv(self.c, "mv")}["X"]
        self.assertEqual(first["version"], second["version"])
        # A value-identical update on the hot key also must not bump it.
        tx = self.c.begin()
        tx.stage_update("rights", "R1", {"cat": "p"})
        tx.commit()
        third = {r["grp"]: r for r in read_mv(self.c, "mv")}["X"]
        self.assertEqual(first["version"], third["version"])
        stats = [s for s in self.c.materialized_maintenance_stats("mv", limit=10)
                 if s["phase"] == "maintenance"]
        self.assertEqual(len(stats), 2)

    # -- overflow -------------------------------------------------------------

    def test_int64_overflow_rejects_the_whole_commit(self):
        self.insert("lefts", [("L1", lrow("a", "X"))])
        self.insert("rights", [("R1", rrow("a", "p", amt=1))])
        self.c.define_materialized_summary(
            "mvs", "lefts", "rights", "k", "k", "inner", DEF_GROUPS,
            [{"fn": "sum",
              "column": {"table": "right", "column": "amt"}, "alias": "big"}])
        build_fully(self.c, "mvs")
        tx = self.c.begin()
        tx.stage_update("rights", "R1", {"amt": INT64_MAX})
        tx.commit()
        before = {r["grp"]: r for r in read_mv(self.c, "mvs")}["X"]["big"]
        self.assertEqual(before, INT64_MAX)
        # Adding one more matching row overflows: the entire commit is
        # refused and the source row insert is rolled back too.
        tx = self.c.begin()
        tx.stage_insert("rights", "R2", rrow("a", "p", amt=1))
        with self.assertRaises(ConflictError) as ctx:
            tx.commit()
        self.assertEqual(ctx.exception.code, "materialized_overflow")
        self.assertNotIn("R2", {r["row_id"] for r in self.c.rows("rights")})
        # A re-send reproduces the same durable rejection (idempotent, no
        # partial maintenance).
        with self.assertRaises(ConflictError):
            tx.commit()

    def test_transient_negative_intermediate_does_not_falsely_overflow(self):
        # The maintained total moves by (new partial - old partial).  A commit
        # that removes a big row and re-adds a smaller one makes the SUM delta
        # transiently negative; only the *final* stored total is range-checked,
        # so this must commit rather than trip a false overflow.
        big = INT64_MAX - 1000
        small = 5
        self.insert("lefts", [("L1", lrow("a", "X"))])
        self.insert("rights", [("R1", rrow("a", "p", amt=big)),
                               ("R2", rrow("a", "p", amt=small))])
        self.c.define_materialized_summary(
            "mvn", "lefts", "rights", "k", "k", "inner", DEF_GROUPS,
            [{"fn": "sum",
              "column": {"table": "right", "column": "amt"}, "alias": "big"}])
        build_fully(self.c, "mvn")
        tx = self.c.begin()
        tx.stage_delete("rights", "R1")
        tx.stage_insert("rights", "R3", rrow("a", "p", amt=1))
        tx.commit()
        rows = {r["grp"]: r for r in read_mv(self.c, "mvn")}
        self.assertEqual(rows["X"]["big"], small + 1)

    # -- read-only enforcement ------------------------------------------------

    def test_clients_cannot_stage_or_migrate_or_reference_the_summary(self):
        self.define("mv")
        build_fully(self.c, "mv")
        tx = self.c.begin()
        with self.assertRaises(MaterializedError):
            tx.stage_insert("mv", "x", {"grp": "X", "n": 1})
        with self.assertRaises(MaterializedError):
            tx.stage_update("mv", "g-00000001", {"n": 2})
        tx.abort()
        with self.assertRaises(MaterializedError):
            self.c.add_column("mv", "extra", "text", "d")
        with self.assertRaises(MaterializedError):
            self.c.register_reference("ref1", "mv", "lefts", "g", "restrict")
        with self.assertRaises(MaterializedError):
            self.c.register_reference("ref2", "lefts", "mv", "grp", "cascade")

    def test_predicate_reads_and_empty_results_are_served(self):
        self.insert("lefts", [("L1", lrow("a", "X", qty=1))])
        self.insert("rights", [("R1", rrow("a", "p", amt=5))])
        self.define("mv")
        build_fully(self.c, "mv")
        tx = self.c.begin()
        self.assertEqual(tx.read("mv", where=eq("grp", "X"))[0]["n"], 1)
        self.assertEqual(tx.read("mv", where=eq("grp", "nope")), [])
        self.assertEqual(tx.read("mv", where=is_null("grp")), [])
        tx.abort()

    # -- cancellation ----------------------------------------------------------

    def test_cancel_releases_structures_and_name_remains_defined_history(self):
        self.insert("lefts", [("L1", lrow("a", "X"))])
        self.define("mv")
        self.c.start_materialized_build("mv")
        self.c.build_materialized_step("mv", budget=1)
        st = self.c.cancel_materialized_build("mv")
        self.assertEqual(st["state"], "cancelled")
        # The physical output table and accumulator are gone.
        import sqlite3
        conn = sqlite3.connect(self.repo.path)
        try:
            names = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertNotIn("mv_rows", names)
        finally:
            conn.close()
        with self.assertRaises(MaterializedNotReady):
            read_mv(self.c, "mv")
        # The shared join index was released.
        self.assertEqual(self.c.materialized_maintenance_stats("mv"), [])

    def test_count_ignores_null_and_all_null_sum_is_null(self):
        self.c.create_table("rl", [("k", "text"), ("g", "text")])
        self.c.create_table("rr", [("k", "text"), ("a", "int64")])
        tx = self.c.begin()
        tx.stage_insert("rl", "L1", {"k": "x", "g": "G"})
        tx.stage_insert("rr", "R1", {"k": "x", "a": 5})
        tx.stage_insert("rr", "R2", {"k": "x", "a": None})
        tx.commit()
        self.c.define_materialized_summary(
            "mvn", "rl", "rr", "k", "k", "inner",
            [{"table": "left", "column": "g", "alias": "grp"}],
            [{"fn": "count_star", "alias": "n"},
             {"fn": "count", "column": {"table": "right", "column": "a"},
              "alias": "ca"},
             {"fn": "sum", "column": {"table": "right", "column": "a"},
              "alias": "sa"}])
        build_fully(self.c, "mvn")
        row = read_mv(self.c, "mvn")[0]
        self.assertEqual(row["n"], 2)       # both join rows counted
        self.assertEqual(row["ca"], 1)      # null ignored by COUNT
        self.assertEqual(row["sa"], 5)      # null ignored by SUM
        tx = self.c.begin()
        tx.stage_update("rr", "R1", {"a": None})
        tx.commit()
        row = read_mv(self.c, "mvn")[0]
        self.assertIsNone(row["sa"])        # all-null SUM -> null
        self.assertEqual(row["ca"], 0)
        self.assertEqual(row["n"], 2)       # COUNT(*) unaffected

    def test_ordinary_table_and_summary_names_are_mutually_exclusive(self):
        self.define("mv")
        # An ordinary table cannot take over a reserved summary name.
        with self.assertRaises(SchemaError):
            self.c.create_table("mv", [("x", "text")])
        # And a summary cannot reuse an existing ordinary table name.
        with self.assertRaises(MaterializedError):
            self.define("accounts", join_kind="inner")

    def test_build_lifecycle_rejects_wrong_state_calls(self):
        self.define("mvl")
        with self.assertRaises(MaterializedError):
            self.c.build_materialized_step("mvl")  # before start
        self.c.start_materialized_build("mvl")
        with self.assertRaises(MaterializedError):
            self.c.start_materialized_build("mvl")  # duplicate start
        with self.assertRaises(MaterializedError):
            self.c.build_materialized_step("mvl", budget=0)  # bad budget
        self.c.cancel_materialized_build("mvl")
        with self.assertRaises(MaterializedError):
            self.c.start_materialized_build("mvl")  # cancelled cannot restart
        with self.assertRaises(MaterializedError):
            self.c.build_materialized_step("mvl")  # build after cancel

    def test_multiple_summaries_maintained_by_one_two_sided_commit(self):
        self.insert("lefts", [("L1", lrow("x", "G", qty=1, flag=True))])
        self.insert("rights", [("R1", rrow("x", "C", amt=5))])
        self.c.define_materialized_summary(
            "m_in", "lefts", "rights", "k", "k", "inner",
            [{"table": "left", "column": "g", "alias": "grp"}],
            [{"fn": "count_star", "alias": "n"}])
        self.c.define_materialized_summary(
            "m_left", "lefts", "rights", "k", "k", "left",
            [{"table": "left", "column": "g", "alias": "grp"}],
            [{"fn": "count_star", "alias": "n"}])
        for nm in ("m_in", "m_left"):
            build_fully(self.c, nm)
        # A single commit changes both sides at once.
        tx = self.c.begin()
        tx.stage_update("lefts", "L1", lrow("x", "G", qty=2, flag=True))
        tx.stage_insert("rights", "R2", rrow("x", "C", amt=3))
        result = tx.commit()
        maintained = {m["materialized"] for m in result["materialized"]}
        self.assertEqual(maintained, {"m_in", "m_left"})
        # One left row now fans out to two right rows.
        self.assertEqual(read_mv(self.c, "m_in")[0]["n"], 2)
        self.assertEqual(read_mv(self.c, "m_left")[0]["n"], 2)
        # An unmatched left row is kept only by the left-join summary.
        tx = self.c.begin()
        tx.stage_insert("lefts", "L2", lrow("nomatch", "Z", qty=9))
        tx.commit()
        self.assertEqual(len(read_mv(self.c, "m_in")), 1)
        left_groups = {r["grp"]: r for r in read_mv(self.c, "m_left")}
        self.assertEqual(left_groups["Z"]["n"], 1)


if __name__ == "__main__":
    unittest.main()
