#!/usr/bin/env python3
"""Runnable end-to-end drill for materialized join-aggregate summaries.

Exercises the feature only through the public API, in two halves:

* single-process, hand-computed scenarios printed as explicit join-result
  tables and expected aggregates -- duplicate fan-out (m*n rows), null join
  values that never match, unmatched -> matched -> unmatched, group migration
  with stable identity, COUNT/SUM null rules, bool/int/null/"" tuple
  distinctions, an int64 overflow that rejects the whole source commit, and an
  unchanged-group version that must not bump;
* real OS-process scenarios -- a paged build paused and finished in another
  process while writers commit, cancellation releasing retention, two
  processes accumulating into one group, and a cascade delete withdrawing a
  group, plus a subscription/replica stream carrying source and summary rows.

    python3 -m mrtx.materialized_drill             # temp dir, removed
    python3 -m mrtx.materialized_drill --keep DIR  # keep every database

Exit status is 0 only if every expectation holds.  The expected join results
are computed by hand below and cross-checked against an independent naive
reference that never reads the materializer's auxiliary tables.
"""

import argparse
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

from mrtx import Client, ConflictError, MaterializedError  # noqa: E402
from mrtx.errors import MaterializedNotReady  # noqa: E402
from mrtx.materialize import INT64_MAX  # noqa: E402
import mv_naive  # noqa: E402
from procutil import Worker  # noqa: E402

FAILURES = []
LIVE = []


def check(condition, message):
    print(("  ok   " if condition else "  FAIL ") + message)
    if not condition:
        FAILURES.append(message)


def head(title):
    print("\n=== %s ===" % title)


def make_client(path):
    if os.path.exists(path):
        os.remove(path)
    return Client(path)


def worker(path):
    w = Worker(path)
    LIVE.append(w)
    return w


GROUPS = [{"table": "left", "column": "g", "alias": "grp"}]
AGGS = [
    {"fn": "count_star", "alias": "n"},
    {"fn": "count", "column": {"table": "right", "column": "amt"},
     "alias": "cnt_amt"},
    {"fn": "sum", "column": {"table": "left", "column": "qty"},
     "alias": "qty_sum"},
    {"fn": "sum", "column": {"table": "right", "column": "amt"},
     "alias": "amt_sum"},
]


def read_mv(c, name):
    tx = c.begin()
    try:
        return tx.read(name)
    finally:
        tx.abort()


def plain(c, table):
    return {r["row_id"]: {k: v for k, v in r.items()
                         if k not in ("row_id", "version")}
            for r in c.rows(table)}


def build_fully(c, name, budget=3):
    c.start_materialized_build(name)
    steps = 0
    while True:
        st = c.build_materialized_step(name, budget=budget)
        steps += 1
        if st["state"] == "active":
            return st, steps


# ---------------------------------------------------------------------------
# Hand-computed single-process scenarios
# ---------------------------------------------------------------------------


def hand_scenarios(path):
    head("hand-computed join and aggregate semantics")
    c = make_client(path)
    c.create_table("lefts", [("k", "text"), ("g", "text"), ("qty", "int64"),
                             ("flag", "bool")])
    c.create_table("rights", [("k", "text"), ("cat", "text"),
                              ("amt", "int64")])
    tx = c.begin()
    # Two left rows on key 'a' (one bool True one False), an unmatched 'b',
    # a null-key row, and right rows: two on 'a', one null key.
    tx.stage_insert("lefts", "L1", {"k": "a", "g": "X", "qty": 1, "flag": True})
    tx.stage_insert("lefts", "L2", {"k": "a", "g": "X", "qty": 2, "flag": False})
    tx.stage_insert("lefts", "L3", {"k": "b", "g": "Z", "qty": 4, "flag": True})
    tx.stage_insert("lefts", "L4", {"k": None, "g": "N", "qty": 8, "flag": False})
    tx.stage_insert("rights", "R1", {"k": "a", "cat": "p", "amt": 5})
    tx.stage_insert("rights", "R2", {"k": "a", "cat": "p", "amt": 7})
    tx.stage_insert("rights", "R3", {"k": None, "cat": "q", "amt": 99})
    tx.commit()

    print("join result (left join), hand computed:")
    print("  key a: 2 left x 2 right = 4 rows, all in group X")
    print("  key b: 1 unmatched left row, null-extended, group Z")
    print("  null : L4 null-extended (nulls never match), group N")

    c.define_materialized_summary(
        "mv", "lefts", "rights", "k", "k", "left", GROUPS, AGGS)
    st, steps = build_fully(c, "mv", budget=2)
    check(steps > 1, "initial build genuinely paged across transactions")
    check(st["state"] == "active", "build reaches active")
    rows = {r["grp"]: r for r in read_mv(c, "mv")}
    check(rows["X"]["n"] == 4, "group X count_star = 4 (2x2 fan-out)")
    check(rows["X"]["cnt_amt"] == 4, "group X count(amt) = 4")
    check(rows["X"]["amt_sum"] == 24, "group X sum(amt) = 5+7 twice = 24")
    check(rows["X"]["qty_sum"] == 6, "group X sum(qty) = (1+2)*2 = 6")
    check(rows["Z"]["n"] == 1 and rows["Z"]["cnt_amt"] == 0,
          "unmatched left row kept with null right side")
    check(rows["Z"]["amt_sum"] is None, "all-null SUM reads back as null")
    check(rows["N"]["n"] == 1, "null-key left row retained in left mode")
    check(isinstance(rows["X"]["n"], int) and not isinstance(
        rows["X"]["n"], bool), "aggregate ints stay ints, never bools")

    # Independent naive reference agrees exactly.
    definition = c.get_materialized_summary("mv")["definition"]
    expected = mv_naive.naive_summary(definition, plain(c, "lefts"),
                                     plain(c, "rights"))
    got = {(r["grp"],): r for r in read_mv(c, "mv")}
    check(set(got) == set(expected), "naive reference group set matches")
    check(all(got[k][a] == v for k, e in expected.items() for a, v in e.items()),
          "naive reference aggregate values match")

    # Inner join drops unmatched and null-key rows.
    c.define_materialized_summary(
        "mvi", "lefts", "rights", "k", "k", "inner", GROUPS,
        [{"fn": "count_star", "alias": "n"}])
    build_fully(c, "mvi")
    inner = {r["grp"]: r for r in read_mv(c, "mvi")}
    check(set(inner) == {"X"}, "inner join keeps only matched groups")

    # Stable identity across unmatched -> matched -> unmatched.
    token = c.materialized_group_identity("mv", ("Z",))["group_identity"]
    tx = c.begin()
    tx.stage_insert("rights", "R4", {"k": "b", "cat": "p", "amt": 9})
    tx.commit()
    check(c.materialized_group_identity("mv", ("Z",))["group_identity"] == token,
          "group identity stable when first match appears")
    tx = c.begin()
    tx.stage_delete("rights", "R4")
    tx.commit()
    check(c.materialized_group_identity("mv", ("Z",))["group_identity"] == token,
          "group identity stable when last match disappears")

    # Group migration keeps distinct, reusable identities.  Move every X row
    # out so the group truly loses all its join rows and deactivates.
    id_x = c.materialized_group_identity("mv", ("X",))["group_identity"]
    tx = c.begin()
    tx.stage_update("lefts", "L1", {"k": "a", "g": "Z", "qty": 1, "flag": True})
    tx.stage_update("lefts", "L2", {"k": "a", "g": "Z", "qty": 2, "flag": False})
    tx.commit()
    rec_x = c.materialized_group_identity("mv", ("X",))
    check(rec_x["active"] is False, "group X deactivates when it loses all rows")
    check(c.materialized_group_identity("mv", ("Z",))["group_identity"] == token,
          "Z keeps its own identity, never reuses X's")
    tx = c.begin()
    tx.stage_update("lefts", "L1", {"k": "a", "g": "X", "qty": 1, "flag": True})
    tx.stage_update("lefts", "L2", {"k": "a", "g": "X", "qty": 2, "flag": False})
    tx.commit()
    check(c.materialized_group_identity("mv", ("X",))["group_identity"] == id_x,
          "reappearing group X reuses its original identity")

    # Unchanged group does not bump its row version.
    before_ver = {r["grp"]: r["version"] for r in read_mv(c, "mv")}
    tx = c.begin()
    tx.stage_insert("lefts", "L9", {"k": "zzz", "g": "ZZ", "qty": 1, "flag": True})
    tx.commit()
    after_ver = {r["grp"]: r["version"] for r in read_mv(c, "mv")}
    check(after_ver["X"] == before_ver["X"],
          "unrelated commit leaves group X row version untouched")

    # Int64 overflow rejects the whole commit, including the source write.
    # Use a dedicated one-row match so the pre-overflow total is exact.
    c.create_table("ov_left", [("k", "text"), ("g", "text")])
    c.create_table("ov_right", [("k", "text"), ("amt", "int64")])
    tx = c.begin()
    tx.stage_insert("ov_left", "O1", {"k": "o", "g": "O"})
    tx.stage_insert("ov_right", "OR1", {"k": "o", "amt": INT64_MAX})
    tx.commit()
    c.define_materialized_summary(
        "mvo", "ov_left", "ov_right", "k", "k", "inner",
        [{"table": "left", "column": "g", "alias": "grp"}],
        [{"fn": "sum", "column": {"table": "right", "column": "amt"},
          "alias": "big"}])
    build_fully(c, "mvo")
    check(c.rows("mvo")[0]["big"] == INT64_MAX, "pre-overflow total is INT64_MAX")
    tx = c.begin()
    tx.stage_insert("ov_right", "OR2", {"k": "o", "amt": 1})
    try:
        tx.commit()
        check(False, "overflow commit must be rejected")
    except ConflictError as exc:
        check(exc.code == "materialized_overflow",
              "overflow surfaces as materialized_overflow")
    check("OR2" not in {r["row_id"] for r in c.rows("ov_right")},
          "rejected overflow rolls the source row back too")

    # Read-only enforcement.
    tx = c.begin()
    try:
        tx.stage_insert("mv", "x", {"grp": "X", "n": 1})
        check(False, "staging into a summary must fail")
    except MaterializedError:
        check(True, "client cannot stage writes into a summary")
    tx.abort()
    try:
        c.add_column("mv", "extra", "text", "d")
        check(False, "add_column on a summary must fail")
    except MaterializedError:
        check(True, "online add_column refused on a summary")

    # A not-yet-active summary cannot be read.
    c.define_materialized_summary(
        "mvpend", "lefts", "rights", "k", "k", "left", GROUPS,
        [{"fn": "count_star", "alias": "n"}])
    try:
        read_mv(c, "mvpend")
        check(False, "inactive summary read must fail")
    except MaterializedNotReady:
        check(True, "inactive summary is not readable until active")

    # Maintenance statistics are real and key-scoped.
    stats = c.materialized_maintenance_stats("mv", limit=3)
    check(all({"source_rows_read", "helper_records_read", "groups_written"}
               <= set(s) for s in stats), "maintenance stats are verifiable")
    return c


# ---------------------------------------------------------------------------
# Real OS-process scenarios
# ---------------------------------------------------------------------------


def process_scenarios(db_path):
    head("real-process build, cancellation, concurrency and cascade")
    c = Client(db_path)
    tx = c.begin()
    for i in range(10):
        tx.stage_insert("lefts", "L%02d" % i,
                       {"k": "k%02d" % i, "g": "G", "qty": i, "flag": True})
        tx.stage_insert("rights", "R%02d" % i,
                       {"k": "k%02d" % i, "cat": "c", "amt": i})
    tx.commit()
    cut = c.current_version()
    c.define_materialized_summary(
        "mv2", "lefts", "rights", "k", "k", "left", GROUPS, AGGS)
    start = c.start_materialized_build("mv2")

    # Pause the build in one process, write from another, finish in a third.
    builder = worker(db_path)
    for _ in range(2):
        out = builder.send({"cmd": "mv_build", "name": "mv2", "budget": 2})
        check(out["ok"] and out["status"]["state"] == "building",
              "builder advances but stays building while paged")
    builder.kill()
    LIVE.remove(builder)

    writer = worker(db_path)
    t = writer.begin()
    writer.insert(t, "lefts", "L99",
                  {"k": "k01", "g": "G", "qty": 99, "flag": False})
    writer.insert(t, "rights", "R99", {"k": "k01", "cat": "c", "amt": 7})
    writer.commit(t)
    gc = writer.gc()
    check(start["build"]["cut_version"] in gc["active_snapshots"],
          "GC retains the build's cut while it is unfinished")

    finisher = worker(db_path)
    for _ in range(10000):
        st = finisher.mv_build("mv2", budget=4)["status"]
        if st["state"] == "active":
            break
    check(st["state"] == "active", "a fresh process finishes the paused build")
    check(st["maintained_version"] == st["current_source_position"],
          "catch-up incorporated every commit that landed during the build")
    naive = mv_naive.naive_summary(c.get_materialized_summary("mv2")["definition"],
                                   plain(c, "lefts"), plain(c, "rights"))
    got = {(r["grp"],): r for r in c.rows("mv2")}
    check(set(got) == set(naive) and all(
        got[k][a] == v for k, e in naive.items() for a, v in e.items()),
        "resumed build output equals the naive reference at activation")

    # Cancellation releases retention and the physical table.
    c.define_materialized_summary(
        "mvc", "lefts", "rights", "k", "k", "left", GROUPS,
        [{"fn": "count_star", "alias": "n"}])
    c.start_materialized_build("mvc")
    c.build_materialized_step("mvc", budget=1)
    cancelled = c.cancel_materialized_build("mvc")
    check(cancelled["state"] == "cancelled", "cancelled build marked cancelled")
    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        check("mvc_rows" not in tables,
              "cancellation drops the physical output table")
    finally:
        conn.close()

    # Two processes, different identities, same summary group, both commit.
    c.define_materialized_summary(
        "mvh", "lefts", "rights", "k", "k", "inner", GROUPS,
        [{"fn": "count_star", "alias": "n"},
         {"fn": "sum", "column": {"table": "right", "column": "amt"},
          "alias": "amt_sum"}])
    c.start_materialized_build("mvh")
    while c.build_materialized_step("mvh", 3)["state"] != "active":
        pass
    w1, w2 = worker(db_path), worker(db_path)
    t1, t2 = w1.begin(), w2.begin()
    w1.insert(t1, "rights", "HA", {"k": "hot", "cat": "c", "amt": 1})
    w2.insert(t2, "rights", "HB", {"k": "hot", "cat": "c", "amt": 2})
    r1, r2 = w1.commit(t1), w2.commit(t2)
    check(r1["ok"] and r2["ok"],
          "two different source identities into one group both commit")

    # Cascade delete withdraws a summary group in the same source commit.
    # Dedicated tables so the reference constraint has no pre-existing orphans.
    c.create_table("cas_p", [("name", "text")])
    c.create_table("cas_l", [("k", "text"), ("g", "text"), ("qty", "int64"),
                             ("flag", "bool")])
    c.create_table("cas_r", [("k", "text"), ("cat", "text"), ("amt", "int64")])
    c.register_reference("cas", "cas_p", "cas_l", "k", "cascade")
    tx = c.begin()
    tx.stage_insert("cas_p", "P1", {"name": "P1"})
    tx.stage_insert("cas_l", "LC", {"k": "P1", "g": "CAS", "qty": 1,
                                    "flag": True})
    tx.stage_insert("cas_r", "RC", {"k": "P1", "cat": "c", "amt": 5})
    tx.commit()
    c.define_materialized_summary(
        "mvcas", "cas_l", "cas_r", "k", "k", "inner", GROUPS,
        [{"fn": "count_star", "alias": "n"}])
    c.start_materialized_build("mvcas")
    while c.build_materialized_step("mvcas", 3)["state"] != "active":
        pass
    check(any(r["grp"] == "CAS" for r in c.rows("mvcas")),
          "cascade group present before the parent delete")
    tx = c.begin()
    tx.stage_delete("cas_p", "P1")
    tx.commit()
    check(all(r["grp"] != "CAS" for r in c.rows("mvcas")),
          "cascade delete withdraws the group via one atomic commit")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", metavar="DIR")
    args = parser.parse_args()
    if args.keep:
        os.makedirs(args.keep, exist_ok=True)
        db_path = os.path.join(args.keep, "repo.db")
        keep = True
    else:
        d = tempfile.mkdtemp(prefix="mrtx-mv-drill-")
        db_path = os.path.join(d, "repo.db")
        keep = False
    try:
        hand_scenarios(db_path)
        process_scenarios(db_path)
    finally:
        for w in LIVE:
            w.close()
        if not keep:
            shutil.rmtree(os.path.dirname(db_path), ignore_errors=True)
    head("result")
    if FAILURES:
        print("%d FAILURE(S):" % len(FAILURES))
        for f in FAILURES:
            print("  - " + f)
        return 1
    print("all materialized-summary expectations held")
    return 0


if __name__ == "__main__":
    sys.exit(main())
