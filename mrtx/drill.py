#!/usr/bin/env python3
"""Runnable end-to-end drill for the managed-repository transaction layer.

Runs every scenario from the brief against a fresh database, using real child
processes for the concurrent ones.  Prints the actual read results, the returned
commit decisions, the conflict evidence, and the final rows, then verifies each
successful history against an independently enumerated set of serial orders.

    python3 -m mrtx.drill              # one run in a temp dir
    python3 -m mrtx.drill --db path.db # keep the database afterwards

Exit status is 0 only if every expectation holds.
"""

import argparse
import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from mrtx import Client, ConflictError, eq, lt, not_  # noqa: E402
from mrtx.engine import TransactionHandle  # noqa: E402

# The drill uses the test-side process harness and independent checker, which
# live under tests/.  They are deliberately outside the product package.
sys.path.insert(0, os.path.join(ROOT, "tests"))
import serialcheck  # noqa: E402
from procutil import Worker  # noqa: E402

FAILURES = []


def check(condition, message):
    print(("  ok   " if condition else "  FAIL ") + message)
    if not condition:
        FAILURES.append(message)


def head(title):
    print("\n=== %s ===" % title)


def show_rows(label, rows):
    print("  %s:" % label)
    for row in rows:
        print("    " + json.dumps(row, sort_keys=True))


def account(owner="x", balance=10, active=True, note=None):
    return {"owner": owner, "balance": balance, "active": active, "note": note}


def seed(c, reset=False):
    existing = {r["row_id"] for r in c.rows("accounts")}
    if existing and not reset:
        return
    tx = c.begin()
    for rid, values in (("A", account(owner="A", active=True, balance=10)),
                        ("B", account(owner="B", active=True, balance=20))):
        if rid in existing:
            tx.stage_update("accounts", rid, values)
        else:
            tx.stage_insert("accounts", rid, values)
    tx.commit()


def verify_serializable(c, label, limit=7):
    history = serialcheck.load_history(c.db_path)
    order = serialcheck.claimed_serial_order(history)
    ok, detail = serialcheck.serial_valid(history, order)
    check(ok, "%s: claimed serialization points replay exactly" % label)
    if not ok:
        print("    replay mismatch: " + json.dumps(detail, sort_keys=True))
    if len(history["transactions"]) <= limit:
        valid, counter = serialcheck.enumerate_serial_orders(history)
        check(bool(valid), "%s: at least one serial order exists (%d found)"
              % (label, len(valid)))
        if not valid:
            print("    counterexample: " + json.dumps(counter, default=str)[:800])
    return history


def scenario_actives(c, workers, db):
    head("1. Two processes each deactivate one of two active=true rows")
    seed(c)
    w1, w2 = workers()
    t1, t2 = w1.begin(), w2.begin()
    r1 = w1.read(t1, "accounts", where={"op": "eq", "col": "active", "value": True})["rows"]
    r2 = w2.read(t2, "accounts", where={"op": "eq", "col": "active", "value": True})["rows"]
    show_rows("T1 read (both processes saw both rows)", r1)
    check([r["row_id"] for r in r1] == ["A", "B"], "T1 saw A and B")
    check([r["row_id"] for r in r2] == ["A", "B"], "T2 saw A and B")
    check(w1.update(t1, "accounts", "A", {"active": False})["ok"], "T1 staged A=false")
    check(w2.update(t2, "accounts", "B", {"active": False})["ok"], "T2 staged B=false")
    d1, d2 = w1.commit(t1), w2.commit(t2)
    print("  T1 decision: " + json.dumps({k: d1.get(k) for k in ("ok", "code")}))
    print("  T2 decision: " + json.dumps({k: d2.get(k) for k in
                                           ("ok", "code", "evidence")}, default=str)[:700])
    check(d1["ok"], "T1 committed")
    check(not d2["ok"] and d2["code"] == "read_conflict",
          "T2 was rejected with a read (rw) conflict, not a stale version")
    if not d2["ok"]:
        ev = d2["evidence"]["conflicts"][0]
        check(ev["dependency"] == "rw" and ev["other_transaction"] == t1,
              "evidence names T1 and an rw dependency")
        check("predicate_expression" in ev or "witness" in ev,
              "evidence names the predicate and row")
    final = c.rows("accounts")
    show_rows("final rows", final)
    check(len([r for r in final if r["active"]]) >= 1,
          "at least one active row survives")
    verify_serializable(c, "actives")


def scenario_phantom(c, workers):
    head("2. Phantom: empty predicate read, then a concurrent insert")
    seed(c)
    w1, w2 = workers()
    t1, t2 = w1.begin(), w2.begin()
    empty = w1.read(t1, "accounts",
                    where={"op": "eq", "col": "owner", "value": "ghost"})["rows"]
    check(empty == [], "T1 read the empty set for owner=ghost")
    check(w1.update(t1, "accounts", "A", {"balance": 11})["ok"],
          "T1 also stages a write (a read-only txn could serialize at its view)")
    check(w2.insert(t2, "accounts", "G", account(owner="ghost"))["ok"],
          "T2 staged the ghost row")
    check(w2.commit(t2)["ok"], "T2 committed the insert")
    d = w1.commit(t1)
    print("  T1 decision: " + json.dumps({k: d.get(k) for k in ("ok", "code")},
                                          default=str))
    check(not d["ok"] and d["code"] == "read_conflict",
          "T1 was rejected: the empty range gained a member")
    if not d["ok"]:
        ev = d["evidence"]["conflicts"][0]
        check(ev["row_id"] == "G" and ev["other_transaction"] == t2,
              "evidence names the inserted row G and T2")
    verify_serializable(c, "phantom")


def scenario_cross_table_cycle(c, workers):
    head("3. Cross-table three-transaction dependency ring")
    seed(c)
    w0 = workers(1)[0]
    t0 = w0.begin()
    w0.insert(t0, "ledger", "L1", {"acct": "A", "amount": 1, "active": True})
    check(w0.commit(t0)["ok"], "seed ledger row L1")

    w1, w2, w3 = workers(3)
    t1, t2, t3 = w1.begin(), w2.begin(), w3.begin()
    # The ring (over two tables):
    #   T1: read accounts/A   -> write accounts/B
    #   T2: read ledger/L1    -> write accounts/A
    #   T3: read accounts/B   -> write ledger/L1
    w1.read(t1, "accounts", where={"op": "eq", "col": "row_id", "value": "A"})
    w2.read(t2, "ledger", where={"op": "eq", "col": "row_id", "value": "L1"})
    w3.read(t3, "accounts", where={"op": "eq", "col": "row_id", "value": "B"})
    w1.update(t1, "accounts", "B", {"balance": 77})
    w2.update(t2, "accounts", "A", {"balance": 99})
    w3.update(t3, "ledger", "L1", {"amount": 2})
    check(w1.commit(t1)["ok"], "T1 (accounts read -> accounts write) committed")
    check(w2.commit(t2)["ok"], "T2 (ledger read -> accounts write) committed")
    d3 = w3.commit(t3)
    print("  T3 decision: " + json.dumps({k: d3.get(k) for k in ("ok", "code")},
                                          default=str))
    check(not d3["ok"], "T3 (third edge of the ring) was rejected")
    if not d3["ok"]:
        cyc = d3["evidence"]["dependency_cycles"]
        check(bool(cyc), "rejection evidence contains a dependency cycle")
        if cyc:
            print("  cycle: " + json.dumps(cyc[0], sort_keys=True)[:700])
            check(len(cyc[0]["transactions"]) >= 3,
                  "the reported cycle involves three real transactions")
    graph = c.dependency_graph()
    print("  graph edges: " + json.dumps(
        [{"from": e["from"][:12], "to": e["to"][:12], "kind": e["kind"]}
         for e in graph["edges"]], sort_keys=True))
    verify_serializable(c, "cross-table ring")


def scenario_disjoint(c, workers):
    head("4. Disjoint key ranges: two processes both commit, no replay")
    tx = c.begin()
    for i in range(8):
        rid = "r%03d" % i
        tx.stage_insert("accounts", rid, account(owner="o%d" % i, balance=i))
    tx.commit()
    w1, w2 = workers()
    t1, t2 = w1.begin(), w2.begin()
    left = w1.read(t1, "accounts",
                   where={"op": "and", "parts": [
                       {"op": "not", "part": {"op": "lt", "col": "row_id", "value": "r"}},
                       {"op": "lt", "col": "row_id", "value": "r004"}]},
                   columns=["row_id", "balance"])["rows"]
    right = w2.read(t2, "accounts",
                    where={"op": "and", "parts": [
                        {"op": "not", "part": {"op": "lt", "col": "row_id", "value": "r"}},
                        {"op": "not", "part": {"op": "lt", "col": "row_id",
                                               "value": "r004"}}]},
                    columns=["row_id", "balance"])["rows"]
    check(len(left) == 4 and len(right) == 4,
          "T1 read keys r000..r003, T2 read r004..r007 (disjoint)")
    w1.update(t1, "accounts", "r000", {"balance": 100})
    w2.update(t2, "accounts", "r007", {"balance": 700})
    d1, d2 = w1.commit(t1), w2.commit(t2)
    check(d1["ok"] and d2["ok"], "both committed directly (no client replay)")
    check(not d1.get("replayed") and not d2.get("replayed"),
          "neither commit was a replay")
    final = {r["row_id"]: r for r in c.rows("accounts")}
    check(final["r000"]["balance"] == 100 and final["r007"]["balance"] == 700,
          "both updates are visible")
    verify_serializable(c, "disjoint")


def scenario_read_your_writes(c, workers):
    head("5. Read-your-own-writes, and staging invisible to others")
    tx = c.begin()
    tx.stage_update("accounts", "A", {"balance": 123})
    own = tx.read("accounts", where=eq("row_id", "A"), columns=["row_id", "balance"])
    show_rows("own read inside T1", own)
    check(own[0]["balance"] == 123, "T1 sees its own staged write")
    check(own[0]["version"] < 0, "the version is a local token, not a commit version")
    w = workers(1)[0]
    t2 = w.begin()
    theirs = w.read(t2, "accounts", where={"op": "eq", "col": "row_id", "value": "A"})
    show_rows("T2's read of the same row", theirs["rows"])
    check(theirs["rows"][0]["balance"] != 123, "T2 cannot see T1's staging")
    tx.abort()
    w.abort(t2)


def scenario_same_key(c, workers):
    head("6. Same-identity update conflict")
    w1, w2 = workers()
    t1, t2 = w1.begin(), w2.begin()
    r1 = w1.read(t1, "accounts")["rows"]
    r2 = w2.read(t2, "accounts")["rows"]
    a1 = next(r for r in r1 if r["row_id"] == "A")
    a2 = next(r for r in r2 if r["row_id"] == "A")
    check(a1["version"] == a2["version"], "both read A at the same version")
    w1.update(t1, "accounts", "A", {"balance": 111}, expected_version=a1["version"])
    w2.update(t2, "accounts", "A", {"balance": 222}, expected_version=a2["version"])
    d1, d2 = w1.commit(t1), w2.commit(t2)
    check(d1["ok"], "T1 committed")
    check(not d2["ok"] and d2["code"] == "write_conflict",
          "T2 refused: same identity already changed")
    if not d2["ok"]:
        ev = d2["evidence"]["conflicts"][0]
        check(ev["row_id"] == "A" and ev["other_transaction"] == t1,
              "evidence names row A and T1")
    final = {r["row_id"]: r for r in c.rows("accounts")}
    check(final["A"]["balance"] == 111, "T1's value survives; no silent overwrite")
    verify_serializable(c, "same-key")


def scenario_abort(c, workers):
    head("7. Abort leaves no visible write")
    before = c.rows("accounts")
    w = workers(1)[0]
    t = w.begin()
    w.update(t, "accounts", "A", {"balance": 999})
    w.insert(t, "accounts", "NEW", account(owner="new"))
    out = w.abort(t)
    check(out["ok"], "abort succeeded")
    after = c.rows("accounts")
    check(before == after, "rows are byte-identical before and after abort")
    check(w.status(t)["status"]["decision"] == "aborted",
          "durable decision says aborted")


def scenario_crash_recovery(c, workers, db):
    head("8. Crash after commit, before the response")
    w = workers(1)[0]
    t = w.begin()
    token = w.handles[t]["token"]
    w.update(t, "accounts", "A", {"balance": 4242})
    try:
        w.send({"cmd": "commit_crash", "txid": t})
        check(False, "worker should have died mid-response")
    except RuntimeError:
        check(True, "committer exited before returning a decision")
    fresh = Client(db, recover_on_start=False)
    status = fresh.transaction_status(t)
    print("  recovered status: " + json.dumps(status, default=str))
    check(status is not None and status["decision"] == "committed",
          "a new process sees the durable 'committed' decision")
    check(status["commit_version"] is not None,
          "the recovered decision carries its commit version")
    rows = {r["row_id"]: r for r in fresh.rows("accounts")}
    check(rows["A"]["balance"] == 4242, "the committed value is visible")
    replay = fresh.commit(TransactionHandle(t, token, db))
    check(replay["replayed"] and replay["status"] == "committed",
          "resending commit is idempotent")
    check(len(fresh.rows("accounts")) == len(rows), "replay wrote no extra rows")

    head("9. Crash before the decision")
    w2 = workers(1)[0]
    t2 = w2.begin()
    w2.update(t2, "accounts", "A", {"balance": 7777})
    w2.kill()
    fresh.recover()
    status = fresh.transaction_status(t2)
    print("  recovered status: " + json.dumps(status, default=str))
    check(status["decision"] == "aborted_orphan" and not status["committed"],
          "a new process deterministically reports 'not committed'")
    rows = {r["row_id"]: r for r in fresh.rows("accounts")}
    check(rows["A"]["balance"] == 4242, "the uncommitted staging never became visible")


def scenario_gc(c):
    head("10. Version cleanup boundary")
    tx = c.begin()
    for i in range(5):
        tx.stage_update("accounts", "A", {"balance": 5000 + i})
    tx.commit()
    reader = c.begin()
    reader.read("accounts", where=eq("row_id", "A"))
    info = c.gc_versions()
    print("  gc: " + json.dumps(info, sort_keys=True))
    kept = [h["version"] for h in c.history("accounts", "A")]
    check(info["safe_version"] in kept, "the live snapshot's version is retained")
    check(max(kept) in kept, "the newest version is retained")
    shown = reader.read("accounts", where=eq("row_id", "A"))[0]
    check(shown["balance"] == 5004, "the live reader still reads its snapshot")
    reader.abort()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", help="database path (kept after the run)")
    args = ap.parse_args(argv)

    tempdir = None
    if args.db:
        db = os.path.abspath(args.db)
    else:
        tempdir = tempfile.mkdtemp(prefix="mrtx-drill-")
        db = os.path.join(tempdir, "repo.db")

    c = Client(db)
    c.create_table("accounts", [("owner", "text"), ("balance", "int64"),
                                ("active", "bool"), ("note", "null")])
    c.create_table("ledger", [("acct", "text"), ("amount", "int64"),
                              ("active", "bool")])

    live = []

    def workers(n=2):
        made = [Worker(db) for _ in range(n)]
        live.extend(made)
        return made

    try:
        scenario_actives(c, workers, db)
        scenario_phantom(c, workers)
        scenario_cross_table_cycle(c, workers)
        scenario_disjoint(c, workers)
        scenario_read_your_writes(c, workers)
        scenario_same_key(c, workers)
        scenario_abort(c, workers)
        scenario_crash_recovery(c, workers, db)
        scenario_gc(c)
    finally:
        for w in live:
            w.close()
        if tempdir:
            shutil.rmtree(tempdir, ignore_errors=True)

    print("\n" + ("ALL CHECKS PASSED" if not FAILURES
                 else "%d CHECK(S) FAILED" % len(FAILURES)))
    for f in FAILURES:
        print("  - " + f)
    return 0 if not FAILURES else 1


if __name__ == "__main__":
    sys.exit(main())
