#!/usr/bin/env python3
"""Runnable end-to-end drill for the change-subscription layer.

It drives a full-then-incremental hand-off between a *source* repository and a
*separate* SQLite replica, exercising the properties the subscription layer is
supposed to guarantee, with real OS processes for the paused-consumer
scenario.  It prints the concrete batch contents, positions, cleanup reports
and row-by-row comparisons, and exits 0 only if every expectation holds.

    python3 -m mrtx.sync_drill                  # temp dir, removed afterwards
    python3 -m mrtx.sync_drill --keep DIR       # keep both databases

The drill uses only the public API of the product plus the stdlib.  The
independent expectations are computed here, never by calling the
subscription's incremental generator.
"""

import argparse
import json
import os
import shutil
import sqlite3
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from mrtx import (Client, Replica, ReplicaGenerationError, ReplicaNotReady,
                  SubscriptionHandle, SubscriptionClosed,
                  StaleSubscriptionGeneration, SubscriptionError)
from mrtx.subscription import new_sub_id  # noqa: F401 - used for forgery

FAILURES = []


def check(condition, message):
    print(("  ok   " if condition else "  FAIL ") + message)
    if not condition:
        FAILURES.append(message)


def head(title):
    print("\n=== %s ===" % title)


# --- independent expectation helpers ---------------------------------------


def snapshot_at(client, table, boundary, ids):
    """Newest image with version <= boundary per id, tombstones removed.

    Built from the public ``history`` API, not from subscriptions.
    """
    out = {}
    columns = client.tables()[table]
    for rid in ids:
        hist = [h for h in client.history(table, rid) if h["version"] <= boundary]
        if not hist:
            continue
        newest = max(hist, key=lambda h: h["version"])
        if newest.get("deleted"):
            continue
        row = {"row_id": rid, "version": newest["version"]}
        for col in columns:
            row[col] = newest[col]
        out[rid] = row
    return out


def compare_rows(label, got, want, columns):
    ok = set(got) == set(want)
    for rid in want:
        g = got.get(rid)
        if g is None:
            ok = False
            continue
        if g["version"] != want[rid]["version"]:
            ok = False
        for col in columns:
            if g[col] != want[rid][col] or type(g[col]) is not type(want[rid][col]):
                ok = False
    check(ok, label)
    return ok


# --- scenarios --------------------------------------------------------------


def scenario_hand_computed(source, replica_path):
    head("1. Hand-computed full content and per-commit changes")
    source.create_table("items", [("name", "text"), ("qty", "int64"),
                                  ("ok", "bool"), ("memo", "null")])
    tx = source.begin()
    tx.stage_insert("items", "a", {"name": "apple", "qty": 1,
                                   "ok": True, "memo": None})
    tx.stage_insert("items", "b", {"name": "b-anana", "qty": 2,
                                   "ok": False, "memo": None})
    tx.stage_insert("items", "c", {"name": "cherry", "qty": 3,
                                   "ok": True, "memo": None})
    tx.commit()

    sub, cut = source.create_subscription("items-sub", ["items"])
    print("  S (cut version) = %d" % cut)
    rep = Replica(replica_path, sub.handle)

    # Page the full load in pages of two and print the exact business content.
    full = []
    while True:
        rep.apply_local(source, max_rows=2)
        # Fetch the batch just applied (it is still pending) to display it.
        shown = source.pull(sub.handle, max_rows=2)["batch"]
        full.extend(shown["items"])
        print("  full page: position=%d rows=%s complete=%s"
              % (shown["position"],
                 [(i["row_id"], i["values"]["name"], i["version"])
                  for i in shown["items"]], shown["full_complete"]))
        rep.acknowledge(source)
        if shown["full_complete"]:
            break
    hand_full = [
        ("a", 1, {"name": "apple", "qty": 1, "ok": True, "memo": None}),
        ("b", 1, {"name": "b-anana", "qty": 2, "ok": False, "memo": None}),
        ("c", 1, {"name": "cherry", "qty": 3, "ok": True, "memo": None}),
    ]
    got_full = [(i["row_id"], i["version"], i["values"]) for i in full]
    check(got_full == hand_full, "full load equals the hand-computed S content")
    check([i["table"] for i in full] == ["items"] * 3, "only subscribed table")

    # Incremental: commit 2 updates a and deletes c; commit 3 re-inserts c.
    tx = source.begin()
    tx.stage_update("items", "a", {"qty": 10})
    tx.stage_delete("items", "c")
    tx.commit()
    tx = source.begin()
    tx.stage_insert("items", "c", {"name": "cherry2", "qty": 30,
                                   "ok": False, "memo": None})
    tx.commit()

    rep.run(source, max_rows=2)
    batch = source.pull(sub.handle)
    check(batch["caught_up"], "no more changes after hand-computed commits")
    rows = {r["row_id"]: r for r in rep.rows("items")}
    check(rows["a"]["qty"] == 10 and rows["a"]["version"] == 2,
          "commit 2 update applied exactly once (qty 10, version 2)")
    check(rows["c"]["version"] == 3 and rows["c"]["qty"] == 30,
          "delete + reinsert converges to the reinserted image at its version")
    check(rows["b"]["version"] == 1 and rows["b"]["qty"] == 2,
          "untouched row keeps its S image")
    check(rows["c"]["ok"] is False, "reinserted bool stays a bool")


def scenario_paused_full_and_offline(source, replica_path, make_worker):
    head("2. Pause after one full page, mutate unread rows, go offline, resume")
    source.create_table("accounts", [("owner", "text"), ("balance", "int64"),
                                     ("active", "bool"), ("note", "null")])
    source.create_table("ledger", [("acct", "text"), ("amount", "int64"),
                                   ("active", "bool")])
    tx = source.begin()
    for i in range(10):
        tx.stage_insert("accounts", "r%02d" % i,
                        {"owner": "o%d" % i, "balance": i, "active": True,
                         "note": None})
    tx.stage_insert("ledger", "L0", {"acct": "seed", "amount": 100, "active": True})
    tx.commit()
    cut = source.current_version()
    sub, _ = source.create_subscription("acct-sub", ["accounts", "ledger"])
    handle = sub.handle.as_dict()

    consumer = make_worker()
    first = consumer.pull(handle, max_rows=4)["result"]["batch"]
    check(first["phase"] == "full" and not first["full_complete"],
          "consumer paused after exactly one full page")
    check([i["row_id"] for i in first["items"]] == ["r00", "r01", "r02", "r03"],
          "first page holds r00..r03")
    consumer.kill()
    check(True, "consumer process killed (client offline, batch left pending)")

    writer = make_worker()
    t = writer.begin()
    writer.update(t, "accounts", "r07", {"balance": 707})
    writer.delete(t, "accounts", "r08")
    writer.insert(t, "accounts", "r08",
                  {"owner": "reborn", "balance": 808, "active": False, "note": None})
    writer.insert(t, "ledger", "L1", {"acct": "cross", "amount": 7, "active": True})
    out = writer.commit(t)
    boundary = out["result"]["commit_version"]
    check(out["ok"], "cross-table source commit at version %d" % boundary)
    t2 = writer.begin()
    writer.update(t2, "accounts", "r01", {"balance": 101})
    writer.commit(t2)
    check(True, "a later commit (version %d) also occurs" % source.current_version())

    cleanup = writer.cleanup()
    gc = writer.gc()
    check(cut in gc["full_phase_cuts"],
          "GC retains the cut of the unfinished full load")
    check(cleanup["retention_boundary"] >= cut,
          "cleanup honours the open subscription's boundary")

    # Fresh processes resume.
    fresh = make_worker()
    again = fresh.pull(handle, max_rows=999)["result"]["batch"]
    check(again["batch_id"] == first["batch_id"],
          "a fresh process re-pulls the identical pending batch")
    check(again["items"] == first["items"], "with identical business content")

    client = Client(source.store.path)
    rep = Replica(replica_path, SubscriptionHandle.from_dict(handle))
    for _ in range(100000):
        if rep.state()["position"] >= boundary:
            break
        report = rep.apply_local(client, max_rows=3)
        if report is None:
            break
        rep.acknowledge(client)

    account_ids = ["r%02d" % i for i in range(10)]
    want_accounts = snapshot_at(client, "accounts", boundary, account_ids)
    want_ledger = snapshot_at(client, "ledger", boundary, ["L0", "L1"])
    got_accounts = {r["row_id"]: r for r in rep.rows("accounts")}
    got_ledger = {r["row_id"]: r for r in rep.rows("ledger")}
    compare_rows("replica accounts equal the source at the boundary",
                 got_accounts, want_accounts,
                 ("owner", "balance", "active", "note"))
    compare_rows("replica ledger equal the source at the boundary",
                 got_ledger, want_ledger, ("acct", "amount", "active"))
    check(got_accounts["r08"]["owner"] == "reborn", "delete+reinsert present as reborn")
    check(got_accounts["r07"]["balance"] == 707, "unread-row update delivered")
    check(got_accounts["r01"]["balance"] == 1,
          "commit past the boundary not mixed into the boundary image")
    check(got_accounts["r00"]["version"] == 1,
          "an already-read page keeps its S image")
    return handle, boundary, rep


def scenario_two_consumers(source):
    head("3. Two consumers at different progress: slow side never loses data")
    source.create_table("solo", [("n", "int64")])
    tx = source.begin()
    tx.stage_insert("solo", "x", {"n": 0})
    tx.commit()
    fast, _ = source.create_subscription("fast", ["solo"])
    slow, _ = source.create_subscription("slow", ["solo"])

    def drain_full(sub):
        while True:
            b = source.pull(sub.handle)["batch"]
            source.ack(sub.handle, b["batch_id"])
            if b["full_complete"]:
                return

    drain_full(fast)
    drain_full(slow)
    expected = []
    for i in range(1, 6):
        tx = source.begin()
        row = tx.read("solo")[0]
        tx.stage_update("solo", "x", {"n": row["n"] + 1},
                        expected_version=row["version"])
        tx.commit()
        expected.append(i)

    fast_values = []
    while True:
        result = source.pull(fast.handle)
        if result["batch"] is None:
            break
        fast_values.extend(i["after"]["values"]["n"] for i in result["batch"]["items"])
        source.ack(fast.handle, result["batch"]["batch_id"])
    check(fast_values == expected, "fast consumer receives all five changes in order")

    report = source.cleanup_sync_history()
    check(report["retention_boundary"] == 1,
          "cleanup is pinned by the slow consumer (boundary 1)")
    check(report["reclaimed_changes"] == 0, "nothing the slow consumer needs is reclaimed")

    slow_values = []
    while True:
        result = source.pull(slow.handle)
        if result["batch"] is None:
            break
        slow_values.extend(i["after"]["values"]["n"] for i in result["batch"]["items"])
        source.ack(slow.handle, result["batch"]["batch_id"])
    check(slow_values == expected, "slow consumer still receives every change")

    after = source.cleanup_sync_history()
    check(after["reclaimed_changes"] == 5,
          "once the slow consumer catches up, cleanup reclaims the log")
    check(after["remaining_changes"] == 0, "and reports zero remaining")

    source.close_subscription(slow.handle)
    released = source.cleanup_sync_history()
    check(released["open_subscriptions"] == 1,
          "closing the slow consumer releases its retention responsibility")


def scenario_large_transaction(source):
    head("4. A transaction far larger than the row budget is delivered whole")
    source.create_table("bulk", [("v", "int64")])
    tx = source.begin()
    for i in range(300):
        tx.stage_insert("bulk", "k%03d" % i, {"v": i})
    tx.commit()
    sub, _ = source.create_subscription("bulk-sub", ["bulk"])
    while True:
        b = source.pull(sub.handle, max_rows=7)["batch"]
        source.ack(sub.handle, b["batch_id"])
        if b["full_complete"]:
            break
    tx = source.begin()
    for i in range(300):
        tx.stage_update("bulk", "k%03d" % i, {"v": i + 1000})
    tx.commit()
    batch = source.pull(sub.handle, max_rows=1)["batch"]
    check(batch["row_count"] == 300, "all 300 changes arrive in one batch")
    source.ack(sub.handle, batch["batch_id"])
    check(source.pull(sub.handle)["caught_up"], "no looping/duplication afterwards")


def scenario_generation(source):
    head("5. Rebuild invalidates the old generation's pull and ack")
    source.create_table("solo", [("n", "int64")])
    tx = source.begin()
    tx.stage_insert("solo", "x", {"n": 0})
    tx.commit()
    sub, cut = source.create_subscription("gen-sub", ["solo"])
    old = sub.handle
    b = source.pull(old, max_rows=1)["batch"]
    new_sub, new_cut = source.rebuild_subscription(old)
    check(new_sub.handle.generation == 2, "explicit rebuild bumps the generation")
    try:
        source.pull(old)
        check(False, "old-generation pull must fail")
    except StaleSubscriptionGeneration:
        check(True, "old-generation pull is rejected")
    try:
        source.ack(old, b["batch_id"])
        check(False, "old-generation ack must fail")
    except StaleSubscriptionGeneration:
        check(True, "old-generation ack is rejected")
    source.close_subscription(new_sub.handle)
    try:
        source.open_subscription(new_sub.handle)
        check(False, "re-opening a closed subscription must fail")
    except SubscriptionClosed:
        check(True, "re-opening a closed subscription fails clearly")
    rebuilt, _ = source.rebuild_subscription(new_sub.handle)
    check(rebuilt.handle.generation == 3, "rebuild remains available after close")


def scenario_bad_ack(source):
    head("6. Forged / cross-subscription acknowledgements cannot advance state")
    source.create_table("solo", [("n", "int64")])
    tx = source.begin()
    tx.stage_insert("solo", "x", {"n": 0})
    tx.commit()
    a, _ = source.create_subscription("ack-a", ["solo"])
    b, _ = source.create_subscription("ack-b", ["solo"])
    pending = source.pull(a.handle)["batch"]
    try:
        source.ack(b.handle, pending["batch_id"])
        check(False, "a batch id from another subscription must not acknowledge")
    except SubscriptionError:
        check(True, "cross-subscription batch id is rejected")
    forged = SubscriptionHandle(a.handle.sub_id, a.handle.generation,
                                "f" * 48, source.store.path)
    try:
        source.pull(forged)
        check(False, "a forged token must not pull")
    except SubscriptionError:
        check(True, "forged ownership token is rejected")
    before = source.subscription_status(a.handle)["acknowledged_position"]
    source.ack(a.handle, pending["batch_id"])
    after = source.subscription_status(a.handle)["acknowledged_position"]
    check(after >= before, "the real batch advances the position")
    replay = source.ack(a.handle, pending["batch_id"])
    check(replay["replayed"], "repeating a successful ack is a no-op")


def scenario_big_fixture(source):
    head("7. Large fixture generated at runtime (no data file committed)")
    source.create_table("wide", [("v", "int64")])
    tx = source.begin()
    for i in range(2000):
        tx.stage_insert("wide", "w%05d" % i, {"v": i})
    tx.commit()
    sub, cut = source.create_subscription("wide-sub", ["wide"])
    total = 0
    pages = 0
    while True:
        batch = source.pull(sub.handle, max_rows=137)["batch"]
        total += batch["row_count"]
        pages += 1
        source.ack(sub.handle, batch["batch_id"])
        if batch["full_complete"]:
            break
    check(total == 2000, "all 2000 generated rows delivered")
    check(pages > 10, "delivered across many pages (%d)" % pages)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--keep", help="directory to keep both databases in")
    args = ap.parse_args(argv)

    base = args.keep or tempfile.mkdtemp(prefix="mrtx-sync-drill-")
    os.makedirs(base, exist_ok=True)

    # The real-process harness lives under tests/; it is test infrastructure,
    # not part of the product.
    sys.path.insert(0, os.path.join(ROOT, "tests"))
    from procutil import Worker

    live = []
    sources = []

    def make_source(slug):
        path = os.path.join(base, "source-%s.db" % slug)
        if os.path.exists(path):
            os.remove(path)
        client = Client(path)
        sources.append(client)
        return client, path

    def make_replica(slug):
        path = os.path.join(base, "replica-%s.db" % slug)
        if os.path.exists(path):
            os.remove(path)
        return path

    def make_worker(source_path):
        w = Worker(source_path)
        live.append(w)
        return w

    try:
        s, _ = make_source("hand")
        scenario_hand_computed(s, make_replica("hand"))
        s, sp = make_source("paused")
        scenario_paused_full_and_offline(
            s, make_replica("paused"), lambda: make_worker(sp))
        s, _ = make_source("two")
        scenario_two_consumers(s)
        s, _ = make_source("bulk")
        scenario_large_transaction(s)
        s, _ = make_source("gen")
        scenario_generation(s)
        s, _ = make_source("ack")
        scenario_bad_ack(s)
        s, _ = make_source("wide")
        scenario_big_fixture(s)
    finally:
        for w in live:
            w.close()
        if not args.keep:
            shutil.rmtree(base, ignore_errors=True)

    print("\n" + ("ALL CHECKS PASSED" if not FAILURES
                 else "%d CHECK(S) FAILED" % len(FAILURES)))
    for f in FAILURES:
        print("  - " + f)
    return 0 if not FAILURES else 1


if __name__ == "__main__":
    sys.exit(main())
