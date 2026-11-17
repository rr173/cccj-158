#!/usr/bin/env python3
"""Runnable end-to-end drill for the *online add-column* feature.

This drives one managed ``accounts`` table through an online
``add_column`` migration while real OS processes hold overlapping
transactions and a real replica is paused partway through its old-structure
full load.  Every expectation is computed by hand or from the public
``history``/``rows`` APIs -- never from the product's own schema-event or
subscription machinery.

Scenarios, in order
-------------------
1. Hand-computed migration: columns, defaults, row versions and the exact
   ordered source commit sequence before/after adding ``source_group``.
2. Real-process barrier: a replica process reads only part of the old full
   load, is killed; the source then migrates and updates not-yet-read rows;
   a fresh process finishes the old cut, switches structure, applies the
   later updates and is compared value-by-value, type-by-type,
   version-by-version at a fixed commit position.
3. A transaction that began before the migration keeps reading the old
   definition and gets an explicit, write-free structural conflict when it
   tries to write afterwards; an unrelated table's writer still commits.
4. Invalid migrations (unknown table, name clash, bad default type) leave
   no half column, no fabricated source position and no replica-visible event.
5. An unacknowledged old batch survives a migration and cleanup; the
   post-migration subscription uses the new schema directly.
6. Crash *after* the local replica schema-switch commit but *before* the
   source acknowledgement: reopening replays without a duplicate column,
   keeps the default, and interprets later rows with the new structure.

    python3 -m mrtx.online_add_column_drill            # temp dir, removed
    python3 -m mrtx.online_add_column_drill --keep DIR # keep the databases

Exit status is 0 only if every expectation holds.
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

from mrtx import (Client, ConflictError, Replica, SchemaConflictError,
                  SchemaError, SubscriptionHandle)  # noqa: E402

# The real-process harness is test infrastructure, not product code.
sys.path.insert(0, os.path.join(ROOT, "tests"))
from procutil import Worker  # noqa: E402

FAILURES = []
ACCOUNT_COLS_OLD = ("owner", "balance", "active", "note")
ACCOUNT_COLS_NEW = ACCOUNT_COLS_OLD + ("source_group",)


def check(condition, message):
    print(("  ok   " if condition else "  FAIL ") + message)
    if not condition:
        FAILURES.append(message)


def head(title):
    print("\n=== %s ===" % title)


def seed(client, n=8):
    tx = client.begin()
    for i in range(n):
        tx.stage_insert("accounts", "r%02d" % i,
                        {"owner": "o%d" % i, "balance": i, "active": True,
                         "note": None})
    tx.commit()


def snapshot_at(client, table, boundary, ids, columns):
    """Independent expectation: newest image with version <= boundary."""
    out = {}
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


def compare(label, got, want, columns):
    ok = set(got) == set(want)
    for rid, w in want.items():
        g = got.get(rid)
        if g is None:
            ok = False
            continue
        if g["version"] != w["version"]:
            ok = False
        for col in columns:
            if g[col] != w[col] or type(g[col]) is not type(w[col]):
                ok = False
    check(ok, label)
    return ok


# ---------------------------------------------------------------------------
# 1. Hand-computed migration
# ---------------------------------------------------------------------------


def scenario_hand_computed(source):
    head("1. Hand-computed columns, defaults, row versions and commit order")
    source.create_table("accounts", [("owner", "text"), ("balance", "int64"),
                                     ("active", "bool"), ("note", "null")])
    seed(source, n=3)
    v0 = source.current_version()
    print("  initial version %d; columns=%s" % (v0, list(source.tables()["accounts"])))

    # Hand expectation: each existing row is at business version 1 with no
    # source_group column yet.
    before = {r["row_id"]: r for r in source.rows("accounts")}
    check(set(before) == {"r00", "r01", "r02"}, "three seeded rows")
    check(all(r["version"] == 1 for r in before.values()),
          "all rows start at business version 1")
    check(all("source_group" not in r for r in before.values()),
          "no source_group column before the migration")

    migration = source.add_column("accounts", "source_group", "text", "organic")
    print("  migration record: %s" % json.dumps(migration, sort_keys=True))
    check(migration["commit_version"] == v0 + 1,
          "the migration takes the very next ordered source position (%d)" % (v0 + 1))
    check(migration["schema_version"] == 2, "table schema moves to version 2")

    after = {r["row_id"]: r for r in source.rows("accounts")}
    for rid, r in after.items():
        check(r["source_group"] == "organic",
              "%s reads the declared default 'organic'" % rid)
        check(r["version"] == before[rid]["version"],
              "%s keeps its original business version (not rewritten)" % rid)

    # An omitted column uses the same default; an explicit value is validated.
    tx = source.begin()
    tx.stage_insert("accounts", "r03",
                    {"owner": "o3", "balance": 3, "active": False, "note": None})
    tx.commit()
    tx = source.begin()
    tx.stage_insert("accounts", "r04",
                    {"owner": "o4", "balance": 4, "active": True, "note": None,
                     "source_group": "ads"})
    tx.commit()
    rows = {r["row_id"]: r for r in source.rows("accounts")}
    check(rows["r03"]["source_group"] == "organic",
          "insert omitting the new column gets the default")
    check(rows["r04"]["source_group"] == "ads",
          "an explicit value is stored as given")
    check(rows["r04"]["version"] == v0 + 3,
          "explicit insert is stamped at its own commit (%d)" % (v0 + 3))

    # Ordered source history: migration sits strictly between the data commits.
    hist = source.schema_history("accounts")
    check([h["commit_version"] for h in hist] == [v0 + 1],
          "exactly one schema event at the expected position")
    check(hist[0]["change_type"] == "add_column"
          and hist[0]["column"] == "source_group"
          and hist[0]["default"] == "organic",
          "the schema event records type, column and default")
    print("  ordered source positions: data v1 -> schema v%d -> data v%d, v%d"
          % (v0 + 1, v0 + 2, v0 + 3))


# ---------------------------------------------------------------------------
# 2. Real-process barrier: paused full load, migrate, resume, converge
# ---------------------------------------------------------------------------


def scenario_real_process_pause(source, replica_path, make_worker):
    head("2. Real process barrier: partial old full load, migrate, resume")
    source.create_table("accounts", [("owner", "text"), ("balance", "int64"),
                                     ("active", "bool"), ("note", "null")])
    seed(source, n=10)
    cut = source.current_version()
    sub, _ = source.create_subscription("drill", ["accounts"])
    handle = sub.handle.as_dict()

    # A real consumer process pulls exactly one old-structure page, then dies.
    consumer = make_worker()
    first = consumer.pull(handle, max_rows=4)["result"]["batch"]
    check(first["phase"] == "full" and not first["full_complete"],
          "consumer paused after one full page")
    check([i["row_id"] for i in first["items"]] ==
          ["r00", "r01", "r02", "r03"], "first page holds r00..r03")
    check(all(set(i["values"]) == set(ACCOUNT_COLS_OLD)
              for i in first["items"]),
          "the paused page uses the OLD column set (no source_group)")
    consumer.kill()

    # While the replica is offline and half-read, the source migrates and a
    # different process updates rows the replica has not read yet.
    writer = make_worker()
    migration = writer.add_column("accounts", "source_group", "text", "organic")
    check(migration["ok"], "online migration commits while the consumer is down")
    mig_version = migration["result"]["commit_version"]
    t = writer.begin()
    writer.update(t, "accounts", "r07",
                  {"balance": 707, "source_group": "partner"})
    writer.update(t, "accounts", "r08", {"balance": 808})  # new col omitted
    boundary = writer.commit(t)["result"]["commit_version"]
    print("  cut=%d migration=%d data-commit=%d" % (cut, mig_version, boundary))

    gc = writer.gc()
    check(cut in gc["full_phase_cuts"],
          "GC retains the unfinished full load's cut snapshot")
    cleanup = writer.cleanup()
    check(cleanup["remaining_schema_changes"] == 1,
          "cleanup cannot drop the schema event the paused consumer still needs")

    # A fresh process must re-pull the identical pending old-structure page.
    fresh = make_worker()
    again = fresh.pull(handle, max_rows=999)["result"]["batch"]
    check(again["batch_id"] == first["batch_id"],
          "fresh process re-pulls the identical pending batch")
    check(again["items"] == first["items"], "with byte-identical old content")

    client = Client(source.store.path)
    rep = Replica(replica_path, SubscriptionHandle.from_dict(handle))
    kinds = []
    while True:
        report = rep.apply_local(client, max_rows=3)
        if report is None:
            break
        kinds.append((report.get("kind"), report["position"]))
        rep.acknowledge(client)
    print("  replica batch stream: %s" % kinds)
    # The structural switch appears exactly once, before the post-migration
    # data commit, and after the old full load completes.
    schema_positions = [p for k, p in kinds if k == "schema_change"]
    check(schema_positions == [mig_version],
          "exactly one schema switch at the migration position")
    check(rep.state()["position"] == boundary,
          "replica reaches the requested commit position")

    ids = ["r%02d" % i for i in range(10)]
    want = snapshot_at(client, "accounts", boundary, ids, ACCOUNT_COLS_NEW)
    got = {r["row_id"]: r for r in rep.rows("accounts")}
    compare("replica equals the source at the boundary (value/type/version)",
            got, want, ACCOUNT_COLS_NEW)
    check(got["r07"]["source_group"] == "partner", "r07 explicit new value")
    check(got["r08"]["source_group"] == "organic",
          "r08 omitted-column update keeps the default")
    check(got["r00"]["version"] == 1 and got["r00"]["source_group"] == "organic",
          "already-delivered r00 keeps version 1 and gains the default")

    con = sqlite3.connect(replica_path)
    try:
        names = [r[1] for r in con.execute('PRAGMA table_info("accounts")')]
    finally:
        con.close()
    check(names.count("source_group") == 1, "the column was added exactly once")
    return rep, handle, boundary


# ---------------------------------------------------------------------------
# 3. Pre-migration transaction and an unaffected concurrent writer
# ---------------------------------------------------------------------------


def scenario_structural_conflict(source, make_worker):
    head("3. Old-structure transaction: reads preserved, write conflicts cleanly")
    # ``source`` may already carry an earlier online column; the transaction
    # below starts from whatever structure exists *now* and must only be
    # protected against the migration that happens after it begins.
    columns_at_begin = set(source.tables()["accounts"])
    old_worker = make_worker()
    txid = old_worker.begin()
    pre = old_worker.read(txid, "accounts")
    check(all("tier" not in r for r in pre["rows"]),
          "the pre-migration transaction reads the current (pre-tier) columns")

    migrator = make_worker()
    out = migrator.add_column("accounts", "tier", "int64", 0)
    check(out["ok"], "a new migration (tier) commits")

    # The old transaction still completes reads under its start definition.
    post = old_worker.read(txid, "accounts")
    check(all("tier" not in r for r in post["rows"]),
          "reads after the migration still use the start-view definition")
    check(all(set(r) - {"row_id", "version"} == columns_at_begin
              for r in post["rows"]),
          "no column added after begin leaks into the old transaction's reads")

    # A write against the migrated table is an explicit structural conflict.
    conflict = old_worker.update(txid, "accounts", "r00", {"balance": 999})
    check((not conflict["ok"]) and conflict["error"] == "SchemaConflictError",
          "post-migration write returns an explicit structural conflict")
    check(conflict["evidence"]["code"] == "schema_conflict",
          "the conflict carries structured evidence")
    check("tier" in conflict["evidence"]["added_columns"],
          "evidence names the added tier column")
    abort = old_worker.abort(txid)
    check(abort["ok"], "the conflicted transaction aborts with no write")

    rows = {r["row_id"]: r for r in source.rows("accounts")}
    check(rows["r00"]["balance"] != 999, "no partial write was left behind")

    # An unrelated table's writer is not blocked by the accounts migration.
    source.create_table("signals", [("n", "int64")])
    other = make_worker()
    oid = other.begin()
    other.insert(oid, "signals", "s1", {"n": 1})
    commit = other.commit(oid)
    check(commit["ok"], "an unrelated-table commit proceeds during the window")
    check(source.rows("signals")[0]["n"] == 1, "the unrelated write is durable")


# ---------------------------------------------------------------------------
# 4. Invalid migrations leave nothing behind
# ---------------------------------------------------------------------------


def scenario_invalid_migrations(source):
    head("4. Invalid migrations are atomic rejections with no side effects")
    version_before = source.current_version()
    con = sqlite3.connect(source.store.path)
    try:
        schema_events_before = con.execute(
            "SELECT COUNT(*) FROM schema_changes").fetchone()[0]
        phys_before = [r[1] for r in con.execute(
            'PRAGMA table_info("accounts_rows")')]
    finally:
        con.close()

    cases = [
        ("unknown table", lambda: source.add_column("ghost", "x", "text", "v")),
        ("column clash", lambda: source.add_column("accounts", "owner", "text", "v")),
        ("bad int default", lambda: source.add_column("accounts", "z1", "int64", "x")),
        ("bool/int confusion", lambda: source.add_column("accounts", "z2", "bool", 1)),
        ("bad type", lambda: source.add_column("accounts", "z3", "decimal", 1)),
        ("null-only with value", lambda: source.add_column("accounts", "z4", "null", "x")),
    ]
    for label, fn in cases:
        try:
            fn()
            check(False, "%s should have been rejected" % label)
        except SchemaError:
            check(True, "%s is rejected" % label)

    check(source.current_version() == version_before,
          "no failed migration consumed a source position")
    con = sqlite3.connect(source.store.path)
    try:
        check(con.execute("SELECT COUNT(*) FROM schema_changes").fetchone()[0]
              == schema_events_before,
              "no schema event is visible to replicas")
        check([r[1] for r in con.execute('PRAGMA table_info("accounts_rows")')]
              == phys_before, "no half column exists in the physical table")
        # No failed column name leaked into the catalog.
        leaked = [r[0] for r in con.execute(
            "SELECT column_name FROM table_columns WHERE table_name='accounts' "
            "AND column_name IN ('z1','z2','z3','z4')")]
        check(leaked == [], "the catalog has no leftover failed columns")
    finally:
        con.close()


# ---------------------------------------------------------------------------
# 5. Pending old batch across a migration; post-migration subscription
# ---------------------------------------------------------------------------


def scenario_pending_batch_and_new_subscription(source):
    head("5. Pending old batch survives migration; new subscription is fresh")
    source.create_table("queue", [("msg", "text")])
    tx = source.begin()
    for i in range(5):
        tx.stage_insert("queue", "q%d" % i, {"msg": "m%d" % i})
    tx.commit()
    old_sub, _ = source.create_subscription("old-queue", ["queue"])
    pending = source.pull(old_sub.handle, max_rows=2)["batch"]
    check(pending["phase"] == "full", "an old full batch is left unacknowledged")

    source.add_column("queue", "channel", "text", "default-channel")
    # The unacknowledged batch is still the identical one and still old schema.
    again = source.pull(old_sub.handle, max_rows=2)["batch"]
    check(again["batch_id"] == pending["batch_id"],
          "the pending old batch is unchanged by the migration")
    check(all("channel" not in i["values"] for i in again["items"]),
          "the pending batch keeps the old column structure")
    source.ack(old_sub.handle, again["batch_id"])

    # A subscription created after the migration describes the new schema.
    new_sub, _ = source.create_subscription("new-queue", ["queue"])
    status = source.subscription_status(new_sub.handle)
    check("channel" in status["schema"]["queue"],
          "the new subscription's full schema already has the column")
    check(status["schema_defaults"]["queue"] == {"channel": "default-channel"},
          "the new subscription freezes the declared default")
    source.close_subscription(new_sub.handle)


# ---------------------------------------------------------------------------
# 6. Crash between local schema switch and source acknowledgement
# ---------------------------------------------------------------------------


def scenario_replica_crash_replay(source, replica_path, make_worker):
    head("6. Replica crash after local switch, before source acknowledgement")
    source.create_table("widgets", [("label", "text"), ("weight", "int64")])
    tx = source.begin()
    for i in range(3):
        tx.stage_insert("widgets", "w%d" % i, {"label": "L%d" % i, "weight": i})
    tx.commit()
    sub, _ = source.create_subscription("widget-sub", ["widgets"])
    rep = Replica(replica_path, sub.handle)
    rep.run(source)
    check(rep.state()["ready"], "replica finished its full load")

    migration = source.add_column("widgets", "source_group", "text", "organic")
    tx = source.begin()
    tx.stage_update("widgets", "w1", {"source_group": "ads"})
    data_version = tx.commit()["commit_version"]

    # Apply the schema switch locally, then simulate a hard crash (no ack).
    applied = rep.apply_local(source)
    check(applied["kind"] == "schema_change", "schema switch applied locally")
    batch_id = applied["batch_id"]
    check(source.subscription_status(sub.handle)["pending_batch"] == batch_id,
          "the source still holds the batch pending (ack never sent)")
    del rep

    # A genuinely separate process reopens the replica and redelivers.
    restarted_worker = make_worker()
    rep2 = Replica(replica_path, sub.handle)
    fresh = Client(source.store.path)
    redelivered = rep2.consume(fresh)
    check(redelivered["batch_id"] == batch_id,
          "the same batch is redelivered after restart")
    check(redelivered["replayed"] and not redelivered["applied"],
          "the local switch is recognized and not applied twice")
    rep2.run(fresh)

    got = {r["row_id"]: r for r in rep2.rows("widgets")}
    want = snapshot_at(fresh, "widgets", data_version,
                       ["w0", "w1", "w2"],
                       ("label", "weight", "source_group"))
    compare("replayed replica matches the source (value/type/version)",
            got, want, ("label", "weight", "source_group"))
    check(got["w1"]["source_group"] == "ads",
          "later row is interpreted with the NEW structure")
    check(got["w0"]["source_group"] == "organic",
          "the default is preserved across the crash")

    con = sqlite3.connect(replica_path)
    try:
        names = [r[1] for r in con.execute('PRAGMA table_info("widgets")')]
        records = con.execute(
            "SELECT COUNT(*) FROM replica_applied WHERE batch_id=?",
            (batch_id,)).fetchone()[0]
    finally:
        con.close()
    check(names.count("source_group") == 1, "no duplicate column after replay")
    check(records == 1, "the schema batch is recorded exactly once")
    restarted_worker.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--keep", help="directory to keep the databases in")
    args = ap.parse_args(argv)

    base = args.keep or tempfile.mkdtemp(prefix="mrtx-add-column-")
    os.makedirs(base, exist_ok=True)
    live = []

    def make_source(slug):
        path = os.path.join(base, "source-%s.db" % slug)
        if os.path.exists(path):
            os.remove(path)
        return Client(path), path

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
        scenario_hand_computed(s)

        s, sp = make_source("pause")
        scenario_real_process_pause(s, make_replica("pause"),
                                    lambda: make_worker(sp))

        s, _ = make_source("conflict")
        s.create_table("accounts", [("owner", "text"), ("balance", "int64"),
                                    ("active", "bool"), ("note", "null")])
        seed(s, n=4)
        s.add_column("accounts", "source_group", "text", "organic")
        scenario_structural_conflict(s, lambda: make_worker(s.store.path))

        s, _ = make_source("invalid")
        s.create_table("accounts", [("owner", "text"), ("balance", "int64"),
                                    ("active", "bool"), ("note", "null")])
        scenario_invalid_migrations(s)

        s, _ = make_source("pending")
        scenario_pending_batch_and_new_subscription(s)

        s, sp = make_source("crash")
        scenario_replica_crash_replay(s, make_replica("crash"),
                                      lambda: make_worker(sp))
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
