#!/usr/bin/env python3
"""Runnable end-to-end drill for managed reference constraints.

It exercises the whole feature against the public API only, in two halves:

* single-process, hand-computed scenarios -- child-before-parent loading,
  re-parenting, delete-and-reinsert, a cascade chain, a self/multi-table ring,
  diamond de-duplication, and the two restrict outcomes (surviving referrer
  versus a referrer removed in the same batch).  Every expected delete set is
  given as an explicit literal or computed by the tiny independent
  ``hand_reach`` graph routine below, which never imports the product's
  cascade planner;
* real OS-process scenarios driven through barriers -- a parent delete racing
  a child insert and a cascade racing a child update in both commit orders,
  two unrelated groups committing together, an old write transaction meeting a
  newly registered constraint, and crash/replay seams for both a resent source
  commit and an unacknowledged replica-local commit.

    python3 -m mrtx.reference_drill             # temp dir, removed afterwards
    python3 -m mrtx.reference_drill --keep DIR  # keep every database

Exit status is 0 only if every expectation holds.
"""

import argparse
import os
import shutil
import sys
import tempfile
import threading

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from mrtx import Client, ConflictError, Replica, SchemaError  # noqa: E402
from mrtx.engine import TransactionHandle  # noqa: E402

# The real-process harness is test infrastructure, not product code.
sys.path.insert(0, os.path.join(ROOT, "tests"))
from procutil import Worker  # noqa: E402

FAILURES = []
LIVE = []


def check(condition, message):
    print(("  ok   " if condition else "  FAIL ") + message)
    if not condition:
        FAILURES.append(message)


def head(title):
    print("\n=== %s ===" % title)


def hand_reach(adjacency, roots):
    """Independent worklist reachability used solely to state expectations.

    ``adjacency`` maps an identity key to the child identity keys its
    *cascade* edges reach.  The visited set is the de-duplicated fixpoint;
    this is textbook graph reachability, deliberately separate from the
    product code under test.
    """
    seen = set(roots)
    pending = list(roots)
    while pending:
        cur = pending.pop(0)
        for nxt in adjacency.get(cur, ()):
            if nxt not in seen:
                seen.add(nxt)
                pending.append(nxt)
    return seen


def make_client(path):
    if os.path.exists(path):
        os.remove(path)
    return Client(path)


def worker(path):
    w = Worker(path)
    LIVE.append(w)
    return w


# ---------------------------------------------------------------------------
# Hand-computed single-process scenarios
# ---------------------------------------------------------------------------


def scenario_hand_computed(source):
    head("1. Hand-computed reference semantics (expected sets are literals)")
    source.create_table("accounts", [("owner", "text")])
    source.create_table("entries", [("acct_id", "text"), ("memo", "text")])
    source.create_table("lines", [("entry_id", "text"), ("kind", "text")])
    source.register_reference("entry_acct", "accounts", "entries",
                              "acct_id", "cascade")
    source.register_reference("line_entry", "entries", "lines",
                              "entry_id", "cascade")

    # 1a. Child staged before its parent inside one transaction.
    tx = source.begin()
    tx.stage_insert("entries", "E1", {"acct_id": "A1", "memo": "child first"})
    tx.stage_insert("accounts", "A1", {"owner": "ada"})
    tx.stage_insert("lines", "L1", {"entry_id": "E1", "kind": "fee"})
    result = tx.commit()
    check(result["status"] == "committed" and result["cascade_deletes"] == [],
          "child-before-parent load commits with no cascade")

    # 1b. Re-parent an entry to a new account; deleting the old account must
    # not remove the moved entry.
    tx = source.begin()
    tx.stage_insert("accounts", "A2", {"owner": "bo"})
    tx.stage_insert("entries", "E2", {"acct_id": "A2", "memo": "b"})
    tx.commit()
    tx = source.begin()
    tx.stage_update("entries", "E1", {"acct_id": "A2"})
    tx.stage_delete("accounts", "A1")
    result = tx.commit()
    check(result["cascade_deletes"] == [],
          "re-parented entry survives the old account's deletion")
    check({r["row_id"] for r in source.rows("entries")} == {"E1", "E2"},
          "both entries still present after re-parenting")

    # 1c. Delete an account and re-insert the same row_id in one transaction:
    # the parent exists at the end, so nothing cascades.
    tx = source.begin()
    tx.stage_delete("accounts", "A2")
    tx.stage_insert("accounts", "A2", {"owner": "bo-reborn"})
    result = tx.commit()
    check(result["cascade_deletes"] == [],
          "delete-then-reinsert of the same row_id triggers no cascade")
    check({r["row_id"] for r in source.rows("entries")} == {"E1", "E2"},
          "entries untouched by re-inserted parent")

    # 1d. Cascade chain accounts -> entries -> lines, with a literal set.
    tx = source.begin()
    tx.stage_insert("accounts", "A3", {"owner": "cy"})
    tx.stage_insert("entries", "E3", {"acct_id": "A3", "memo": "c"})
    tx.stage_insert("lines", "L3", {"entry_id": "E3", "kind": "tax"})
    tx.commit()
    result = source.begin()
    result.stage_delete("accounts", "A3")
    committed = result.commit()
    got = {(x["table"], x["row_id"]) for x in committed["cascade_deletes"]}
    # Literal expectation, derived by hand from the chain A3 -> E3 -> L3.
    check(got == {("entries", "E3"), ("lines", "L3")},
          "cascade chain delete set equals the hand literal %r" % got)
    depths = {(x["table"], x["row_id"]): x["depth"]
              for x in committed["cascade_deletes"]}
    check(depths == {("entries", "E3"): 1, ("lines", "L3"): 2},
          "propagation depth increases one per hop")

    # 1e. A self-referencing ring must terminate and delete each id once.
    source.create_table("nodes", [("ref", "text"), ("tag", "text")])
    source.register_reference("node_self", "nodes", "nodes", "ref", "cascade")
    tx = source.begin()
    tx.stage_insert("nodes", "n1", {"ref": "n2", "tag": "ring"})
    tx.stage_insert("nodes", "n2", {"ref": "n1", "tag": "ring"})
    tx.stage_insert("nodes", "n3", {"ref": "n2", "tag": "leaf"})
    tx.commit()
    committed = source.begin()
    committed.stage_delete("nodes", "n1")
    result = committed.commit()
    got = {(x["table"], x["row_id"]) for x in result["cascade_deletes"]}
    want = hand_reach(
        {("nodes", "n1"): [("nodes", "n2")],
         ("nodes", "n2"): [("nodes", "n1"), ("nodes", "n3")],
         ("nodes", "n3"): []},
        {("nodes", "n1")}) - {("nodes", "n1")}
    check(got == want and got == {("nodes", "n2"), ("nodes", "n3")},
          "self ring terminates; independent reachability gives %r" % got)
    check(len(got) == len(result["cascade_deletes"]),
          "an identity reached around the ring is deleted exactly once")

    # 1f. Diamond: one leaf reached through two middle rows, deleted once.
    source.create_table("root", [("v", "text")])
    source.create_table("mid", [("root_id", "text")])
    source.create_table("leaf", [("m1", "text"), ("m2", "text")])
    source.register_reference("rm", "root", "mid", "root_id", "cascade")
    source.register_reference("lm1", "mid", "leaf", "m1", "cascade")
    source.register_reference("lm2", "mid", "leaf", "m2", "cascade")
    tx = source.begin()
    tx.stage_insert("root", "R", {"v": "1"})
    tx.stage_insert("mid", "M1", {"root_id": "R"})
    tx.stage_insert("mid", "M2", {"root_id": "R"})
    tx.stage_insert("leaf", "L", {"m1": "M1", "m2": "M2"})
    tx.commit()
    tx = source.begin()
    tx.stage_delete("root", "R")
    result = tx.commit()
    got = [(x["table"], x["row_id"]) for x in result["cascade_deletes"]]
    want = hand_reach(
        {("root", "R"): [("mid", "M1"), ("mid", "M2")],
         ("mid", "M1"): [("leaf", "L")],
         ("mid", "M2"): [("leaf", "L")],
         ("leaf", "L"): []},
        {("root", "R")}) - {("root", "R")}
    check(set(got) == want and len(got) == 3,
          "diamond deletes M1, M2 and L once each (independent solver)")


def scenario_restrict(source):
    head("2. Restrict: surviving referrer vs same-batch removed referrer")
    source.create_table("parents", [("v", "text")])
    source.create_table("kids", [("p_id", "text")])
    source.register_reference("kr", "parents", "kids", "p_id", "restrict")

    tx = source.begin()
    tx.stage_insert("parents", "P1", {"v": "1"})
    tx.stage_insert("kids", "K1", {"p_id": "P1"})
    tx.commit()
    tx = source.begin()
    tx.stage_delete("parents", "P1")
    try:
        tx.commit()
        check(False, "surviving restrict referrer must block the delete")
    except ConflictError as exc:
        p = exc.evidence["conflicts"][0]
        check(exc.code == "restrict_violation"
              and p["row_id"] == "K1" and p["missing_parent"] == "P1",
              "restrict refusal names constraint, child and missing parent")
    check({r["row_id"] for r in source.rows("kids")} == {"K1"},
          "restrict never silently deletes the child")

    # Same graph, but the referrer is itself deleted in the same transaction:
    # the restrict edge points at a non-surviving row and must not fire.
    tx = source.begin()
    tx.stage_delete("kids", "K1")
    tx.stage_delete("parents", "P1")
    result = tx.commit()
    check(result["status"] == "committed" and result["cascade_deletes"] == [],
          "deleting the restrict referrer in the same batch is allowed")
    check(source.rows("parents") == [] and source.rows("kids") == [],
          "both rows are gone after the same-batch delete")

    # Mixed graph proving restrict is evaluated only after the cascade set is
    # fixed: a restrict referrer removed via a parallel cascade path must not
    # block the root delete.
    source.create_table("r2", [("v", "text")])
    source.create_table("m2t", [("r_id", "text")])
    source.create_table("l2", [("restrict_id", "text"),
                               ("cascade_id", "text")])
    source.register_reference("r2m", "r2", "m2t", "r_id", "cascade")
    source.register_reference("m2l_r", "m2t", "l2", "restrict_id", "restrict")
    source.register_reference("m2l_c", "m2t", "l2", "cascade_id", "cascade")
    tx = source.begin()
    tx.stage_insert("r2", "R", {"v": "1"})
    tx.stage_insert("m2t", "M", {"r_id": "R"})
    tx.stage_insert("l2", "L", {"restrict_id": "M", "cascade_id": "M"})
    tx.commit()
    tx = source.begin()
    tx.stage_delete("r2", "R")
    result = tx.commit()
    got = {(x["table"], x["row_id"]) for x in result["cascade_deletes"]}
    check(got == {("m2t", "M"), ("l2", "L")},
          "restrict edge skipped once its referrer is in the cascade set")


def scenario_registration_failures(source):
    head("3. Registration failures, existing orphans and old transactions")
    source.create_table("okp", [("v", "text")])
    source.create_table("okc", [("ref", "text"), ("num", "int64")])
    source.register_reference("good", "okp", "okc", "ref", "cascade")

    cases = [
        (dict(name="good", parent_table="okp", child_table="okc",
              child_column="ref", on_delete="cascade"), "duplicate name"),
        (dict(name="bad_table", parent_table="okp", child_table="ghost",
              child_column="ref", on_delete="cascade"), "unknown table"),
        (dict(name="bad_col", parent_table="okp", child_table="okc",
              child_column="nope", on_delete="cascade"), "unknown column"),
        (dict(name="bad_type", parent_table="okp", child_table="okc",
              child_column="num", on_delete="cascade"), "non-text column"),
        (dict(name="bad_strategy", parent_table="okp", child_table="okc",
              child_column="ref", on_delete="set-null"), "bad strategy"),
    ]
    for kwargs, label in cases:
        try:
            source.register_reference(**kwargs)
            check(False, label + " should have been rejected")
        except SchemaError:
            check(True, label + " rejected with no half definition")
    names = {r["constraint_name"] for r in source.list_references()}
    check("good" in names and "bad_table" not in names and "bad_col" not in names
          and "bad_type" not in names and "bad_strategy" not in names,
          "failed registrations added no durable definition")

    # An existing orphan blocks registration until the data is repaired.
    source.create_table("op", [("v", "text")])
    source.create_table("oc", [("p_id", "text")])
    tx = source.begin()
    tx.stage_insert("op", "P", {"v": "1"})
    tx.stage_insert("oc", "clean", {"p_id": "P"})
    tx.stage_insert("oc", "dirty", {"p_id": "MISSING"})
    tx.commit()
    try:
        source.register_reference("orph", "op", "oc", "p_id", "cascade")
        check(False, "registration over an existing orphan must fail")
    except SchemaError as exc:
        check("dirty" in str(exc), "orphan evidence names the offending row")
    check(source.get_reference("orph") is None, "failed registration left nothing")
    tx = source.begin()
    tx.stage_update("oc", "dirty", {"p_id": "P"})
    tx.commit()
    out = source.register_reference("orph", "op", "oc", "p_id", "cascade")
    check(out["name"] == "orph", "registration succeeds once the orphan is fixed")

    # An old read-only transaction keeps its historical view; an old write
    # transaction is checked by the newly registered rule.
    source.create_table("np", [("v", "text")])
    source.create_table("nc", [("p_id", "text")])
    reader = source.begin()
    reader.read("np")
    writer = source.begin()
    writer.stage_insert("nc", "w", {"p_id": "GONE"})
    source.register_reference("late", "np", "nc", "p_id", "cascade")
    check(reader.commit()["status"] == "committed",
          "old read-only transaction still completes under its start view")
    try:
        writer.commit()
        check(False, "old write transaction must meet the new constraint")
    except ConflictError as exc:
        check(exc.code == "reference_violation",
              "old write transaction is rejected by the new rule")


# ---------------------------------------------------------------------------
# Real-process concurrency scenarios
# ---------------------------------------------------------------------------


def scenario_process_races(path):
    head("4. Real processes, barrier-ordered: delete/insert and cascade/update")
    c = make_client(path + "-race.db")
    c.create_table("parents", [("name", "text")])
    c.create_table("children", [("p_id", "text"), ("note", "text")])
    c.register_reference("cr", "parents", "children", "p_id", "cascade")

    def seed(pid, cid, note="seed"):
        tx = c.begin()
        tx.stage_insert("parents", pid, {"name": pid})
        tx.stage_insert("children", cid, {"p_id": pid, "note": note})
        tx.commit()

    def run_ordered(tag, first_action, second_action):
        """Drive two worker transactions with a deterministic commit order.

        Both workers stage first and meet a three-party barrier with the
        orchestrating thread; an explicit gate event then releases exactly the
        designated first committer while the other waits, so both real-process
        interleavings are reproducible rather than left to scheduler luck.
        """
        staged = threading.Barrier(3)
        gate_first = threading.Event()
        gate_second = threading.Event()
        results = {}

        def first_worker():
            first_action["stage"]()
            staged.wait()
            gate_first.wait()
            results["first"] = first_action["commit"]()
            gate_second.set()

        def second_worker():
            second_action["stage"]()
            staged.wait()
            gate_second.wait()
            results["second"] = second_action["commit"]()

        t1, t2 = threading.Thread(target=first_worker), threading.Thread(
            target=second_worker)
        t1.start()
        t2.start()
        # Main thread is the third barrier participant: once all three return,
        # both workers have finished staging and are parked on their gates.
        staged.wait()
        gate_first.set()
        t1.join()
        t2.join()
        live_parents = {r["row_id"] for r in c.rows("parents")}
        orphans = [r["row_id"] for r in c.rows("children")
                   if r["p_id"] and r["p_id"] not in live_parents]
        check(orphans == [], tag + " leaves no orphan")
        return results

    def code(out):
        return "ok" if out["ok"] else out["evidence"]["code"]

    # --- parent delete vs concurrent child insert, both commit orders -------
    # Order A: delete commits first; the later insert references a gone parent.
    pid, cid = "DA", "CA"
    seed(pid, cid)
    wd, wi = worker(c.db_path), worker(c.db_path)
    td, ti = wd.begin(), wi.begin()
    res = run_ordered(
        "delete-first (cascade)",
        {"stage": lambda: wd.delete(td, "parents", pid),
         "commit": lambda: wd.commit(td)},
        {"stage": lambda: wi.insert(ti, "children", cid + "-new",
                                    {"p_id": pid, "note": "new"}),
         "commit": lambda: wi.commit(ti)})
    check(code(res["first"]) == "ok"
          and code(res["second"]) == "reference_violation",
          "delete-first: parent delete commits, child insert loses "
          "reference_violation")

    # Order B: insert commits first; the cascade would overwrite a row a
    # concurrent transaction just wrote, so it loses with cascade_conflict.
    pid, cid = "DB", "CB"
    seed(pid, cid)
    wd, wi = worker(c.db_path), worker(c.db_path)
    td, ti = wd.begin(), wi.begin()
    res = run_ordered(
        "insert-first (cascade)",
        {"stage": lambda: wi.insert(ti, "children", cid + "-new",
                                    {"p_id": pid, "note": "new"}),
         "commit": lambda: wi.commit(ti)},
        {"stage": lambda: wd.delete(td, "parents", pid),
         "commit": lambda: wd.commit(td)})
    check(code(res["first"]) == "ok"
          and code(res["second"]) == "cascade_conflict",
          "insert-first: child insert commits, cascade loses "
          "cascade_conflict")
    check(not (res["first"]["ok"] and res["second"]["ok"]),
          "the two conflicting commits never both succeed")

    # --- cascade delete vs concurrent update of the targeted child ----------
    # Order A: the cascade lands first; the stale child update is a ww loss.
    pid, cid = "UA", "UCA"
    seed(pid, cid)
    wd, wu = worker(c.db_path), worker(c.db_path)
    td, tu = wd.begin(), wu.begin()
    res = run_ordered(
        "cascade-first vs update",
        {"stage": lambda: wd.delete(td, "parents", pid),
         "commit": lambda: wd.commit(td)},
        {"stage": lambda: wu.update(tu, "children", cid, {"note": "changed"}),
         "commit": lambda: wu.commit(tu)})
    check(code(res["first"]) == "ok"
          and code(res["second"]) == "write_conflict",
          "cascade-first: child update loses as write_conflict")

    # Order B: the update lands first; the cascade refuses to overwrite it.
    pid, cid = "UB", "UCB"
    seed(pid, cid)
    wd, wu = worker(c.db_path), worker(c.db_path)
    td, tu = wd.begin(), wu.begin()
    res = run_ordered(
        "update-first vs cascade",
        {"stage": lambda: wu.update(tu, "children", cid, {"note": "changed"}),
         "commit": lambda: wu.commit(tu)},
        {"stage": lambda: wd.delete(td, "parents", pid),
         "commit": lambda: wd.commit(td)})
    check(code(res["first"]) == "ok"
          and code(res["second"]) == "cascade_conflict",
          "update-first: cascade refuses with cascade_conflict")
    surviving = next((r for r in c.rows("children") if r["row_id"] == cid), None)
    check(surviving is not None and surviving["note"] == "changed",
          "the surviving child keeps the concurrent update")

    # --- two unrelated groups commit concurrently and both succeed ----------
    w1, w2 = worker(c.db_path), worker(c.db_path)
    t1, t2 = w1.begin(), w2.begin()
    barrier = threading.Barrier(2)
    results = []

    def group_a():
        w1.insert(t1, "parents", "GA", {"name": "GA"})
        w1.insert(t1, "children", "GA-c", {"p_id": "GA", "note": "a"})
        barrier.wait()
        results.append(("A", w1.commit(t1)))

    def group_b():
        w2.insert(t2, "parents", "GB", {"name": "GB"})
        w2.insert(t2, "children", "GB-c", {"p_id": "GB", "note": "b"})
        barrier.wait()
        results.append(("B", w2.commit(t2)))

    t1h, t2h = threading.Thread(target=group_a), threading.Thread(target=group_b)
    t1h.start()
    t2h.start()
    t1h.join()
    t2h.join()
    check(all(out["ok"] for _, out in results),
          "two unrelated parent/child groups both commit without replay")


def scenario_replica_and_replay(base):
    head("5. Replica mirror, single source position, and crash/replay seams")
    source_path = os.path.join(base, "source.db")
    replica_path = os.path.join(base, "replica.db")
    source = make_client(source_path)
    source.create_table("accounts", [("owner", "text")])
    source.create_table("entries", [("acct_id", "text"), ("memo", "text")])
    source.register_reference("e_acct", "accounts", "entries",
                              "acct_id", "cascade")
    tx = source.begin()
    tx.stage_insert("accounts", "A1", {"owner": "ada"})
    tx.stage_insert("accounts", "A2", {"owner": "bo"})
    tx.stage_insert("entries", "E1", {"acct_id": "A1", "memo": "one"})
    tx.stage_insert("entries", "E2", {"acct_id": "A2", "memo": "two"})
    tx.commit()

    sub, cut = source.create_subscription("mirror", ["accounts", "entries"])
    rep = Replica(replica_path, sub.handle)
    rep.run(source)
    before_source = {r["row_id"]: r for r in source.rows("entries")}
    before_replica = {r["row_id"]: r for r in rep.rows("entries")}
    check(set(before_source) == set(before_replica) == {"E1", "E2"},
          "full-load mirror matches the source before any cascade")
    for rid in before_source:
        gs, gr = before_source[rid], before_replica[rid]
        check(gs["version"] == gr["version"]
              and type(gs["acct_id"]) is type(gr["acct_id"]) is str
              and type(gs["memo"]) is type(gr["memo"]) is str,
              "mirror row %s matches type and version" % rid)

    # One cross-table cascade commit.
    tx = source.begin()
    tx.stage_delete("accounts", "A1")
    outcome = tx.commit()
    position = outcome["commit_version"]
    check(len({w["commit_version"] for w in outcome["writes"]}
               | {x["commit_version"] for x in outcome["cascade_deletes"]}) == 1,
          "explicit and cascade deletes share one source position %d" % position)
    check([(x["table"], x["row_id"], x["propagation_source"])
           for x in outcome["cascade_deletes"]]
          == [("entries", "E1", ["accounts", "A1"])],
          "public result distinguishes the cascade delete and its source")

    # The consumer sees the whole net change as one batch, applies it locally
    # in one replica transaction, then acknowledges.
    applied = rep.apply_local(source)
    check(applied["position"] == position and applied["row_count"] == 2,
          "one incremental batch carries the parent and the child delete")
    batch = source.pull(sub.handle)["batch"]
    items = {(i["table"], i["row_id"]): i for i in batch["items"]}
    entry_change = items[("entries", "E1")]
    check(entry_change["before"]["present"] and not entry_change["after"]["present"]
          and entry_change["before"]["values"]["memo"] == "one"
          and entry_change["before"]["version"] < position,
          "before/after mirror shows the true prior image then absence")
    rep.acknowledge(source)
    check([r["row_id"] for r in rep.rows("entries")] == ["E2"],
          "replica applies the cascade and retains the untouched entry")
    check(source.pull(sub.handle)["caught_up"], "no further pending batch")

    # Source commit resend from a new process after the worker exits.
    w = worker(source_path)
    txid = w.begin()
    w.delete(txid, "accounts", "A2")
    first = w.commit(txid)
    check(first["ok"], "second cascade commits in a worker process")
    saved_handle = w.handles[txid]
    saved_position = first["result"]["commit_version"]
    w.close()
    LIVE.remove(w)
    replayed = Client(source_path).commit(
        TransactionHandle.from_dict(saved_handle))
    check(replayed["replayed"]
          and replayed["commit_version"] == saved_position,
          "resent source commit returns the same durable decision, no new position")
    check(source.current_version() == saved_position,
          "replay does not advance the source position or delete twice")

    # Replica-local commit left unacknowledged: reopening re-delivers the same
    # batch, which the replica recognizes instead of applying twice.
    source.create_table("notes", [("text", "text")])
    # Rebuild not needed: create a fresh subscription over all three tables.
    sub2, _ = source.create_subscription("mirror2", ["accounts", "entries", "notes"])
    rep2_path = os.path.join(base, "replica2.db")
    rep2 = Replica(rep2_path, sub2.handle)
    rep2.run(source)
    tx = source.begin()
    tx.stage_insert("notes", "n1", {"text": "hello"})
    note_commit = tx.commit()
    applied = rep2.apply_local(source)
    check(applied["position"] == note_commit["commit_version"],
          "replica applies the note batch locally")
    del rep2
    pending = source.pull(sub2.handle)["batch"]
    reopened = Replica(rep2_path, sub2.handle)
    redelivery = reopened.apply_local(source)
    check(redelivery["replayed"] and redelivery["batch_id"] == applied["batch_id"],
          "unacknowledged local commit is re-delivered identically and skipped")
    reopened.acknowledge(source)
    check(reopened.rows("notes")[0]["text"] == "hello",
          "the redelivered data is preserved, not duplicated")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--keep", help="directory to keep the generated databases in")
    args = ap.parse_args(argv)
    base = args.keep or tempfile.mkdtemp(prefix="mrtx-reference-drill-")
    os.makedirs(base, exist_ok=True)
    try:
        hand = make_client(os.path.join(base, "hand.db"))
        scenario_hand_computed(hand)
        scenario_restrict(hand)
        scenario_registration_failures(hand)
        scenario_process_races(os.path.join(base, "proc"))
        scenario_replica_and_replay(base)
    finally:
        for w in list(LIVE):
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
