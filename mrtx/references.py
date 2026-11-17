"""Managed reference constraints and commit-time cascade planning.

A *reference constraint* declares that a ``text`` column of a child table
references the implicit ``row_id`` of a parent table (the two tables may be the
same one).  Business ``None`` means "no reference"; every other string must
match exactly one *live* parent row, compared by Unicode code point with no
normalization.  Two delete strategies exist:

``restrict``
    A commit that would leave a surviving child pointing at a parent identity
    the same commit deletes is refused.

``cascade``
    The commit also deletes the still-pointing children, then propagates from
    those identities, transitively, with set semantics so an identity reached
    by many paths is deleted exactly once.

Reference checking is deferred to ``commit`` and is evaluated against the
*complete business state the commit would produce*, never against the order of
the staged operations: a child may be staged before its parent, an orphan may
exist transiently and be repaired, and a parent may be deleted and re-inserted
with the same ``row_id``.  Nothing here uses SQLite's native foreign-key switch
(the product manages logical, append-only, versioned history itself).

Concurrency
===========
Registration runs in one ``BEGIN IMMEDIATE`` short transaction.  Because it
holds the write lock while it inspects the live committed state, a write
transaction that began *before* registration and commits *after* it is
validated by the same lock at its own commit: the planner below runs inside the
committer's atomic transaction against the then-current state, so such a
transaction can never slip an orphan past the new rule.  A read-only
transaction that began earlier still completes -- it serializes at its start
view and never revalidates constraints.

The planner is deliberately pure with respect to its inputs (the live rows, the
staged effective changes and the durable constraint catalog), so a re-sent
``commit`` reconstructs exactly the same decision and the same cascade set.
"""

import time

from .errors import ReferenceError
from .store import Store, hist_table
from .values import TYPE_TEXT

RESTRICT = "restrict"
CASCADE = "cascade"
ON_DELETE = frozenset((RESTRICT, CASCADE))

# Rejection evidence codes (raised as ConflictError by the engine).
REF_VIOLATION = "reference_violation"
RESTRICT_VIOLATION = "restrict_violation"
CASCADE_CONFLICT = "cascade_conflict"


# ---------------------------------------------------------------------------
# Catalog access
# ---------------------------------------------------------------------------


def list_constraints(conn):
    """Every durable constraint as plain dicts, ordered by name for stability."""
    rows = conn.execute(
        "SELECT constraint_name, parent_table, child_table, child_column, on_delete, "
        "registered_version, created_wall FROM reference_constraints "
        "ORDER BY constraint_name").fetchall()
    return [dict(r) for r in rows]


def live_row_ids(conn, table):
    """The set of ``row_id`` values present in the latest committed state."""
    hist = hist_table(table)
    rows = conn.execute(
        'SELECT h.row_id FROM "%s" h JOIN (SELECT row_id, MAX(version) AS mv FROM "%s" '
        'GROUP BY row_id) v ON h.row_id = v.row_id AND h.version = v.mv '
        'WHERE h.deleted = 0' % (hist, hist)).fetchall()
    return {r[0] for r in rows}


def _latest_live_images(conn, table):
    """``{row_id: values}`` for every currently live committed row of *table*."""
    hist = hist_table(table)
    rows = conn.execute(
        'SELECT h.* FROM "%s" h JOIN (SELECT row_id, MAX(version) AS mv FROM "%s" '
        'GROUP BY row_id) v ON h.row_id = v.row_id AND h.version = v.mv '
        'WHERE h.deleted = 0' % (hist, hist)).fetchall()
    out = {}
    for r in rows:
        values = {k: r[k] for k in r.keys() if k not in ("row_id", "version", "deleted")}
        out[r["row_id"]] = values
    return out


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def register(client, name, parent_table, child_table, child_column, on_delete):
    """Validate against existing committed data, then persist one constraint.

    The whole check-and-insert is one short write transaction, so a refusal
    leaves no half definition and a concurrent committer cannot land an orphan
    between the scan and the durable insert.  Raises :class:`ReferenceError` for
    every malformed or unsupported declaration.
    """
    if not isinstance(name, str) or not name.strip():
        raise ReferenceError("constraint name must be a non-empty string")
    client.store.check_ident(name, "constraint name")
    client.store.check_ident(parent_table, "parent table")
    client.store.check_ident(child_table, "child table")
    client.store.check_ident(child_column, "child column")
    if on_delete not in ON_DELETE:
        raise ReferenceError("on_delete must be %r or %r, got %r"
                          % (RESTRICT, CASCADE, on_delete))

    # Validate the shape before opening the write transaction so an obviously
    # bad declaration has no side effects at all.
    parent_types = client.store.column_types()
    if parent_table not in parent_types:
        raise ReferenceError("unknown parent table %r" % (parent_table,))
    if child_table not in parent_types:
        raise ReferenceError("unknown child table %r" % (child_table,))
    child_types = parent_types[child_table]
    if child_column not in child_types:
        raise ReferenceError("child table %r has no column %r"
                          % (child_table, child_column))
    if child_types[child_column] != TYPE_TEXT:
        raise ReferenceError(
            "reference column %s.%s must be declared text, found %r"
            % (child_table, child_column, child_types[child_column]))

    def _register(conn):
        if conn.execute("SELECT 1 FROM reference_constraints WHERE constraint_name=?",
                        (name,)).fetchone() is not None:
            raise ReferenceError("a reference constraint named %r already exists" % name)
        # Re-check the catalog inside the lock: a table cannot be dropped in
        # this product, but the duplicate (table, column) rule is enforced here
        # against the authoritative durable rows.
        dup = conn.execute(
            "SELECT constraint_name FROM reference_constraints WHERE child_table=? "
            "AND child_column=?", (child_table, child_column)).fetchone()
        if dup is not None:
            raise ReferenceError(
                "column %s.%s already carries reference constraint %r; one managed "
                "reference per column is supported"
                % (child_table, child_column, dup["constraint_name"]))
        # Existing committed data must already satisfy the rule.  This is a
        # full live scan of the child table; the write lock held here means no
        # concurrent commit can insert an orphan that the scan would miss.
        images = _latest_live_images(conn, child_table)
        parents = live_row_ids(conn, parent_table)
        orphans = []
        for rid in sorted(images):
            ref = images[rid].get(child_column)
            if ref is None:
                continue
            if ref not in parents:
                orphans.append({"table": child_table, "row_id": rid,
                                "column": child_column, "missing_parent": ref})
        if orphans:
            raise ReferenceError(
                "cannot register constraint %r: %d existing row(s) reference a missing "
                "parent; first orphan: %s/%s -> %r"
                % (name, len(orphans), child_table, orphans[0]["row_id"],
                   orphans[0]["missing_parent"]))
        registered_version = Store.current_commit_version(conn)
        conn.execute(
            "INSERT INTO reference_constraints(constraint_name, parent_table, child_table, "
            "child_column, on_delete, registered_version, created_wall) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (name, parent_table, child_table, child_column, on_delete,
             registered_version, time.time()))
        return {"name": name, "parent_table": parent_table,
                "child_table": child_table, "child_column": child_column,
                "on_delete": on_delete, "registered_version": registered_version}

    return client.store.run_tx(_register)


def list_registered(client):
    def _q(conn):
        return list_constraints(conn)
    return client.store.run_tx(_q, readonly=True)


def get_constraint(client, name):
    def _q(conn):
        row = conn.execute(
            "SELECT constraint_name, parent_table, child_table, child_column, on_delete, "
            "registered_version, created_wall FROM reference_constraints "
            "WHERE constraint_name=?", (name,)).fetchone()
        return dict(row) if row is not None else None
    return client.store.run_tx(_q, readonly=True)


# ---------------------------------------------------------------------------
# Commit-time planning
# ---------------------------------------------------------------------------


def final_views(conn, engine, txid, tables):
    """The post-commit business state this transaction would produce.

    For each table in *tables*: the newest committed live images (at the
    current version, so concurrent commits that landed after this transaction
    began are visible to the precondition checks) overlaid with the
    transaction's own folded staged changes (insert/update kept, delete
    removed).  This is the same "start view plus own staging" semantics a
    ``read`` uses, materialized against the current committed versions so the
    planner sees concurrent commits.  Returns ``{table: {row_id: values}}``
    (live rows only).
    """
    views = {}
    for table in tables:
        view = {}
        for rid, _ver, vals in engine._visible_rows(conn, table,
                                                    Store.current_commit_version(conn)):
            view[rid] = vals
        for rid, eff in engine._staged_for_table(conn, txid, table):
            if eff["op"] == "delete":
                view.pop(rid, None)
            else:
                view[rid] = eff["new_values"]
        views[table] = view
    return views


def plan_commit(conn, engine, txid, base_version, staged_tables):
    """Compute the reference outcome of one committing transaction.

    Returns ``(problems, cascade)`` where

    * ``problems`` is a list of evidence-carrying conflict records (empty when
      the commit is reference-valid); the engine appends them to its ordinary
      OCC problems and rejects the whole transaction atomically;
    * ``cascade`` is the ordered, de-duplicated list of implicit deletes as
      ``{"table", "row_id", "constraint", "parent_table", "parent_id",
      "depth", "old_values", "old_version", "ephemeral"}`` records, each
      carrying the propagation source that explains it.

    The evaluation order is fixed: build the complete final state, find the
    explicit deleted parent identities, expand the full transitive cascade set
    with worklist/set semantics, and only afterwards check ``restrict`` and
    generic reference validity.  A ``restrict`` edge whose referrer is itself
    deleted (explicitly or by the cascade) therefore never blocks the commit.
    """
    all_constraints = list_constraints(conn)
    if not all_constraints:
        return [], []

    # Every table whose final state this commit can change, plus every table
    # reachable from them through a constraint, so a cascade crossing into a
    # table the transaction never explicitly staged still has a materialized
    # view to read and delete from.
    needed = set(staged_tables)
    changed = True
    while changed:
        changed = False
        for con in all_constraints:
            if con["child_table"] in needed and con["parent_table"] not in needed:
                needed.add(con["parent_table"])
                changed = True
            if con["parent_table"] in needed and con["child_table"] not in needed:
                needed.add(con["child_table"])
                changed = True
    ordered_tables = sorted(needed)
    views = final_views(conn, engine, txid, ordered_tables)

    # Folded staged changes, keyed for quick lookup.
    staged = {}
    for table in staged_tables:
        staged[table] = {rid: eff for rid, eff in
                         engine._staged_for_table(conn, txid, table)}

    # (1) Roots: identities explicitly deleted by this transaction that do not
    # survive in the final state.  A delete followed by a same-id re-insert is
    # not a root: the parent still exists at the end, so nothing may cascade
    # from it.  A parent inserted and deleted again inside this transaction is
    # still a root -- the rule is evaluated on the final state, where it is
    # absent; any child this transaction inserted against it becomes an
    # ephemeral cascade delete rather than an orphan rejection.
    roots = []
    for table in staged_tables:
        for rid, eff in staged[table].items():
            if eff["op"] != "delete":
                continue
            if rid in views[table]:
                continue  # re-inserted in this transaction
            roots.append({"table": table, "row_id": rid})

    # (2) Cascade expansion with set semantics.  ``deleted`` is the full
    # transitive delete set; ``records`` remembers one auditable propagation
    # source per identity (the first path that reaches it; diamond edges keep
    # the same single deletion).
    deleted = set()
    records = {}
    worklist = []
    for root in sorted(roots, key=lambda r: (r["table"], r["row_id"])):
        key = (root["table"], root["row_id"])
        if key in deleted:
            continue
        deleted.add(key)
        records[key] = {"table": root["table"], "row_id": root["row_id"],
                        "constraint": None, "parent_table": None,
                        "parent_id": None, "depth": 0}
        worklist.append(key)

    by_parent = {}
    for con in all_constraints:
        if con["on_delete"] == CASCADE:
            by_parent.setdefault(con["parent_table"], []).append(con)

    while worklist:
        p_table, p_id = worklist.pop(0)
        for con in by_parent.get(p_table, []):
            child_table, column = con["child_table"], con["child_column"]
            view = views.get(child_table, {})
            for c_id in sorted(view):
                if view[c_id].get(column) != p_id:
                    continue
                key = (child_table, c_id)
                if key in deleted:
                    continue
                deleted.add(key)
                depth = records[(p_table, p_id)]["depth"] + 1
                records[key] = {"table": child_table, "row_id": c_id,
                                "constraint": con["constraint_name"],
                                "parent_table": p_table, "parent_id": p_id,
                                "depth": depth}
                worklist.append(key)

    # (3) Materialize every implicit delete with its true prior image.  A
    # child that this transaction itself inserted and the cascade then removes
    # is "ephemeral": it never existed in the committed state, so the engine
    # must not fabricate a historical business row for it.
    cascade = []
    for key in sorted(deleted, key=lambda k: (records[k]["depth"], k[0], k[1])):
        table, rid = key
        rec = records[key]
        if rec["depth"] == 0:
            continue  # explicit user delete, already in the staged write set
        eff = staged.get(table, {}).get(rid)
        ephemeral = eff is not None and not eff["base_present"]
        old_values = None
        old_version = None
        if not ephemeral:
            hist = hist_table(table)
            row = conn.execute(
                'SELECT h.* FROM "%s" h JOIN (SELECT row_id, MAX(version) AS mv FROM "%s" '
                'WHERE row_id=? GROUP BY row_id) v ON h.row_id=v.row_id AND h.version=v.mv'
                % (hist, hist), (rid,)).fetchone()
            if row is not None and not row["deleted"]:
                old_values = {k: row[k] for k in row.keys()
                              if k not in ("row_id", "version", "deleted")}
                old_version = row["version"]
        cascade.append({"table": table, "row_id": rid,
                        "constraint": rec["constraint"],
                        "parent_table": rec["parent_table"],
                        "parent_id": rec["parent_id"], "depth": rec["depth"],
                        "old_values": old_values, "old_version": old_version,
                        "ephemeral": ephemeral})

    # (4) Conflict evidence for implicit deletes of rows a concurrent
    # transaction changed after this snapshot.  The cascade is a real write:
    # it participates in the same same-identity (ww) precondition rule as an
    # explicit delete, so a long transaction cannot silently remove a row that
    # appeared or changed after its start view.
    problems = []
    current = Store.current_commit_version(conn)
    for item in cascade:
        table, rid = item["table"], item["row_id"]
        for w in conn.execute(
                "SELECT user_txid, op, commit_version FROM committed_writes "
                "WHERE table_name=? AND row_id=? AND commit_version > ? "
                "ORDER BY commit_version", (table, rid, base_version)).fetchall():
            if w["user_txid"] == txid:
                continue
            problems.append({
                "code": CASCADE_CONFLICT, "dependency": "ww",
                "summary": ("cascade delete of %s/%s (via constraint %r from %s/%s) would "
                            "overwrite %s's concurrent commit %s"
                            % (table, rid, item["constraint"], item["parent_table"],
                               item["parent_id"], w["user_txid"], w["commit_version"])),
                "table": table, "row_id": rid,
                "constraint": item["constraint"],
                "parent_identity": [item["parent_table"], item["parent_id"]],
                "other_transaction": w["user_txid"],
                "other_commit_version": w["commit_version"],
                "snapshot_version": base_version, "current_version": current,
                "witness": {"identity": [table, rid],
                            "propagation_source": [item["parent_table"],
                                                   item["parent_id"]]}})

    # (5) Restrict: only a *surviving* referrer of a deleted identity blocks
    # the commit.  A referrer inside the cascade set (or explicitly deleted) is
    # already gone, so it is skipped rather than producing a premature refusal.
    for con in all_constraints:
        if con["on_delete"] != RESTRICT:
            continue
        child_table, column = con["child_table"], con["child_column"]
        view = views.get(child_table, {})
        for c_id in sorted(view):
            if (child_table, c_id) in deleted:
                continue
            ref = view[c_id].get(column)
            if ref is None:
                continue
            if (con["parent_table"], ref) in deleted:
                problems.append({
                    "code": RESTRICT_VIOLATION, "dependency": "fk",
                    "summary": ("constraint %r forbids deleting %s/%s: surviving row %s/%s "
                                "still references it"
                                % (con["constraint_name"], con["parent_table"], ref,
                                   child_table, c_id)),
                    "constraint": con["constraint_name"],
                    "on_delete": RESTRICT,
                    "table": child_table, "row_id": c_id, "column": column,
                    "parent_table": con["parent_table"], "missing_parent": ref,
                    "snapshot_version": base_version, "current_version": current,
                    "witness": {"identity": [child_table, c_id],
                                "missing_parent": [con["parent_table"], ref]}})

    # (6) Generic reference validity of the final state: every non-null text in
    # a referencing column must resolve to a live parent.  This covers orphans
    # a transaction creates outright (parent never existed) and any surviving
    # reference to a deleted identity on a *cascade* edge that the traversal
    # could not reach (it has no deleted root, so no cascade removed it).
    # Restrict edges are fully covered by step (5); checking them again here
    # would duplicate the same surviving-referrer evidence under another code.
    for con in all_constraints:
        if con["on_delete"] == RESTRICT:
            continue
        child_table, column = con["child_table"], con["child_column"]
        view = views.get(child_table, {})
        parent_ids = views.get(con["parent_table"], {})
        for c_id in sorted(view):
            if (child_table, c_id) in deleted:
                continue
            ref = view[c_id].get(column)
            if ref is None:
                continue
            if ref in parent_ids:
                continue
            problems.append({
                "code": REF_VIOLATION, "dependency": "fk",
                "summary": ("%s/%s.%s = %r does not reference any live %s row "
                            "(constraint %r)"
                            % (child_table, c_id, column, ref, con["parent_table"],
                               con["constraint_name"])),
                "constraint": con["constraint_name"],
                "on_delete": con["on_delete"],
                "table": child_table, "row_id": c_id, "column": column,
                "parent_table": con["parent_table"], "missing_parent": ref,
                "snapshot_version": base_version, "current_version": current,
                "witness": {"identity": [child_table, c_id],
                            "missing_parent": [con["parent_table"], ref]}})

    return problems, cascade
