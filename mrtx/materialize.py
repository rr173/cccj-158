"""Materialized join-aggregate summaries with paged builds and key-scoped upkeep.

A *materialized summary* is a named, read-only managed table that permanently
reflects a restricted declaration over two ordinary managed tables:

* an ``inner`` or ``left`` join on one ``text`` column of each side, matched by
  exact non-null text equality (nulls never match; no de-duplication -- an
  ``m x n`` fan-out really contributes ``m * n`` join rows);
* a non-empty list of typed group columns taken from either side;
* ``COUNT(*)``, ``COUNT(column)`` and ``SUM(int64 column)`` aggregates with
  caller-chosen output aliases.

There is no arbitrary SQL, no expression, no filter, no self join and no
stacking of one materialized table on another.

Build model
===========
``define`` only validates and persists the declaration.  ``start_build`` fixes
one source cut ``V`` and creates the on-disk build structures; the initial
scan and the catch-up of commits that landed while the build was running are
advanced one short transaction at a time by ``build_step`` with an explicit
row budget, so a real process can commit between pages, exit and resume in
another process.  Both pagination cursors are anchored at the same cut, so the
two source tables can never mix values from different source positions.  When
the maintained state reaches the current source position the output is
published and the definition becomes ``active`` at one explicit position.

Maintenance model
=================
After activation every source commit maintains every affected summary *inside
the commit's own atomic transaction*.  Only the join keys the commit changed
are touched: the partial aggregates of the affected keys are recomputed once
against the pre-commit state and once against the final state, and the stored
totals move by their difference.  The whole join result is never materialized
-- nested loops stream straight into on-disk accumulator rows, so a huge
fan-out on one key needs no in-memory join-row list.

Int64 overflow is decided on the *final* totals only; intermediate subtract/
add arithmetic is deliberately allowed to leave the int64 range transiently,
so a valid final result never trips a false alarm while a genuinely
out-of-range final result rejects the whole source commit before it becomes
visible.

Group identity
==============
Each distinct group tuple gets a stable token (the output row's ``row_id``)
allocated once and retained while the group is absent, so a disappearing and
reappearing group keeps its identity and two groups never share one.  Tuples
are encoded as ordered JSON of the original typed values -- bool stays bool,
integers stay integers, null stays distinct from the empty text.
"""

import json
import secrets
import time

from . import canonical
from .errors import MaterializedError, MaterializedNotReady
from .store import Store, hist_table
from .values import (INT64_MAX, INT64_MIN, TYPE_BOOL, TYPE_INT, TYPE_NULL,
                     TYPE_TEXT)

DEFINED = "defined"
BUILDING = "building"
ACTIVE = "active"
CANCELLED = "cancelled"
FAILED = "failed"

INNER = "inner"
LEFT = "left"
JOIN_KINDS = frozenset((INNER, LEFT))

COUNT_STAR = "count_star"
COUNT = "count"
SUM = "sum"
AGG_FNS = frozenset((COUNT_STAR, COUNT, SUM))

PHASE_INITIAL = "initial"
PHASE_CATCHUP = "catchup"
PHASE_PUBLISHING = "publishing"

# chunk size for IN-list key lookups so one maintenance step never binds an
# unbounded SQL variable list
KEY_CHUNK = 400

# ---------------------------------------------------------------------------
# Declaration validation and persistence
# ---------------------------------------------------------------------------


def _ref_side(ref):
    if not isinstance(ref, dict):
        raise MaterializedError("group/aggregate reference must be an object")
    side = ref.get("table")
    if side not in ("left", "right"):
        raise MaterializedError("reference table must be 'left' or 'right', got %r"
                                % (side,))
    column = ref.get("column")
    if not isinstance(column, str) or not column:
        raise MaterializedError("reference column must be a non-empty string")
    if column == "row_id":
        raise MaterializedError("row_id is the managed identity, not a business column")
    return side, column, ref.get("alias")


def _check_ident(store, name, what):
    return store.check_ident(name, what)


def validate_and_define(client, name, left_table, right_table, left_join_column,
                        right_join_column, join_kind, groups, aggregates):
    """Validate the restricted declaration fully, then persist it (no cut yet)."""
    store = client.store
    _check_ident(store, name, "materialized summary name")
    for tname in (left_table, right_table):
        _check_ident(store, tname, "table name")
    if join_kind not in JOIN_KINDS:
        raise MaterializedError("join_kind must be %r or %r" % (INNER, LEFT))
    if left_table == right_table:
        raise MaterializedError("the two managed tables must be different (self joins "
                                "are not supported)")
    if not isinstance(groups, (list, tuple)) or not groups:
        raise MaterializedError("groups must be a non-empty list of column references")
    if not isinstance(aggregates, (list, tuple)) or not aggregates:
        raise MaterializedError("aggregates must be a non-empty list")

    columns = store.column_types()
    if left_table not in columns:
        raise MaterializedError("unknown left table %r" % (left_table,))
    if right_table not in columns:
        raise MaterializedError("unknown right table %r" % (right_table,))
    left_types, right_types = columns[left_table], columns[right_table]
    if left_join_column not in left_types or left_types[left_join_column] != TYPE_TEXT:
        raise MaterializedError("left join column %s.%s must be a declared text column"
                                % (left_table, left_join_column))
    if right_join_column not in right_types or right_types[right_join_column] != TYPE_TEXT:
        raise MaterializedError("right join column %s.%s must be a declared text column"
                                 % (right_table, right_join_column))

    def coltype(side, column):
        types = left_types if side == "left" else right_types
        if column not in types:
            raise MaterializedError("unknown %s column %r" % (side, column))
        return types[column]

    output_names = set()
    group_specs = []
    seen_refs = set()
    for ref in groups:
        side, column, alias = _ref_side(ref)
        if (side, column) in seen_refs:
            raise MaterializedError("duplicate group reference %s.%s" % (side, column))
        seen_refs.add((side, column))
        ctype = coltype(side, column)
        out_name = alias or column
        _check_ident(store, out_name, "group output alias")
        if out_name in output_names or out_name in ("row_id", "version"):
            raise MaterializedError("duplicate or reserved output column name %r"
                                    % (out_name,))
        output_names.add(out_name)
        group_specs.append({"side": side, "column": column, "alias": out_name,
                            "type": ctype})

    agg_specs = []
    for agg in aggregates:
        if not isinstance(agg, dict):
            raise MaterializedError("aggregate must be an object")
        fn = agg.get("fn") or agg.get("function")
        if fn not in AGG_FNS:
            raise MaterializedError("aggregate fn must be one of %s" % (sorted(AGG_FNS),))
        alias = agg.get("alias")
        if not isinstance(alias, str) or not alias:
            raise MaterializedError("aggregate needs a non-empty output alias")
        _check_ident(store, alias, "aggregate alias")
        if alias in output_names or alias in ("row_id", "version"):
            raise MaterializedError("duplicate or reserved output column name %r"
                                    % (alias,))
        output_names.add(alias)
        spec = {"fn": fn, "alias": alias, "side": None, "column": None}
        if fn == COUNT_STAR:
            if agg.get("column") is not None:
                raise MaterializedError("count_star takes no column")
        else:
            col_ref = agg.get("column")
            if isinstance(col_ref, dict):
                side, column, _x = _ref_side(col_ref)
            else:
                # Flat shorthand: {"fn", "table", "column", "alias"}.
                side, column, _x = _ref_side(
                    {"table": agg.get("table"), "column": col_ref})
            ctype = coltype(side, column)
            if fn == SUM and ctype != TYPE_INT:
                raise MaterializedError("sum requires an int64 column, %s.%s is %r"
                                        % (side, column, ctype))
            spec["side"], spec["column"] = side, column
        agg_specs.append(spec)

    output_columns = [{"name": g["alias"], "type": g["type"], "kind": "group"}
                      for g in group_specs]
    for spec in agg_specs:
        output_columns.append({"name": spec["alias"], "type": TYPE_INT,
                               "kind": spec["fn"],
                               "nullable": spec["fn"] == SUM})

    definition = {
        "left_table": left_table, "right_table": right_table,
        "left_join_column": left_join_column,
        "right_join_column": right_join_column,
        "join_kind": join_kind, "groups": group_specs,
        "aggregates": agg_specs,
    }
    now = time.time()

    def _define(conn):
        if conn.execute("SELECT 1 FROM materialized_defs WHERE mv_name=?",
                        (name,)).fetchone() is not None:
            raise MaterializedError(
                "a materialized summary named %r already exists; definitions cannot be "
                "silently replaced" % name)
        if conn.execute("SELECT 1 FROM tables WHERE table_name=?",
                        (name,)).fetchone() is not None:
            raise MaterializedError("name %r is already an ordinary managed table" % name)
        if conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (hist_table(name),)).fetchone() is not None:
            raise MaterializedError("physical output table for %r already exists" % name)
        conn.execute(
            "INSERT INTO materialized_defs(mv_name, state, left_table, right_table, "
            "left_join_column, right_join_column, join_kind, definition_json, "
            "output_columns_json, cut_version, maintained_version, created_wall, "
            "updated_wall) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, ?, ?)",
            (name, DEFINED, left_table, right_table, left_join_column,
             right_join_column, join_kind,
             json.dumps(definition, sort_keys=True),
             json.dumps(output_columns, sort_keys=True), now, now))
        return _status_row(conn, name)
    return store.run_tx(_define)


# ---------------------------------------------------------------------------
# Durable row helpers
# ---------------------------------------------------------------------------


def _load_def(conn, name):
    row = conn.execute("SELECT * FROM materialized_defs WHERE mv_name=?",
                       (name,)).fetchone()
    if row is None:
        raise MaterializedError("no materialized summary named %r" % name)
    return row


def _def_objects(row):
    return (json.loads(row["definition_json"]),
            json.loads(row["output_columns_json"]))


def _status_row(conn, name):
    row = _load_def(conn, name)
    build = conn.execute("SELECT * FROM materialized_builds WHERE mv_name=?",
                         (name,)).fetchone()
    out = {"name": row["mv_name"], "state": row["state"],
           "left_table": row["left_table"], "right_table": row["right_table"],
           "join_kind": row["join_kind"],
           "cut_version": row["cut_version"],
           "maintained_version": row["maintained_version"],
           "current_source_position": Store.current_commit_version(conn),
           "definition": json.loads(row["definition_json"]),
           "output_columns": json.loads(row["output_columns_json"])}
    if build is not None:
        out["build"] = {"phase": build["phase"], "cut_version": build["cut_version"],
                        "left_cursor_row": build["left_cursor_row"],
                        "right_cursor_row": build["right_cursor_row"],
                        "catchup_from_version": build["catchup_from_version"]}
    return out


def list_defs(client):
    def _q(conn):
        return [_status_row(conn, r[0]) for r in conn.execute(
            "SELECT mv_name FROM materialized_defs ORDER BY mv_name")]
    return client.store.run_tx(_q, readonly=True)


def get_def(client, name):
    def _q(conn):
        return _status_row(conn, name)
    return client.store.run_tx(_q, readonly=True)


def active_defs_for_tables(conn, tables):
    """Active definitions whose join touches any of *tables*."""
    if not tables:
        return []
    marks = ", ".join("?" for _ in tables)
    rows = conn.execute(
        "SELECT * FROM materialized_defs WHERE state=? AND ("
        "left_table IN (%s) OR right_table IN (%s))" % (marks, marks),
        (ACTIVE, *tables, *tables)).fetchall()
    return [dict(r) for r in rows]


def build_phase_snapshots(conn):
    """Cut versions pinned by unfinished builds (GC retention need)."""
    return [int(r[0]) for r in conn.execute(
        "SELECT cut_version FROM materialized_defs WHERE state=?", (BUILDING,))]


def is_materialized(conn, name):
    return conn.execute("SELECT 1 FROM materialized_defs WHERE mv_name=?",
                        (name,)).fetchone() is not None


def output_schema(conn, name):
    """Ordered output column descriptors of a (possibly inactive) summary."""
    row = _load_def(conn, name)
    return json.loads(row["output_columns_json"])


# ---------------------------------------------------------------------------
# Build lifecycle
# ---------------------------------------------------------------------------


def _physical_decl(ctype):
    return {TYPE_INT: "INTEGER", TYPE_TEXT: "TEXT", TYPE_BOOL: "INTEGER",
            TYPE_NULL: ""}[ctype]


def _ensure_join_index(conn, table, column, add):
    hist = hist_table(table)
    idx = "idx_%s_%s_mvjoin" % (table, column)
    row = conn.execute(
        "SELECT refcount FROM materialized_indexes WHERE table_name=? AND column_name=?",
        (table, column)).fetchone()
    if row is None:
        conn.execute(
            'CREATE INDEX IF NOT EXISTS "%s" ON "%s"("%s")' % (idx, hist, column))
        conn.execute(
            "INSERT INTO materialized_indexes(table_name, column_name, refcount) "
            "VALUES (?, ?, 1)", (table, column))
    elif add:
        conn.execute(
            "UPDATE materialized_indexes SET refcount=refcount+1 WHERE table_name=? "
            "AND column_name=?", (table, column))
    return idx


def _release_join_indexes(conn, definition):
    for table, column in ((definition["left_table"], definition["left_join_column"]),
                          (definition["right_table"], definition["right_join_column"])):
        row = conn.execute(
            "SELECT refcount FROM materialized_indexes WHERE table_name=? AND column_name=?",
            (table, column)).fetchone()
        if row is None:
            continue
        remaining = int(row["refcount"]) - 1
        if remaining <= 0:
            idx = "idx_%s_%s_mvjoin" % (table, column)
            conn.execute('DROP INDEX IF EXISTS "%s"' % idx)
            conn.execute(
                "DELETE FROM materialized_indexes WHERE table_name=? AND column_name=?",
                (table, column))
        else:
            conn.execute(
                "UPDATE materialized_indexes SET refcount=? WHERE table_name=? AND column_name=?",
                (remaining, table, column))


def start_build(client, name):
    """Fix the source cut and create the durable build/output structures."""
    def _start(conn):
        row = _load_def(conn, name)
        if row["state"] != DEFINED:
            raise MaterializedError(
                "materialized summary %r is %r; only a defined summary can start a build"
                % (name, row["state"]))
        definition, out_cols = _def_objects(row)
        cut = Store.current_commit_version(conn)
        # The read-only append-only output table has the same physical shape
        # as an ordinary managed history table, so every existing snapshot,
        # subscription and GC mechanism can serve it unchanged.
        coldefs = ["row_id TEXT NOT NULL", "version INTEGER NOT NULL",
                   "deleted INTEGER NOT NULL"]
        for col in out_cols:
            decl = _physical_decl(col["type"])
            coldefs.append(('"%s" %s' % (col["name"], decl)) if decl
                           else '"%s"' % col["name"])
        coldefs.append("PRIMARY KEY (row_id, version)")
        conn.execute('CREATE TABLE "%s" (%s)'
                     % (hist_table(name), ", ".join(coldefs)))
        conn.execute('CREATE INDEX IF NOT EXISTS "%s_v" ON "%s"(row_id, version DESC)'
                     % (hist_table(name), hist_table(name)))
        _ensure_join_index(conn, definition["left_table"],
                           definition["left_join_column"], True)
        _ensure_join_index(conn, definition["right_table"],
                           definition["right_join_column"], True)
        now = time.time()
        conn.execute(
            "INSERT INTO materialized_builds(mv_name, cut_version, left_cursor_row, "
            "right_cursor_row, phase, catchup_from_version, updated_wall) "
            "VALUES (?, ?, '', '', ?, ?, ?)",
            (name, cut, PHASE_INITIAL, cut, now))
        conn.execute(
            "UPDATE materialized_defs SET state=?, cut_version=?, updated_wall=? "
            "WHERE mv_name=?", (BUILDING, cut, now, name))
        return _status_row(conn, name)
    return client.store.run_tx(_start)


def cancel_build(client, name):
    """Cancel a not-yet-active build and release every retained structure."""
    def _cancel(conn):
        row = _load_def(conn, name)
        if row["state"] == ACTIVE:
            raise MaterializedError(
                "materialized summary %r is active; deletion is not supported" % name)
        if row["state"] == CANCELLED:
            return _status_row(conn, name)
        definition, _out = _def_objects(row)
        conn.execute("DELETE FROM materialized_builds WHERE mv_name=?", (name,))
        conn.execute("DELETE FROM materialized_accum WHERE mv_name=?", (name,))
        conn.execute("DELETE FROM materialized_groups WHERE mv_name=?", (name,))
        conn.execute("DELETE FROM materialized_group_keys WHERE mv_name=?", (name,))
        conn.execute("DELETE FROM materialized_maintenance_stats WHERE mv_name=?",
                     (name,))
        conn.execute('DROP TABLE IF EXISTS "%s"' % hist_table(name))
        conn.execute('DROP INDEX IF EXISTS "%s_v"' % hist_table(name))
        _release_join_indexes(conn, definition)
        now = time.time()
        conn.execute(
            "UPDATE materialized_defs SET state=?, updated_wall=? WHERE mv_name=?",
            (CANCELLED, now, name))
        return _status_row(conn, name)
    return client.store.run_tx(_cancel)


def build_step(client, name, budget=500):
    """Advance one paged build inside one short transaction; resumable."""
    if isinstance(budget, bool) or not isinstance(budget, int) or budget < 1:
        raise MaterializedError("budget must be a positive integer")
    return client.store.run_tx(lambda conn: _build_step(conn, client, name, budget))


def build_status(client, name):
    return get_def(client, name)


# ---------------------------------------------------------------------------
# Typed tuple encoding
# ---------------------------------------------------------------------------


def group_key(values):
    """Stable encoding of a group tuple preserving type distinctions.

    JSON keeps True/False distinct from 0/1, None distinct from "" and ints as
    numbers; no separator concatenation is involved.
    """
    return canonical.values_to_json(list(values))


def _group_tuple(definition, left_values, right_values):
    out = []
    for g in definition["groups"]:
        vals = left_values if g["side"] == "left" else right_values
        out.append(vals.get(g["column"]))
    return tuple(out)


# ---------------------------------------------------------------------------
# Partial aggregation over restricted key sets
# ---------------------------------------------------------------------------


def _empty_measures(definition):
    measures = {"star": 0, "cols": {}, "sums": {}}
    for spec in definition["aggregates"]:
        if spec["fn"] == COUNT_STAR:
            continue
        if spec["fn"] == COUNT:
            measures["cols"][spec["alias"]] = 0
        else:
            measures["sums"][spec["alias"]] = {"sum": None, "nn": 0}
    return measures


def _add_measures(target, other):
    target["star"] += other["star"]
    for alias, value in other["cols"].items():
        target["cols"][alias] = target["cols"].get(alias, 0) + value
    for alias, part in other["sums"].items():
        slot = target["sums"].setdefault(alias, {"sum": None, "nn": 0})
        slot["nn"] += part["nn"]
        if part["sum"] is not None:
            slot["sum"] = part["sum"] if slot["sum"] is None else slot["sum"] + part["sum"]


def _join_row_measures(definition, left_values, right_values, matched):
    """Measures contributed by one join result row (right side may be nulls)."""
    measures = _empty_measures(definition)
    measures["star"] = 1
    for spec in definition["aggregates"]:
        if spec["fn"] == COUNT_STAR:
            continue
        vals = (left_values if spec["side"] == "left" else right_values)
        value = vals.get(spec["column"])
        if value is None:
            continue
        if spec["fn"] == COUNT:
            measures["cols"][spec["alias"]] = 1
        else:
            slot = measures["sums"][spec["alias"]]
            slot["nn"] = 1
            slot["sum"] = value
    return measures


def partial_aggregate(definition, left_rows, right_rows):
    """Aggregate join rows for already-restricted row mappings.

    ``left_rows`` / ``right_rows`` map ``row_id -> values`` and only contain
    rows whose join key belongs to the affected set (plus, on the left in
    ``left`` mode, rows with a null key that need a null-extended row).  The
    nested loop streams one join row at a time into a dict keyed by group
    tuple; it never builds a list of join rows.
    """
    lcol = definition["left_join_column"]
    rcol = definition["right_join_column"]
    left_mode = definition["join_kind"] == LEFT
    right_by_key = {}
    null_right = []
    for rid, vals in right_rows.items():
        key = vals.get(rcol)
        if key is None:
            null_right.append((rid, vals))
            continue
        right_by_key.setdefault(key, []).append((rid, vals))
    groups = {}
    for _lid, lvals in left_rows.items():
        key = lvals.get(lcol)
        if key is None:
            # Null join values never match.  Left mode keeps the row with the
            # right side entirely null; inner mode drops it.
            if left_mode:
                tup = _group_tuple(definition, lvals, {})
                _add_measures(groups.setdefault(
                    tup, _empty_measures(definition)),
                    _join_row_measures(definition, lvals, {}, False))
            continue
        matches = right_by_key.get(key)
        if not matches:
            if left_mode:
                tup = _group_tuple(definition, lvals, {})
                _add_measures(groups.setdefault(
                    tup, _empty_measures(definition)),
                    _join_row_measures(definition, lvals, {}, False))
            continue
        for _rid, rvals in matches:
            tup = _group_tuple(definition, lvals, rvals)
            _add_measures(groups.setdefault(
                tup, _empty_measures(definition)),
                _join_row_measures(definition, lvals, rvals, True))
    # null_right can never match (nulls never join); referenced only to make
    # the contract explicit.
    del null_right
    return groups


def _live_rows_by_keys(conn, table, join_column, version, keys, include_null,
                       stats):
    """Live images at *version* restricted to a set of join keys (indexed).

    Null is a first-class member of the affected key set when *include_null*
    is true (left mode must keep null-key left rows as null-extended join
    rows).  Counts every examined source row (live image and tombstone) into
    ``stats['source_rows_read']`` so maintenance cost is verifiable.
    """
    hist = hist_table(table)
    columns = [r[1] for r in conn.execute(
        "SELECT position, column_name FROM table_columns WHERE table_name=? "
        "ORDER BY position", (table,))]
    bool_cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM table_columns WHERE table_name=? AND column_type=?",
        (table, TYPE_BOOL))}
    out = {}
    normal = [k for k in keys if k is not None]
    chunks = [normal[i:i + KEY_CHUNK]
              for i in range(0, len(normal), KEY_CHUNK)]
    if include_null:
        chunks.append(None)
    if not chunks:
        return out
    for chunk in chunks:
        if chunk is None:
            where = 'h."%s" IS NULL' % join_column
            params = (version,)
        else:
            marks = ", ".join("?" for _ in chunk)
            where = 'h."%s" IN (%s)' % (join_column, marks)
            params = (version, *chunk)
        rows = conn.execute(
            'SELECT h.* FROM "%s" h JOIN (SELECT row_id, MAX(version) AS mv FROM "%s" '
            'WHERE version <= ? GROUP BY row_id) v ON h.row_id = v.row_id AND h.version = v.mv '
            'WHERE %s' % (hist, hist, where), params).fetchall()
        stats["source_rows_read"] += len(rows)
        for r in rows:
            if r["deleted"]:
                continue
            out[r["row_id"]] = {name: (bool(r[name]) if name in bool_cols
                                       else r[name]) for name in columns}
    return out


# ---------------------------------------------------------------------------
# On-disk accumulator
# ---------------------------------------------------------------------------


def _existing_group_token(conn, name, key_json):
    """The retained token for a group tuple, even while it is inactive.

    A group whose join rows all disappear keeps its identity row (with
    active=0) after its accumulator contribution is deleted, so a later
    reappearance must reuse this token instead of allocating a serial new one.
    """
    row = conn.execute(
        "SELECT group_token FROM materialized_groups WHERE mv_name=? "
        "AND group_key_json=?", (name, key_json)).fetchone()
    return row["group_token"] if row is not None else None


def _upsert_accum(conn, name, key_json, token, measures):
    conn.execute(
        "INSERT INTO materialized_accum(mv_name, group_key_json, group_token, measures_json) "
        "VALUES (?, ?, ?, ?) ON CONFLICT(mv_name, group_key_json) DO UPDATE SET "
        "group_token=excluded.group_token, measures_json=excluded.measures_json",
        (name, key_json, token, json.dumps(measures, sort_keys=True)))


def _all_accum_count(conn, name):
    return conn.execute("SELECT COUNT(*) FROM materialized_accum WHERE mv_name=?",
                        (name,)).fetchone()[0]


def _next_group_token(conn, name):
    """Allocate a never-before-used token for this summary.

    Based on the maximum numeric suffix ever allocated (not a live-row count),
    so an inactive group retained after its output disappeared cannot cause a
    second group to receive the same identity within one commit.
    """
    rows = conn.execute(
        "SELECT group_token FROM materialized_groups WHERE mv_name=?",
        (name,)).fetchall()
    maximum = 0
    for r in rows:
        token = r[0]
        try:
            maximum = max(maximum, int(token.split("-", 1)[1]))
        except (IndexError, ValueError):
            continue
    return "g-%08d" % (maximum + 1)


# ---------------------------------------------------------------------------
# The paged build
# ---------------------------------------------------------------------------


def _scan_left_page(conn, definition, cut, cursor, budget, stats):
    """One keyset page of left-side live rows at the frozen cut."""
    hist = hist_table(definition["left_table"])
    lcol = definition["left_join_column"]
    bool_cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM table_columns WHERE table_name=? AND column_type=?",
        (definition["left_table"], TYPE_BOOL))}
    rows = conn.execute(
        'SELECT h.* FROM "%s" h JOIN (SELECT row_id, MAX(version) AS mv FROM "%s" '
        'WHERE version <= ? GROUP BY row_id) v ON h.row_id = v.row_id AND h.version = v.mv '
        'WHERE h.deleted=0 AND h.row_id > ? ORDER BY h.row_id LIMIT ?'
        % (hist, hist), (cut, cursor, budget)).fetchall()
    stats["source_rows_read"] += len(rows)
    left = {}
    needed = {g["column"] for g in definition["groups"] if g["side"] == "left"}
    needed.add(lcol)
    # Left-side aggregate arguments (COUNT/SUM columns) must be carried too.
    for spec in definition["aggregates"]:
        if spec["side"] == "left":
            needed.add(spec["column"])
    for r in rows:
        left[r["row_id"]] = {name: (bool(r[name]) if name in bool_cols else r[name])
                             for name in needed}
    next_cursor = rows[-1]["row_id"] if rows else cursor
    return left, next_cursor, len(rows)


def _build_step(conn, client, name, budget):
    row = _load_def(conn, name)
    if row["state"] != BUILDING:
        raise MaterializedError("materialized summary %r is not building (state=%r)"
                                % (name, row["state"]))
    definition, _out = _def_objects(row)
    build = conn.execute("SELECT * FROM materialized_builds WHERE mv_name=?",
                         (name,)).fetchone()
    cut = build["cut_version"]
    stats = {"source_rows_read": 0, "helper_records_read": 0, "groups_written": 0}
    phase = build["phase"]

    if phase == PHASE_INITIAL:
        left, next_cursor, n = _scan_left_page(
            conn, definition, cut, build["left_cursor_row"], budget, stats)
        if n:
            keys = {v.get(definition["left_join_column"]) for v in left.values()}
            right = _live_rows_by_keys(
                conn, definition["right_table"], definition["right_join_column"],
                cut, keys, include_null=False, stats=stats)
            partial = partial_aggregate(definition, left, right)
            for tup, measures in partial.items():
                key_json = group_key(tup)
                existing = conn.execute(
                    "SELECT measures_json FROM materialized_accum WHERE mv_name=? "
                    "AND group_key_json=?", (name, key_json)).fetchone()
                stats["helper_records_read"] += 1
                if existing is not None:
                    merged = json.loads(existing["measures_json"])
                    _add_measures(merged, measures)
                    measures = merged
                _upsert_accum(conn, name, key_json, None, measures)
                # Accumulate this group's join-key membership across pages.
                gkeys, gnull = _group_join_keys(definition, tup, left, right)
                known = {r[0] for r in conn.execute(
                    "SELECT join_key FROM materialized_group_keys WHERE mv_name=? "
                    "AND group_key_json=? AND null_scope=0", (name, key_json))}
                known_null = conn.execute(
                    "SELECT 1 FROM materialized_group_keys WHERE mv_name=? "
                    "AND group_key_json=? AND null_scope=1",
                    (name, key_json)).fetchone() is not None
                _record_group_keys(conn, name, key_json,
                                   known | gkeys, known_null or gnull)
                stats["groups_written"] += 1
        conn.execute(
            "UPDATE materialized_builds SET left_cursor_row=?, updated_wall=? WHERE mv_name=?",
            (next_cursor, time.time(), name))
        if n < budget:
            # Whole left side scanned at the same frozen cut.
            conn.execute(
                "UPDATE materialized_builds SET phase=?, catchup_from_version=?, "
                "updated_wall=? WHERE mv_name=?",
                (PHASE_CATCHUP, cut, time.time(), name))
        _write_stats(conn, name, None, "build", stats)
        return _status_row(conn, name)

    if phase == PHASE_CATCHUP:
        current = Store.current_commit_version(conn)
        maintained = build["catchup_from_version"]
        if maintained >= current:
            conn.execute(
                "UPDATE materialized_builds SET phase=?, updated_wall=? WHERE mv_name=?",
                (PHASE_PUBLISHING, time.time(), name))
            return _status_row(conn, name)
        target = min(current, maintained + 1)
        _catch_up_commit(conn, definition, name, maintained + 1, stats)
        conn.execute(
            "UPDATE materialized_builds SET catchup_from_version=?, updated_wall=? "
            "WHERE mv_name=?", (target, time.time(), name))
        conn.execute(
            "UPDATE materialized_defs SET maintained_version=?, updated_wall=? WHERE mv_name=?",
            (target, time.time(), name))
        if target >= current:
            conn.execute(
                "UPDATE materialized_builds SET phase=?, updated_wall=? WHERE mv_name=?",
                (PHASE_PUBLISHING, time.time(), name))
        _write_stats(conn, name, target, "catchup", stats)
        return _status_row(conn, name)

    # PUBLISHING: move accumulator groups into the read-only output table in
    # pages, allocate stable identities, then activate at one position V.
    return _publish_page(conn, definition, name, budget, stats)


def _catch_up_commit(conn, definition, name, version, stats):
    """Incorporate one already-committed source change into the accumulator."""
    changes = _commit_changes(conn, definition, version)
    descriptors, _old, _new = compute_maintenance(
        conn, definition, name, version - 1, version, changes, stats)
    persist_maintenance(conn, name, descriptors, stats)


def _commit_changes(conn, definition, version):
    """Folded per-identity changes of one source commit on the two tables."""
    tables = (definition["left_table"], definition["right_table"])
    marks = ", ".join("?" for _ in tables)
    rows = conn.execute(
        "SELECT table_name, row_id, op, old_values, new_values FROM committed_writes "
        "WHERE commit_version=? AND table_name IN (%s) ORDER BY id"
        % marks, (version, *tables)).fetchall()
    changes = {}
    for r in rows:
        changes.setdefault(r["table_name"], {})[r["row_id"]] = {
            "old": canonical.values_from_json(r["old_values"])
            if r["old_values"] is not None else None,
            "new": canonical.values_from_json(r["new_values"])
            if r["new_values"] is not None else None}
    return changes


def _changed_join_keys(changes, definition):
    """All non-null join keys this commit touches, on either side/state."""
    keys = set()
    for table, col in ((definition["left_table"], definition["left_join_column"]),
                       (definition["right_table"], definition["right_join_column"])):
        for ch in changes.get(table, {}).values():
            for image in (ch["old"], ch["new"]):
                if image is not None and image.get(col) is not None:
                    keys.add(image.get(col))
    return keys


def _null_left_changed(changes, definition):
    """Whether this commit changes any null-key left row (either state)."""
    if definition["join_kind"] != LEFT:
        return False
    col = definition["left_join_column"]
    for ch in changes.get(definition["left_table"], {}).values():
        for image in (ch["old"], ch["new"]):
            if image is not None and image.get(col) is None:
                return True
    return False


def _groups_for_keys(conn, name, keys):
    """Group tuples currently fed by any of the non-null *keys*."""
    normal = [k for k in keys if k is not None]
    if not normal:
        return set()
    marks = ", ".join("?" for _ in normal)
    rows = conn.execute(
        "SELECT DISTINCT group_key_json FROM materialized_group_keys "
        "WHERE mv_name=? AND null_scope=0 AND join_key IN (%s)" % marks,
        (name, *normal)).fetchall()
    return {r[0] for r in rows}


def _null_groups(conn, name):
    """Group tuples currently fed through the left null scope."""
    return {r[0] for r in conn.execute(
        "SELECT DISTINCT group_key_json FROM materialized_group_keys "
        "WHERE mv_name=? AND null_scope=1", (name,))}


def _group_key_scope(conn, name, group_jsons):
    """(non-null keys, null marker) currently feeding the given groups."""
    keys = set()
    null_scope = False
    group_jsons = list(group_jsons)
    if not group_jsons:
        return keys, null_scope
    marks = ", ".join("?" for _ in group_jsons)
    for r in conn.execute(
            "SELECT join_key, null_scope FROM materialized_group_keys "
            "WHERE mv_name=? AND group_key_json IN (%s)" % marks,
            (name, *group_jsons)).fetchall():
        if r["null_scope"]:
            null_scope = True
        elif r["join_key"] is not None:
            keys.add(r["join_key"])
    return keys, null_scope


def _load_state_rows(conn, definition, version, keys, left_null, stats):
    """Live rows of both sides at *version* over the given non-null key set."""
    left = _live_rows_by_keys(
        conn, definition["left_table"], definition["left_join_column"],
        version, keys, include_null=left_null, stats=stats)
    right = _live_rows_by_keys(
        conn, definition["right_table"], definition["right_join_column"],
        version, keys, include_null=False, stats=stats)
    return left, right


def _recompute_scope(definition, left, right):
    """Partial aggregates of the loaded scopes, keyed by group tuple."""
    return partial_aggregate(definition, left, right)


def _record_group_keys(conn, name, group_key_json, join_keys, null_scope):
    """Persist the join keys (and null marker) that feed one group."""
    conn.execute("DELETE FROM materialized_group_keys WHERE mv_name=? "
                 "AND group_key_json=?", (name, group_key_json))
    for key in join_keys:
        conn.execute(
            "INSERT INTO materialized_group_keys(mv_name, group_key_json, "
            "join_key, null_scope) VALUES (?, ?, ?, 0)",
            (name, group_key_json, key))
    if null_scope:
        conn.execute(
            "INSERT INTO materialized_group_keys(mv_name, group_key_json, "
            "join_key, null_scope) VALUES (?, ?, NULL, 1)",
            (name, group_key_json))


def _group_join_keys(definition, tup, left, right):
    """Non-null keys and null marker of the rows of *tup* in the loaded scope.

    Membership is verified against the actual group tuple, so a key that is
    loaded only because it changed but whose rows belong to another group is
    never attributed here.
    """
    keys = set()
    null_scope = False
    lcol = definition["left_join_column"]
    rcol = definition["right_join_column"]
    right_by_key = {}
    for vals in right.values():
        k = vals.get(rcol)
        if k is not None:
            right_by_key.setdefault(k, []).append(vals)
    for lvals in left.values():
        k = lvals.get(lcol)
        if k is None:
            if definition["join_kind"] == LEFT and _group_tuple(
                    definition, lvals, {}) == tup:
                null_scope = True
            continue
        matches = right_by_key.get(k)
        if not matches:
            if definition["join_kind"] == LEFT and _group_tuple(
                    definition, lvals, {}) == tup:
                keys.add(k)
            continue
        for rvals in matches:
            if _group_tuple(definition, lvals, rvals) == tup:
                keys.add(k)
    return keys, null_scope


def _fold_state(left, right, definition, changes, state):
    """Apply the commit's net changes onto one loaded state mapping."""
    is_after = state == "after"
    for rid, ch in changes.get(definition["left_table"], {}).items():
        if not is_after:
            if ch["old"] is None:
                left.pop(rid, None)
        elif ch["new"] is None:
            left.pop(rid, None)
        else:
            left[rid] = ch["new"]
    for rid, ch in changes.get(definition["right_table"], {}).items():
        if not is_after:
            if ch["old"] is None:
                right.pop(rid, None)
        elif ch["new"] is None:
            right.pop(rid, None)
        else:
            right[rid] = ch["new"]


def compute_maintenance(conn, definition, name, before_version, after_version,
                        changes, stats):
    """Recompute every group a changed join key can affect, in both states.

    The scoping chain is exact and converges:

    changed non-null keys (+ the left null scope) -> the groups they currently
    feed -> every join key those groups own -> load both states over that key
    universe -> recompute partial aggregates -> the affected group set is the
    before/after partial union.  A group fed by several join keys is therefore
    always recomputed from all of them even when one key changed, and a group
    reached only through the null scope is discovered from the actual null-key
    left rows rather than a possibly stale marker.
    """
    changed = _changed_join_keys(changes, definition)
    null_changed = _null_left_changed(changes, definition)

    # Seed groups: groups fed by a changed non-null key, plus (in left mode)
    # every group currently reached through the null scope.
    seed_groups = _groups_for_keys(conn, name, changed)
    if null_changed:
        seed_groups |= _null_groups(conn, name)

    # The full key universe of those groups, always including the changed keys
    # (brand-new groups have no membership rows yet) and the null scope.
    universe, universe_null = _group_key_scope(conn, name, seed_groups)
    universe |= changed
    load_null = universe_null or null_changed

    # Fixpoint expansion.  The seed groups are found from current membership,
    # but a changed row can migrate *into* a group that no changed key directly
    # names yet: recomputing over the current universe surfaces that group,
    # whose persisted keys then extend the universe, and so on until the loaded
    # scope contains every join key of every affected group in both states.
    for _ in range(16):
        before_left, before_right = _load_state_rows(
            conn, definition, before_version, universe, load_null, stats)
        after_left, after_right = _load_state_rows(
            conn, definition, after_version, universe, load_null, stats)
        _fold_state(before_left, before_right, definition, changes, "before")
        _fold_state(after_left, after_right, definition, changes, "after")
        probe_groups = set(_recompute_scope(definition, before_left, before_right))
        probe_groups |= set(_recompute_scope(definition, after_left, after_right))
        probe_jsons = [group_key(tup) for tup in probe_groups]
        extra_keys, extra_null = _group_key_scope(conn, name, probe_jsons)
        grown = (extra_keys - universe) or (extra_null and not load_null)
        if not grown:
            break
        universe |= extra_keys
        load_null = load_null or extra_null

    old_groups = _recompute_scope(definition, before_left, before_right)
    new_groups = _recompute_scope(definition, after_left, after_right)
    affected = set(old_groups) | set(new_groups)

    descriptors = []
    for tup in affected:
        key_json = group_key(tup)
        token = _existing_group_token(conn, name, key_json)
        fresh = new_groups.get(tup)
        disappears = fresh is None or fresh["star"] == 0
        if disappears:
            join_keys, join_null = set(), False
        else:
            join_keys, join_null = _group_join_keys(
                definition, tup, after_left, after_right)
        descriptors.append({
            "group_tuple": tup, "key_json": key_json, "token": token,
            "measures": fresh if not disappears else _empty_measures(definition),
            "disappears": disappears, "join_keys": (join_keys, join_null)})
    return descriptors, old_groups, new_groups


def persist_maintenance(conn, name, descriptors, stats):
    """Rewrite accumulator rows and group-key membership for affected groups."""
    for desc in descriptors:
        stats["groups_written"] += 1
        if desc["disappears"]:
            conn.execute(
                "DELETE FROM materialized_accum WHERE mv_name=? AND group_key_json=?",
                (name, desc["key_json"]))
            conn.execute(
                "DELETE FROM materialized_group_keys WHERE mv_name=? "
                "AND group_key_json=?", (name, desc["key_json"]))
        else:
            _upsert_accum(conn, name, desc["key_json"], desc["token"],
                          desc["measures"])
            keys, null_scope = desc["join_keys"]
            _record_group_keys(conn, name, desc["key_json"], keys, null_scope)


def _output_values(definition, tup, measures):
    values = {}
    for i, g in enumerate(definition["groups"]):
        values[g["alias"]] = tup[i]
    for spec in definition["aggregates"]:
        alias = spec["alias"]
        if spec["fn"] == COUNT_STAR:
            values[alias] = measures["star"]
        elif spec["fn"] == COUNT:
            values[alias] = measures["cols"][alias]
        else:
            slot = measures["sums"][alias]
            values[alias] = None if slot["nn"] == 0 else slot["sum"]
    return values


def _publish_page(conn, definition, name, budget, stats):
    total = _all_accum_count(conn, name)
    published = conn.execute(
        "SELECT COUNT(*) FROM materialized_groups WHERE mv_name=?", (name,)
    ).fetchone()[0]
    if published >= total:
        return _activate(conn, definition, name)
    rows = conn.execute(
        "SELECT group_key_json, measures_json FROM materialized_accum WHERE mv_name=? "
        "ORDER BY group_key_json LIMIT ? OFFSET ?", (name, budget, published)).fetchall()
    out_cols = [c["name"] for c in json.loads(
        conn.execute("SELECT output_columns_json FROM materialized_defs WHERE mv_name=?",
                     (name,)).fetchone()["output_columns_json"])]
    activation_version = Store.current_commit_version(conn) + 1
    for r in rows:
        tup = tuple(json.loads(r["group_key_json"]))
        measures = json.loads(r["measures_json"])
        token = _next_group_token(conn, name)
        values = _output_values(definition, tup, measures)
        conn.execute(
            "INSERT INTO materialized_groups(mv_name, group_token, group_key_json, active) "
            "VALUES (?, ?, ?, 1)", (name, token, r["group_key_json"]))
        conn.execute(
            "UPDATE materialized_accum SET group_token=? WHERE mv_name=? AND group_key_json=?",
            (token, name, r["group_key_json"]))
        _insert_output(conn, name, out_cols, token, activation_version, values, deleted=0)
        stats["groups_written"] += 1
    return _maybe_activate(conn, definition, name)


def _maybe_activate(conn, definition, name):
    total = _all_accum_count(conn, name)
    published = conn.execute(
        "SELECT COUNT(*) FROM materialized_groups WHERE mv_name=?", (name,)
    ).fetchone()[0]
    if published < total:
        _write_stats(conn, name, None, "build",
                     {"source_rows_read": 0, "helper_records_read": 0,
                      "groups_written": published})
        return _status_row(conn, name)
    return _activate(conn, definition, name)


def _activate(conn, definition, name):
    # The single explicit source position at which the summary first becomes
    # readable.  No source row commit can interleave: we hold the write lock.
    activation_version = Store.bump_commit_version(conn)
    now = time.time()
    conn.execute(
        "UPDATE materialized_defs SET state=?, maintained_version=?, updated_wall=? "
        "WHERE mv_name=?", (ACTIVE, activation_version, now, name))
    conn.execute("DELETE FROM materialized_builds WHERE mv_name=?", (name,))
    _write_stats(conn, name, activation_version, "build",
                 {"source_rows_read": 0, "helper_records_read": 0,
                  "groups_written": 0})
    return _status_row(conn, name)


def _insert_output(conn, name, out_cols, token, version, values, deleted):
    placeholders = ["row_id", "version", "deleted"] + ['"%s"' % c for c in out_cols]
    args = [token, version, 1 if deleted else 0]
    for c in out_cols:
        args.append(None if deleted else values[c])
    conn.execute(
        'INSERT INTO "%s" (%s) VALUES (%s)'
        % (hist_table(name), ", ".join(placeholders),
           ", ".join("?" for _ in placeholders)), args)


def _write_stats(conn, name, version, phase, stats):
    conn.execute(
        "INSERT INTO materialized_maintenance_stats(mv_name, commit_version, phase, "
        "source_rows_read, helper_records_read, groups_written, detail_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (name, version, phase, stats["source_rows_read"],
         stats["helper_records_read"], stats["groups_written"],
         json.dumps(stats, sort_keys=True)))


def maintenance_stats(client, name=None, limit=50):
    def _q(conn):
        sql = ("SELECT mv_name, commit_version, phase, source_rows_read, "
               "helper_records_read, groups_written, detail_json, id "
               "FROM materialized_maintenance_stats")
        params = ()
        if name is not None:
            sql += " WHERE mv_name=?"
            params = (name,)
        sql += " ORDER BY id DESC LIMIT ?"
        params = (*params, limit)
        out = []
        for r in conn.execute(sql, params):
            out.append({"name": r["mv_name"], "commit_version": r["commit_version"],
                        "phase": r["phase"], "source_rows_read": r["source_rows_read"],
                        "helper_records_read": r["helper_records_read"],
                        "groups_written": r["groups_written"],
                        "detail": json.loads(r["detail_json"])})
        return out
    return client.store.run_tx(_q, readonly=True)


# ---------------------------------------------------------------------------
# Group identity mapping (public)
# ---------------------------------------------------------------------------


def group_identity(client, name, group_tuple):
    """Stable row_id of a group tuple (only while it exists or once existed)."""
    if not isinstance(group_tuple, (list, tuple)):
        raise MaterializedError("group_tuple must be a tuple/list of group values")
    key_json = group_key(tuple(group_tuple))

    def _q(conn):
        row = _load_def(conn, name)
        rec = conn.execute(
            "SELECT group_token, active FROM materialized_groups WHERE mv_name=? "
            "AND group_key_json=?", (name, key_json)).fetchone()
        if rec is None:
            return None
        return {"group_identity": rec["group_token"], "active": bool(rec["active"]),
                "state": row["state"]}
    return client.store.run_tx(_q, readonly=True)


def tuple_for_identity(client, name, token):
    def _q(conn):
        rec = conn.execute(
            "SELECT group_key_json, active FROM materialized_groups WHERE mv_name=? "
            "AND group_token=?", (name, token)).fetchone()
        if rec is None:
            return None
        return {"group_identity": token, "group_tuple": tuple(
            json.loads(rec["group_key_json"])), "active": bool(rec["active"])}
    return client.store.run_tx(_q, readonly=True)


def list_group_identities(client, name, include_inactive=True):
    def _q(conn):
        sql = ("SELECT group_token, group_key_json, active FROM materialized_groups "
               "WHERE mv_name=?")
        if not include_inactive:
            sql += " AND active=1"
        sql += " ORDER BY group_token"
        return [{"group_identity": r["group_token"],
                 "group_tuple": tuple(json.loads(r["group_key_json"])),
                 "active": bool(r["active"])}
                for r in conn.execute(sql, (name,))]
    return client.store.run_tx(_q, readonly=True)


# ---------------------------------------------------------------------------
# Read-only serving (inside a transaction's fixed start view)
# ---------------------------------------------------------------------------


def read_rows(conn, name, version, selected, pred, predicate_holds, row_dict):
    """Materialize visible output rows at the transaction's snapshot version."""
    row = _load_def(conn, name)
    if row["state"] != ACTIVE:
        raise MaterializedNotReady(
            "materialized summary %r is %r; it cannot serve complete reads until active"
            % (name, row["state"]))
    out_cols = json.loads(row["output_columns_json"])
    types = {c["name"]: c["type"] for c in out_cols}
    hist = hist_table(name)
    rows = conn.execute(
        'SELECT h.* FROM "%s" h JOIN (SELECT row_id, MAX(version) AS mv FROM "%s" '
        'WHERE version <= ? GROUP BY row_id) v ON h.row_id = v.row_id AND h.version = v.mv '
        'ORDER BY h.row_id' % (hist, hist), (version,)).fetchall()
    out = []
    for r in rows:
        if r["deleted"]:
            continue
        values = row_dict(r, types)
        if not predicate_holds(pred, r["row_id"], values):
            continue
        item = {"row_id": r["row_id"], "version": r["version"]}
        for col in selected:
            if col != "row_id":
                item[col] = values[col]
        out.append(item)
    return out, list(types)


# ---------------------------------------------------------------------------
# Incremental maintenance inside a source commit transaction
# ---------------------------------------------------------------------------


def plan_commit(conn, engine, changes, provisional_version):
    """Compute every affected summary's maintenance plan before the version bump.

    Returns ``(plans, problems)``.  A problem (final int64 overflow) rejects the
    whole source commit; no plan is applied and nothing is visible.  All
    arithmetic is ordinary Python int arithmetic -- only the *final* stored
    totals are range-checked, so a subtract-then-add sequence can transiently
    leave int64 range without a false positive.
    """
    tables = set(changes)
    defs = active_defs_for_tables(conn, tables)
    plans = []
    problems = []
    current = Store.current_commit_version(conn)
    group_aliases = None
    for drow in defs:
        name = drow["mv_name"]
        definition, out_cols = _def_objects(drow)
        group_aliases = {g["alias"] for g in definition["groups"]}
        stats = {"source_rows_read": 0, "helper_records_read": 0,
                 "groups_written": 0}
        descriptors, _old, _new = compute_maintenance(
            conn, definition, name, current, provisional_version, changes, stats)
        writes = []
        overflow = []
        for desc in descriptors:
            tup = desc["group_tuple"]
            token = desc["token"]
            disappears = desc["disappears"]
            final_measures = desc["measures"]
            final_values = _output_values(definition, tup, final_measures)
            for col_name, value in final_values.items():
                if col_name in group_aliases:
                    continue
                if value is not None and not (INT64_MIN <= value <= INT64_MAX):
                    overflow.append({"group_identity": token, "group_tuple": list(tup),
                                     "column": col_name, "value": value})
            old_values = None
            old_present = False
            if token is not None:
                grow = conn.execute(
                    'SELECT h.* FROM "%s" h JOIN (SELECT row_id, MAX(version) AS mv FROM "%s" '
                    'GROUP BY row_id) v ON h.row_id=v.row_id AND h.version=v.mv '
                    'WHERE h.row_id=?' % (hist_table(name), hist_table(name)),
                    (token,)).fetchone()
                stats["helper_records_read"] += 1
                if grow is not None and not grow["deleted"]:
                    old_present = True
                    old_values = {c["name"]: grow[c["name"]] for c in out_cols}
            writes.append({"group_tuple": tup, "key_json": desc["key_json"],
                           "token": token, "measures": final_measures,
                           "values": final_values, "old_values": old_values,
                           "old_present": old_present, "disappears": disappears,
                           "join_keys": desc["join_keys"]})
        if overflow:
            problems.append({
                "code": "materialized_overflow", "dependency": "mv",
                "summary": ("materialized summary %r would leave int64 range at commit "
                            "version %s" % (name, provisional_version)),
                "materialized": name, "snapshot_version": current,
                "commit_version": provisional_version, "overflow": overflow,
                "witness": {"materialized": name, "groups": overflow}})
        plans.append({"name": name, "definition": definition, "out_cols": out_cols,
                      "writes": writes, "stats": stats})
    return plans, problems


def apply_commit(conn, plans, commit_version, watched, user_txid):
    """Apply planned summary changes in the same atomic source commit.

    Output history, write evidence, subscription changes, accumulator and
    group identity rows all land together.  A group whose business values did
    not change gets no new row version and no subscription event, although the
    maintenance progress and statistics are still durably recorded.
    """
    reports = []
    for plan in plans:
        name = plan["name"]
        definition = plan["definition"]
        out_cols = plan["out_cols"]
        col_names = [c["name"] for c in out_cols]
        stats = plan["stats"]
        changed_outputs = 0
        for w in plan["writes"]:
            token = w["token"]
            if token is None and not w["disappears"]:
                token = _next_group_token(conn, name)
                conn.execute(
                    "INSERT INTO materialized_groups(mv_name, group_token, group_key_json, "
                    "active) VALUES (?, ?, ?, 1)",
                    (name, token, w["key_json"]))
            # Persistent accumulator (also carries SUM non-null counts) and
            # the group -> join-key membership used to expand future scopes.
            # A group with no remaining join row drops both; its stable
            # identity is retained in materialized_groups.
            if w["disappears"]:
                conn.execute(
                    "DELETE FROM materialized_accum WHERE mv_name=? AND group_key_json=?",
                    (name, w["key_json"]))
                conn.execute(
                    "DELETE FROM materialized_group_keys WHERE mv_name=? "
                    "AND group_key_json=?", (name, w["key_json"]))
            else:
                _upsert_accum(conn, name, w["key_json"], token, w["measures"])
                gkeys, gnull = w["join_keys"]
                _record_group_keys(conn, name, w["key_json"], gkeys, gnull)
            stats["groups_written"] += 1
            if w["disappears"]:
                conn.execute(
                    "UPDATE materialized_groups SET active=0 WHERE mv_name=? AND group_token=?",
                    (name, token))
                if w["old_present"]:
                    _insert_output(conn, name, col_names, token, commit_version,
                                   None, deleted=1)
                    _evidence_and_change(conn, name, token, w["old_values"], None,
                                         commit_version, watched, user_txid, stats)
                    changed_outputs += 1
                continue
            conn.execute(
                "UPDATE materialized_groups SET active=1 WHERE mv_name=? AND group_token=?",
                (name, token))
            if not w["old_present"]:
                _insert_output(conn, name, col_names, token, commit_version,
                               w["values"], deleted=0)
                _evidence_and_change(conn, name, token, None, w["values"],
                                     commit_version, watched, user_txid, stats)
                changed_outputs += 1
            elif w["old_values"] != w["values"]:
                _insert_output(conn, name, col_names, token, commit_version,
                               w["values"], deleted=0)
                _evidence_and_change(conn, name, token, w["old_values"], w["values"],
                                     commit_version, watched, user_txid, stats)
                changed_outputs += 1
        _write_stats(conn, name, commit_version, "maintenance", stats)
        reports.append({"materialized": name, "commit_version": commit_version,
                        "groups_affected": len(plan["writes"]),
                        "output_rows_changed": changed_outputs, "stats": stats})
    return reports


def _evidence_and_change(conn, name, token, old_values, new_values, commit_version,
                         watched, user_txid, stats):
    old_json = canonical.values_to_json(old_values) if old_values is not None else None
    new_json = canonical.values_to_json(new_values) if new_values is not None else None
    conn.execute(
        "INSERT INTO committed_writes(ordinal, user_txid, table_name, row_id, op, "
        "old_values, new_values, old_version, new_version, commit_version) "
        "VALUES ((SELECT COALESCE(MAX(ordinal),0)+1 FROM committed_writes), "
        "?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (user_txid, name, token,
         "delete" if new_values is None else ("insert" if old_values is None else "update"),
         old_json, new_json,
         commit_version if old_values is not None else None,
         commit_version, commit_version))
    if name in watched:
        conn.execute(
            "INSERT INTO sub_changes(commit_version, user_txid, table_name, row_id, "
            "old_present, old_values, old_version, new_present, new_values, new_version) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (commit_version, user_txid, name, token,
             1 if old_values is not None else 0, old_json,
             commit_version if old_values is not None else None,
             1 if new_values is not None else 0, new_json,
             commit_version if new_values is not None else None))
