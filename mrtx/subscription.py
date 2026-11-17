"""Resumable change subscriptions over a managed repository.

A subscription is a durable, server-side cursor over one source repository.  It
is created with a name and a set of managed tables, which fixes

* a persistent identity (``sub_id`` plus an ownership token),
* generation ``1``, and
* a cut version ``S`` -- the ``commit_version`` captured atomically with the
  subscription row.

Delivery has two phases:

``full``
    The complete committed content of exactly the subscribed tables at ``S``,
    ordered by table name then ``row_id`` in Unicode code-point order.  Every
    row carries its image version at ``S`` and its original typed values.  The
    page size is respected; the final page says ``full_complete``.

``incremental``
    Changes of source commits strictly after the acknowledged position.  One
    source commit is never split across batches; a single transaction larger
    than the budget is still delivered whole.  Commits that changed no
    subscribed table (including read-only commits) are covered by a batch that
    may carry no items.

Exactly one unacknowledged batch can exist per subscription.  Its identity and
payload are durable, so re-pulling from any process, with any budget, returns
the same ``batch_id`` and the same business content until it is acknowledged.
An acknowledgement names only that batch id; the position advanced is the one
durably recorded when the batch was produced, never a client-supplied integer.

History reuse and retention
===========================
The full phase is served directly from the append-only row history at ``S``;
there is no private snapshot copy and no long-lived SQLite transaction.  The
version GC keeps every version needed to reconstruct ``S`` for as long as any
subscription is still in its full phase.  Incremental changes are copied at
commit time into ``sub_changes`` for tables covered by *some* open
subscription.  :func:`cleanup_sync_history` prunes that log only up to the
slowest open subscription's acknowledged position (a full-phase subscription's
position is its cut), so a lagging consumer never loses a change.  A closed
subscription releases its retention responsibility immediately.
"""

import hashlib
import hmac
import json
import os
import secrets
import time

from .errors import (SchemaError, StaleSubscriptionGeneration,
                     SubscriptionClosed, SubscriptionError,
                     SubscriptionNotFound)
from .store import Store, hist_table
from .values import COLUMN_TYPES, TYPE_BOOL

ACTIVE = "active"
CLOSED = "closed"

PHASE_FULL = "full"
PHASE_INCREMENTAL = "incremental"

# Item markers.  ``absent`` is a first-class tag, never a business ``None``.
PRESENT = "present"
ABSENT = "absent"


def new_sub_id():
    return "sub-" + secrets.token_hex(12)


def _hash_token(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _row_image(row, columns):
    """Typed image of one history row (declared columns only)."""
    out = {}
    for name, ctype in columns.items():
        value = row[name]
        if ctype == TYPE_BOOL and value is not None:
            value = bool(value)
        out[name] = value
    return out


def _sorted_tables(tables):
    return sorted(set(tables))


class SubscriptionHandle:
    """Picklable reference proving ownership of a subscription generation.

    ``token`` is a secret; ``db_path`` records which source repository the
    subscription belongs to.  A handle from another repository or generation
    can never advance this subscription's state.
    """

    __slots__ = ("sub_id", "generation", "token", "db_path")

    def __init__(self, sub_id, generation, token, db_path):
        self.sub_id = sub_id
        self.generation = generation
        self.token = token
        self.db_path = db_path

    def as_dict(self):
        return {"sub_id": self.sub_id, "generation": self.generation,
                "token": self.token, "db_path": self.db_path}

    @classmethod
    def from_dict(cls, obj):
        return cls(obj["sub_id"], obj["generation"], obj["token"], obj["db_path"])

    def __repr__(self):
        return "SubscriptionHandle(%r, gen=%r, db=%r)" % (
            self.sub_id, self.generation, self.db_path)


class Subscription:
    """Client-side facade bound to a :class:`Client`."""

    def __init__(self, client, handle):
        self._client = client
        self.handle = handle
        self.sub_id = handle.sub_id

    def pull(self, max_rows=100):
        return self._client.pull(self.handle, max_rows=max_rows)

    def ack(self, batch_id):
        return self._client.ack(self.handle, batch_id)

    def status(self):
        return self._client.subscription_status(self.handle)

    def close(self):
        return self._client.close_subscription(self.handle)

    def rebuild(self):
        """Explicitly replace this subscription with a new cut and generation."""
        return self._client.rebuild_subscription(self.handle)


def _sub_row(conn, sub_id):
    return conn.execute("SELECT * FROM subscriptions WHERE sub_id=?",
                        (sub_id,)).fetchone()


def _check_handle(conn, handle):
    """Authenticate a handle against its durable row; raise on mismatch.

    The generation is checked *before* the token so a superseded generation
    reports :class:`StaleSubscriptionGeneration` (an old generation's token
    has by construction been replaced).
    """
    row = _sub_row(conn, handle.sub_id)
    if row is None:
        raise SubscriptionNotFound("subscription %s does not exist" % handle.sub_id)
    if handle.generation != row["generation"]:
        raise StaleSubscriptionGeneration(
            "subscription %s handle is generation %s but the current generation is %s"
            % (handle.sub_id, handle.generation, row["generation"]))
    if not hmac.compare_digest(row["token_hash"], _hash_token(handle.token)):
        raise SubscriptionError("token does not match subscription %s" % handle.sub_id)
    return row


def _frozen_schema(store, tables):
    columns = store.column_types()
    out = {}
    for table in tables:
        if table not in columns:
            raise SchemaError("subscription names unknown table %r" % (table,))
        out[table] = cols = {}
        for name, ctype in columns[table].items():
            if ctype not in COLUMN_TYPES:
                raise SchemaError("table %s has unsupported column type %r"
                                  % (table, ctype))
            cols[name] = ctype
    return out


def _now():
    return time.time()


def create(client, name, tables):
    """Create a subscription and atomically fix its cut version S.

    The durable row is inserted in the *same* short write transaction that
    reads ``commit_version``, so no concurrent cleanup can slip between
    choosing S and taking retention responsibility for it.
    """
    if not isinstance(name, str) or not name.strip():
        raise SchemaError("subscription name must be a non-empty string")
    if len(name) > 200:
        raise SchemaError("subscription name is too long")
    if not isinstance(tables, (list, tuple)) or not tables:
        raise SchemaError("subscription needs a non-empty list of table names")
    if len(set(tables)) != len(tables):
        raise SchemaError("duplicate table in subscription")
    ordered = _sorted_tables(tables)
    frozen = _frozen_schema(client.store, ordered)
    sub_id = new_sub_id()
    token = secrets.token_hex(24)
    now = _now()

    def _create(conn):
        if conn.execute("SELECT 1 FROM subscriptions WHERE name=?", (name,)).fetchone():
            raise SubscriptionError("a subscription named %r already exists" % (name,))
        cut = Store.current_commit_version(conn)
        conn.execute(
            "INSERT INTO subscriptions(sub_id, name, state, generation, cut_version, "
            "tables_json, schema_json, token_hash, full_done, ack_version, "
            "full_cursor_table, full_cursor_row, last_acked_batch, last_acked_position, "
            "created_wall, updated_wall) "
            "VALUES (?, ?, ?, 1, ?, ?, ?, ?, 0, ?, ?, '', NULL, NULL, ?, ?)",
            (sub_id, name, ACTIVE, cut,
             json.dumps(ordered), json.dumps(frozen, sort_keys=True),
             _hash_token(token), cut, ordered[0], now, now))
        return cut
    cut = client.store.run_tx(_create)
    handle = SubscriptionHandle(sub_id, 1, token, client.db_path)
    return Subscription(client, handle), cut


def open_subscription(client, handle):
    """Re-open a persisted handle; resumes at its durable progress."""
    _check_source(handle, client)
    def _q(conn):
        return _check_handle(conn, handle)
    row = client.store.run_tx(_q, readonly=True)
    if row["state"] != ACTIVE:
        raise SubscriptionClosed(
            "subscription %s is closed; use rebuild_subscription to replace it"
            % handle.sub_id)
    return Subscription(client, handle)


def _check_source(handle, client):
    if not isinstance(handle, SubscriptionHandle):
        handle = SubscriptionHandle.from_dict(handle)
    if handle.db_path and os.path.realpath(handle.db_path) != os.path.realpath(
            client.db_path):
        raise SubscriptionError(
            "subscription handle belongs to source %r, not %r"
            % (handle.db_path, client.db_path))


def status(client, handle):
    _check_source(handle, client)
    def _q(conn):
        row = _check_handle(conn, handle)
        pending = conn.execute(
            "SELECT batch_id, phase, position, full_complete FROM subscription_pending "
            "WHERE sub_id=?", (handle.sub_id,)).fetchone()
        current = Store.current_commit_version(conn)
        return {
            "subscription_id": row["sub_id"],
            "name": row["name"],
            "state": row["state"],
            "generation": row["generation"],
            "cut_version": row["cut_version"],
            "tables": json.loads(row["tables_json"]),
            "schema": json.loads(row["schema_json"]),
            "phase": PHASE_INCREMENTAL if row["full_done"] else PHASE_FULL,
            "full_done": bool(row["full_done"]),
            "acknowledged_position": row["ack_version"],
            "last_acked_batch": row["last_acked_batch"],
            "last_acked_position": row["last_acked_position"],
            "pending_batch": pending["batch_id"] if pending else None,
            "pending_phase": pending["phase"] if pending else None,
            "pending_position": pending["position"] if pending else None,
            "pending_full_complete": bool(pending["full_complete"]) if pending else None,
            "current_source_position": current,
        }
    return client.store.run_tx(_q, readonly=True)


def list_subscriptions(client):
    def _q(conn):
        out = []
        for r in conn.execute("SELECT * FROM subscriptions ORDER BY name"):
            out.append({
                "subscription_id": r["sub_id"], "name": r["name"],
                "state": r["state"], "generation": r["generation"],
                "cut_version": r["cut_version"],
                "tables": json.loads(r["tables_json"]),
                "full_done": bool(r["full_done"]),
                "acknowledged_position": r["ack_version"],
            })
        return out
    return client.store.run_tx(_q, readonly=True)


def _materialize(conn, pending):
    manifest = json.loads(pending["manifest_json"])
    manifest["items"] = [
        json.loads(r[0]) for r in conn.execute(
            "SELECT item_json FROM subscription_items WHERE sub_id=? ORDER BY seq",
            (pending["sub_id"],))]
    return {"batch": manifest, "caught_up": False}


def _fetch_full_page(conn, table, columns, cut, after_row, limit):
    hist = hist_table(table)
    rows = conn.execute(
        'SELECT h.* FROM "%s" h JOIN ('
        ' SELECT row_id, MAX(version) AS mv FROM "%s" WHERE version <= ? GROUP BY row_id'
        ') v ON h.row_id = v.row_id AND h.version = v.mv '
        'WHERE h.deleted = 0 AND h.row_id > ? ORDER BY h.row_id LIMIT ?'
        % (hist, hist), (cut, after_row, limit)).fetchall()
    out = []
    for r in rows:
        out.append({"kind": "row", "table": table, "row_id": r["row_id"],
                    "version": r["version"], "present": True,
                    "values": _row_image(r, columns)})
    return out


def _build_full(conn, handle, row, max_rows):
    tables = json.loads(row["tables_json"])
    schema = json.loads(row["schema_json"])
    cut = row["cut_version"]
    start_table = row["full_cursor_table"] or tables[0]
    start_row = row["full_cursor_row"] or ""
    try:
        idx = tables.index(start_table)
    except ValueError:
        idx, start_row = 0, ""
    items = []
    remaining = max_rows
    first = True
    while remaining > 0 and idx < len(tables):
        table = tables[idx]
        after = start_row if first else ""
        first = False
        fetched = _fetch_full_page(conn, table, schema[table], cut, after, remaining + 1)
        take = fetched[:remaining]
        items.extend(take)
        if len(fetched) <= remaining:
            idx += 1
            start_row = ""
            remaining -= len(take)
        else:
            start_row = take[-1]["row_id"]
            remaining = 0
    full_complete = idx >= len(tables)
    cursor = None if full_complete else {"table": tables[idx], "row_id": start_row}
    batch_id = "b-" + secrets.token_hex(12)
    manifest = {
        "subscription_id": handle.sub_id,
        "generation": row["generation"],
        "batch_id": batch_id,
        "phase": PHASE_FULL,
        "from_position": cut,
        "position": cut,
        "full_complete": full_complete,
        "cursor": cursor,
        "row_count": len(items),
        "tables": [{"table": t, "columns": schema[t]} for t in tables],
    }
    _store_pending(conn, handle.sub_id, batch_id, row["generation"], PHASE_FULL,
                   cut, cut, 1 if full_complete else 0, manifest, items)
    manifest["items"] = items
    return {"batch": manifest, "caught_up": False}


def _build_incremental(conn, handle, row):
    tables = json.loads(row["tables_json"])
    schema = json.loads(row["schema_json"])
    ack = row["ack_version"]
    marks = ", ".join("?" for _ in tables)
    nxt = conn.execute(
        "SELECT MIN(commit_version) FROM sub_changes WHERE commit_version > ? "
        "AND table_name IN (%s)" % marks, (ack, *tables)).fetchone()[0]
    current = Store.current_commit_version(conn)
    if nxt is None:
        return {"batch": None, "caught_up": True,
                "acknowledged_position": ack,
                "current_source_position": current}
    changes = conn.execute(
        "SELECT * FROM sub_changes WHERE commit_version=? AND table_name IN (%s) "
        "ORDER BY id" % marks, (nxt, *tables)).fetchall()
    items = []
    for c in changes:
        # ``present`` is an explicit structural marker; an absent side has no
        # business values and no row version, which is distinct from a row
        # whose business values happen to be null.
        before = {"present": bool(c["old_present"]),
                  "version": c["old_version"] if c["old_present"] else None,
                  "values": json.loads(c["old_values"]) if c["old_present"] else None}
        after = {"present": bool(c["new_present"]),
                 "version": c["new_version"] if c["new_present"] else None,
                 "values": json.loads(c["new_values"]) if c["new_present"] else None}
        items.append({"kind": "change", "table": c["table_name"],
                      "row_id": c["row_id"], "before": before, "after": after})
    batch_id = "b-" + secrets.token_hex(12)
    manifest = {
        "subscription_id": handle.sub_id,
        "generation": row["generation"],
        "batch_id": batch_id,
        "phase": PHASE_INCREMENTAL,
        "from_position": ack,
        "position": nxt,
        "full_complete": False,
        "row_count": len(items),
        "tables": [{"table": t, "columns": schema[t]} for t in tables],
    }
    _store_pending(conn, handle.sub_id, batch_id, row["generation"], PHASE_INCREMENTAL,
                   ack, nxt, 0, manifest, items)
    manifest["items"] = items
    return {"batch": manifest, "caught_up": False}


def _store_pending(conn, sub_id, batch_id, generation, phase, from_position,
                   position, full_complete, manifest, items):
    manifest = dict(manifest)
    manifest.pop("items", None)
    conn.execute(
        "INSERT INTO subscription_pending(sub_id, batch_id, generation, phase, "
        "from_position, position, full_complete, manifest_json, created_wall) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (sub_id, batch_id, generation, phase, from_position, position,
         full_complete, json.dumps(manifest, sort_keys=True), _now()))
    conn.execute("DELETE FROM subscription_items WHERE sub_id=?", (sub_id,))
    for seq, item in enumerate(items):
        conn.execute("INSERT INTO subscription_items(sub_id, seq, item_json) "
                     "VALUES (?, ?, ?)", (sub_id, seq, json.dumps(item, sort_keys=True)))


def pull(client, handle, max_rows=100):
    """Return the one pending batch, or durably create the next one."""
    _check_source(handle, client)
    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 1:
        raise SchemaError("max_rows must be a positive integer")

    def _pull(conn):
        row = _check_handle(conn, handle)
        if row["state"] != ACTIVE:
            raise SubscriptionClosed(
                "subscription %s is closed; it cannot deliver batches" % handle.sub_id)
        pending = conn.execute("SELECT * FROM subscription_pending WHERE sub_id=?",
                               (handle.sub_id,)).fetchone()
        if pending is not None:
            # Same identity and business content, whatever the budget.
            return _materialize(conn, pending)
        if not row["full_done"]:
            return _build_full(conn, handle, row, max_rows)
        return _build_incremental(conn, handle, row)
    return client.store.run_tx(_pull)


def ack(client, handle, batch_id):
    """Acknowledge exactly the pending batch named by *batch_id*.

    No client-supplied position is accepted.  Repeating a successful
    acknowledgement is a no-op.
    """
    _check_source(handle, client)
    if not isinstance(batch_id, str) or not batch_id:
        raise SubscriptionError("batch_id must be a non-empty string")

    def _ack(conn):
        row = _check_handle(conn, handle)
        if row["state"] != ACTIVE:
            raise SubscriptionClosed(
                "subscription %s is closed; it cannot acknowledge batches"
                % handle.sub_id)
        pending = conn.execute("SELECT * FROM subscription_pending WHERE sub_id=?",
                               (handle.sub_id,)).fetchone()
        if pending is None:
            if row["last_acked_batch"] == batch_id:
                return {"subscription_id": handle.sub_id, "batch_id": batch_id,
                        "replayed": True,
                        "acknowledged_position": row["ack_version"],
                        "phase": None}
            raise SubscriptionError(
                "no pending batch for subscription %s; %r is unknown, stale or "
                "belongs to another subscription" % (handle.sub_id, batch_id))
        if not hmac.compare_digest(pending["batch_id"], batch_id):
            raise SubscriptionError(
                "batch %r is not the pending batch of subscription %s"
                % (batch_id, handle.sub_id))
        manifest = json.loads(pending["manifest_json"])
        if pending["generation"] != row["generation"]:
            # Defensive: a rebuild clears pending, this should not happen.
            raise StaleSubscriptionGeneration(
                "pending batch belongs to generation %s" % pending["generation"])
        now = _now()
        if pending["phase"] == PHASE_FULL:
            if manifest["full_complete"]:
                conn.execute(
                    "UPDATE subscriptions SET full_done=1, ack_version=?, "
                    "full_cursor_table=NULL, full_cursor_row=NULL, "
                    "last_acked_batch=?, last_acked_position=?, updated_wall=? "
                    "WHERE sub_id=?",
                    (manifest["position"], batch_id, manifest["position"], now,
                     handle.sub_id))
            else:
                cursor = manifest["cursor"]
                conn.execute(
                    "UPDATE subscriptions SET full_cursor_table=?, full_cursor_row=?, "
                    "last_acked_batch=?, last_acked_position=?, updated_wall=? "
                    "WHERE sub_id=?",
                    (cursor["table"], cursor["row_id"], batch_id,
                     manifest["position"], now, handle.sub_id))
        else:
            conn.execute(
                "UPDATE subscriptions SET ack_version=?, last_acked_batch=?, "
                "last_acked_position=?, updated_wall=? WHERE sub_id=?",
                (manifest["position"], batch_id, manifest["position"], now,
                 handle.sub_id))
        conn.execute("DELETE FROM subscription_pending WHERE sub_id=?", (handle.sub_id,))
        conn.execute("DELETE FROM subscription_items WHERE sub_id=?", (handle.sub_id,))
        return {"subscription_id": handle.sub_id, "batch_id": batch_id,
                "replayed": False, "phase": pending["phase"],
                "acknowledged_position": manifest["position"],
                "full_complete": bool(manifest["full_complete"])}
    return client.store.run_tx(_ack)


def close(client, handle):
    """Release this subscription's retention responsibility."""
    _check_source(handle, client)
    def _close(conn):
        row = _check_handle(conn, handle)
        if row["state"] == CLOSED:
            return {"subscription_id": handle.sub_id, "state": CLOSED,
                    "replayed": True}
        conn.execute("UPDATE subscriptions SET state=?, updated_wall=? WHERE sub_id=?",
                     (CLOSED, _now(), handle.sub_id))
        conn.execute("DELETE FROM subscription_pending WHERE sub_id=?", (handle.sub_id,))
        conn.execute("DELETE FROM subscription_items WHERE sub_id=?", (handle.sub_id,))
        return {"subscription_id": handle.sub_id, "state": CLOSED, "replayed": False}
    return client.store.run_tx(_close)


def rebuild(client, handle):
    """Explicitly replace the subscription: new cut, new generation, new token."""
    _check_source(handle, client)
    token = secrets.token_hex(24)
    def _rebuild(conn):
        row = _check_handle(conn, handle)
        generation = row["generation"] + 1
        cut = Store.current_commit_version(conn)
        tables = json.loads(row["tables_json"])
        conn.execute(
            "UPDATE subscriptions SET state=?, generation=?, cut_version=?, "
            "token_hash=?, full_done=0, ack_version=?, full_cursor_table=?, "
            "full_cursor_row='', last_acked_batch=NULL, last_acked_position=NULL, "
            "updated_wall=? WHERE sub_id=?",
            (ACTIVE, generation, cut, _hash_token(token), cut, tables[0],
             _now(), handle.sub_id))
        conn.execute("DELETE FROM subscription_pending WHERE sub_id=?", (handle.sub_id,))
        conn.execute("DELETE FROM subscription_items WHERE sub_id=?", (handle.sub_id,))
        return cut, generation
    cut, generation = client.store.run_tx(_rebuild)
    new_handle = SubscriptionHandle(handle.sub_id, generation, token, client.db_path)
    return Subscription(client, new_handle), cut


# ---------------------------------------------------------------------------
# Retention helpers used by the transaction engine
# ---------------------------------------------------------------------------


def watched_tables(conn):
    """Union of tables covered by at least one *open* subscription.

    Called inside the commit transaction; closed subscriptions have already
    released their retention responsibility, so they contribute nothing.
    """
    out = set()
    for (tables_json,) in conn.execute(
            "SELECT tables_json FROM subscriptions WHERE state=?", (ACTIVE,)):
        out.update(json.loads(tables_json))
    return out


def full_phase_snapshots(conn):
    """Cut versions S that still need row history for their full load."""
    return [int(r[0]) for r in conn.execute(
        "SELECT cut_version FROM subscriptions WHERE state=? AND full_done=0",
        (ACTIVE,))]


def retention_boundary(conn):
    """Highest sub_changes position still needed by any open subscription.

    Every open subscription needs changes strictly after its acknowledged
    position.  The slowest one bounds the log: positions ``<= min(ack)`` can
    never be delivered by anyone, so they may be reclaimed.  Returns ``None``
    when no subscription is open (the whole log is reclaimable) and the
    current source position otherwise as the reported retention boundary.
    """
    rows = [int(r[0]) for r in conn.execute(
        "SELECT ack_version FROM subscriptions WHERE state=?", (ACTIVE,))]
    if not rows:
        return None
    return min(rows)


def cleanup_sync_history(client):
    """Reclaim change-log entries that no open subscription can still need.

    Also drops the private pending copies of closed subscriptions.  Returns a
    report with the number of reclaimed change rows and the retention
    boundary actually honoured; it is not a "never delete" policy.
    """
    def _clean(conn):
        boundary = retention_boundary(conn)
        current = Store.current_commit_version(conn)
        if boundary is None:
            reclaimed = conn.execute("DELETE FROM sub_changes").rowcount
            boundary_reported = current
        else:
            reclaimed = conn.execute(
                "DELETE FROM sub_changes WHERE commit_version <= ?", (boundary,)
            ).rowcount
            boundary_reported = boundary
        # Private pending copies of closed subscriptions are garbage.
        closed = [r[0] for r in conn.execute(
            "SELECT sub_id FROM subscriptions WHERE state=?", (CLOSED,))]
        released = 0
        for sub_id in closed:
            released += conn.execute(
                "DELETE FROM subscription_pending WHERE sub_id=?", (sub_id,)).rowcount
            released += conn.execute(
                "DELETE FROM subscription_items WHERE sub_id=?", (sub_id,)).rowcount
        remaining = conn.execute("SELECT COUNT(*) FROM sub_changes").fetchone()[0]
        return {"reclaimed_changes": reclaimed,
                "retention_boundary": boundary_reported,
                "current_source_position": current,
                "remaining_changes": remaining,
                "released_pending_rows": released,
                "open_subscriptions": conn.execute(
                    "SELECT COUNT(*) FROM subscriptions WHERE state=?",
                    (ACTIVE,)).fetchone()[0]}
    return client.store.run_tx(_clean)
