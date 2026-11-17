"""A real SQLite replica that consumes a change subscription.

The replica is a *separate* SQLite database.  It is deliberately not
distributed-transaction magic: the source and replica can never commit one
atomic transaction across two files.  Instead delivery is at-least-once and
the replica applies each batch in exactly one local transaction that includes
both the data changes and a durable record of the batch id.  A crash between
"locally applied" and "source acknowledged" therefore causes the same batch to
be pulled again, and the replica recognises it and skips re-applying it.

Replica state machine
=====================
``receiving``
    The full load is still streaming.  Business reads are refused.  Rows are
    staged into the replica tables as pages arrive.

``ready``
    The final full page was applied.  Business reads are allowed.  Every later
    source commit is applied atomically in one local transaction.

Generation changes
==================
A subscription rebuild changes the cut version.  A replica that sees a
different subscription generation must not mix the old full load with the new
one.  :meth:`Replica.consume` raises :class:`ReplicaGenerationError` unless the
caller explicitly re-initialises the replica.  A fresh replica database (or
``reinitialize=True``) starts a clean ``receiving`` state for the new
generation.
"""

import json
import os
import sqlite3

from .errors import (ReplicaError, ReplicaGenerationError, ReplicaNotReady,
                     SchemaError, StaleSubscriptionGeneration)
from .values import COLUMN_TYPES, TYPE_BOOL

_RECEIVING = "receiving"
_READY = "ready"

_REPLICA_DDL = [
    "CREATE TABLE IF NOT EXISTS replica_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS replica_applied (batch_id TEXT PRIMARY KEY, "
    "generation INTEGER NOT NULL, position INTEGER NOT NULL, phase TEXT NOT NULL, "
    "applied_wall REAL NOT NULL)",
]


class Replica:
    """A separate SQLite database populated from one subscription."""

    def __init__(self, path, source_handle=None):
        self.path = os.path.abspath(path)
        handle = source_handle
        if handle is not None and not hasattr(handle, "as_dict"):
            from .subscription import SubscriptionHandle
            handle = SubscriptionHandle.from_dict(handle)
        self.handle = handle
        parent = os.path.dirname(self.path)
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        self._init_db()

    # -- local storage -------------------------------------------------------

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    def _init_db(self):
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("BEGIN IMMEDIATE")
            for ddl in _REPLICA_DDL:
                conn.execute(ddl)
            conn.execute("COMMIT")
        finally:
            conn.close()

    def _meta(self, conn, key, default=None):
        row = conn.execute("SELECT value FROM replica_meta WHERE key=?",
                           (key,)).fetchone()
        return default if row is None else row[0]

    def _set_meta(self, conn, key, value):
        conn.execute("INSERT INTO replica_meta(key, value) VALUES (?, ?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (key, value))

    def _tables(self, conn):
        return json.loads(self._meta(conn, "tables_json", "{}"))

    def _ensure_data_tables(self, conn, tables, schema, reset):
        for table, columns in schema.items():
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,)).fetchone()
            if exists and not reset:
                continue
            if exists:
                conn.execute('DROP TABLE "%s"' % table)
            defs = ["row_id TEXT PRIMARY KEY", "version INTEGER NOT NULL"]
            for name, ctype in columns.items():
                decl = {"int64": "INTEGER", "text": "TEXT", "bool": "INTEGER",
                        "null": ""}[ctype]
                defs.append('"%s" %s' % (name, decl) if decl else '"%s"' % name)
            conn.execute('CREATE TABLE "%s" (%s)' % (table, ", ".join(defs)))

    @staticmethod
    def _convert(value, ctype):
        if value is None:
            return None
        if ctype == TYPE_BOOL:
            return 1 if value else 0
        return value

    def _apply_row(self, conn, table, columns, row_id, version, values):
        # The batch-id de-duplication in apply_local already prevents replays,
        # but never let a stale image regress a newer version in the local
        # replica even if a caller re-applied a batch manually.
        cur = conn.execute('SELECT version FROM "%s" WHERE row_id=?' % table,
                           (row_id,)).fetchone()
        if cur is not None and cur[0] > version:
            return
        names = list(columns)
        conn.execute('DELETE FROM "%s" WHERE row_id=?' % table, (row_id,))
        cols = ["row_id", "version"] + ['"%s"' % n for n in names]
        marks = ", ".join("?" for _ in cols)
        args = [row_id, version] + [self._convert(values[n], columns[n]) for n in names]
        conn.execute('INSERT INTO "%s" (%s) VALUES (%s)' % (table, ", ".join(cols), marks),
                     args)

    def _delete_row(self, conn, table, row_id):
        conn.execute('DELETE FROM "%s" WHERE row_id=?' % table, (row_id,))

    # -- identity / generation ----------------------------------------------

    def prepare_generation(self, handle):
        """Explicitly discard the local replica and begin a clean load."""
        with self._tx() as conn:
            self._reset_for(conn, handle)

    def _reset_for(self, conn, handle):
        for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT IN ('replica_meta', 'replica_applied')").fetchall():
            conn.execute('DROP TABLE "%s"' % name)
        conn.execute("DELETE FROM replica_applied")
        conn.execute("DELETE FROM replica_meta")
        self._set_meta(conn, "subscription_id", handle.sub_id)
        self._set_meta(conn, "generation", str(handle.generation))
        self._set_meta(conn, "source", handle.db_path or "")
        self._set_meta(conn, "state", _RECEIVING)
        self._set_meta(conn, "tables_json", "{}")
        self._set_meta(conn, "schema_json", "{}")
        self._set_meta(conn, "position", "0")

    class _Tx:
        def __init__(self, replica):
            self.replica = replica
            self.conn = None

        def __enter__(self):
            self.conn = self.replica._connect()
            self.conn.execute("BEGIN IMMEDIATE")
            return self.conn

        def __exit__(self, exc_type, exc, tb):
            try:
                if exc_type is None:
                    self.conn.execute("COMMIT")
                else:
                    self.conn.execute("ROLLBACK")
            finally:
                self.conn.close()
            return False

    def _tx(self):
        return Replica._Tx(self)

    # -- consuming -----------------------------------------------------------

    def consume(self, client, max_rows=200, reinitialize=False):
        """Pull, apply locally, then acknowledge one batch.

        Equivalent to :meth:`apply_local` followed by :meth:`acknowledge`.
        The split exists so a caller (or a drill) can simulate a crash after
        the local transaction but before the source acknowledgement.
        """
        batch = self.apply_local(client, max_rows=max_rows,
                                 reinitialize=reinitialize)
        if batch is None:
            report = self._report(caught_up=True)
            report["applied"] = False
            return report
        ack = self.acknowledge(client)
        batch["acknowledged_position"] = ack["acknowledged_position"]
        batch["ack_replayed"] = ack["replayed"]
        return batch

    def apply_local(self, client, max_rows=200, reinitialize=False):
        """Pull one batch and apply it in a single local transaction.

        Returns a report, or ``None`` when the subscription is caught up.
        The batch id is recorded together with the data.  The source is *not*
        acknowledged here.
        """
        handle = self.handle
        if handle is None:
            raise ReplicaError("replica has no subscription handle")
        result = client.pull(handle, max_rows=max_rows)
        batch = result["batch"]
        if batch is None:
            return None

        if batch["generation"] != handle.generation:
            raise StaleSubscriptionGeneration(
                "batch %s is from generation %s, handle is generation %s"
                % (batch["batch_id"], batch["generation"], handle.generation))

        with self._tx() as conn:
            bound_id = self._meta(conn, "subscription_id")
            bound_gen = self._meta(conn, "generation")
            stale = (bound_id is not None
                     and (bound_id != handle.sub_id
                          or int(bound_gen) != handle.generation))
            if reinitialize or bound_id is None or stale:
                if stale and not reinitialize:
                    raise ReplicaGenerationError(
                        "replica holds subscription %s generation %s; the batch "
                        "belongs to generation %s.  Reinitialise explicitly."
                        % (bound_id, bound_gen, handle.generation))
                self._reset_for(conn, handle)
            state = self._meta(conn, "state", _RECEIVING)

            seen = conn.execute("SELECT 1 FROM replica_applied WHERE batch_id=?",
                                (batch["batch_id"],)).fetchone()
            applied = False
            if seen is None:
                schema = {spec["table"]: spec["columns"] for spec in batch["tables"]}
                if state == _RECEIVING:
                    # The full load defines the data tables, including empty
                    # subscribed tables (they still exist, with the frozen
                    # column types).
                    self._ensure_data_tables(conn, batch["tables"], schema,
                                             reset=False)
                    self._set_meta(conn, "tables_json", json.dumps(sorted(schema)))
                    self._set_meta(conn, "schema_json",
                                   json.dumps(schema, sort_keys=True))
                if batch["phase"] == "full":
                    for item in batch["items"]:
                        if item["present"]:
                            self._apply_row(conn, item["table"], schema[item["table"]],
                                            item["row_id"], item["version"],
                                            item["values"])
                        else:  # pragma: no cover - full pages contain only present rows
                            self._delete_row(conn, item["table"], item["row_id"])
                    if batch["full_complete"]:
                        self._set_meta(conn, "state", _READY)
                else:
                    for item in batch["items"]:
                        after = item["after"]
                        if after["present"]:
                            self._apply_row(conn, item["table"], schema[item["table"]],
                                            item["row_id"], after["version"],
                                            after["values"])
                        else:
                            self._delete_row(conn, item["table"], item["row_id"])
                self._set_meta(conn, "position", str(batch["position"]))
                conn.execute(
                    "INSERT INTO replica_applied(batch_id, generation, position, phase, "
                    "applied_wall) VALUES (?, ?, ?, ?, strftime('%s','now'))",
                    (batch["batch_id"], batch["generation"], batch["position"],
                     batch["phase"]))
                applied = True
            state_after = self._meta(conn, "state", _RECEIVING)

        self._last_batch = {
            "batch_id": batch["batch_id"],
            "phase": batch["phase"],
            "generation": batch["generation"],
            "position": batch["position"],
            "full_complete": bool(batch["full_complete"]),
            "row_count": batch["row_count"],
            "applied": applied,
            "replayed": not applied,
            "state": state_after,
            "ready": state_after == _READY,
            "caught_up": False,
        }
        return self._last_batch

    def acknowledge(self, client):
        """Acknowledge the batch most recently applied locally."""
        handle = self.handle
        batch = getattr(self, "_last_batch", None)
        if batch is None or handle is None:
            raise ReplicaError("nothing to acknowledge")
        ack = client.ack(handle, batch["batch_id"])
        return {"batch_id": batch["batch_id"],
                "acknowledged_position": ack["acknowledged_position"],
                "replayed": ack.get("replayed", False)}

    def _report(self, caught_up=False):
        with self._tx() as conn:
            state = self._meta(conn, "state", _RECEIVING)
            return {"state": state, "ready": state == _READY,
                    "position": int(self._meta(conn, "position", "0")),
                    "generation": self._meta(conn, "generation"),
                    "caught_up": caught_up}

    def run(self, client, max_rows=200, max_batches=100000):
        """Consume until caught up.  Returns the last report."""
        report = self._report()
        for _ in range(max_batches):
            report = self.consume(client, max_rows=max_rows)
            if report.get("caught_up"):
                return report
        return report

    # -- business reads ------------------------------------------------------

    def _require_ready(self, conn):
        if self._meta(conn, "state", _RECEIVING) != _READY:
            raise ReplicaNotReady(
                "replica %s is still receiving its full load; business reads are "
                "not allowed until the final full page was applied" % self.path)

    def rows(self, table):
        """Business read of a replica table (only after the full load)."""
        conn = self._connect()
        try:
            self._require_ready(conn)
            schema = json.loads(self._meta(conn, "schema_json", "{}"))
            if table not in schema:
                raise SchemaError("replica has no subscribed table %r" % (table,))
            columns = schema[table]
            conn.execute("BEGIN")
            out = []
            for r in conn.execute('SELECT * FROM "%s" ORDER BY row_id' % table):
                row = {"row_id": r["row_id"], "version": r["version"]}
                for name, ctype in columns.items():
                    value = r[name]
                    if ctype == TYPE_BOOL and value is not None:
                        value = bool(value)
                    row[name] = value
                out.append(row)
            conn.execute("COMMIT")
            return out
        finally:
            conn.close()

    def tables(self):
        conn = self._connect()
        try:
            self._require_ready(conn)
            return json.loads(self._meta(conn, "schema_json", "{}"))
        finally:
            conn.close()

    def state(self):
        conn = self._connect()
        try:
            state = self._meta(conn, "state", _RECEIVING)
            return {
                "state": state,
                "ready": state == _READY,
                "subscription_id": self._meta(conn, "subscription_id"),
                "generation": self._meta(conn, "generation"),
                "position": int(self._meta(conn, "position", "0")),
                "tables": json.loads(self._meta(conn, "tables_json", "{}")),
                "applied_batches": conn.execute(
                    "SELECT COUNT(*) FROM replica_applied").fetchone()[0],
            }
        finally:
            conn.close()

    def applied_batches(self):
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM replica_applied ORDER BY rowid")]
        finally:
            conn.close()
