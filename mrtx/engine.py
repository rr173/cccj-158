"""Managed repository transaction engine.

Isolation
=========
``begin`` fixes a transaction's start view: ``base_version``, the committed
version counter at that moment.  (It is captured in one short SQLite write
transaction; no lock is held afterwards.)  A read materializes, per stable
``row_id``, the newest history entry with ``version <= base_version`` and then
overlays the transaction's own staged writes/deletes.  A transaction therefore
sees a fixed committed snapshot plus its own writes ("read your writes"), and
two user transactions can be open in different processes simultaneously
without either holding a database write lock while its user thinks.

Serializability criterion
=========================
We serialize committed transactions in **commit order**.  Let s(T) be T's
start version.  When T commits, it validates its read set against every write
committed at a version in ``(s(T), current]``:

* **Same identity (ww / precondition).**  If another transaction committed any
  change to an identity T writes, T is rejected: a write may not silently
  overwrite a committed change to the same identity.  Explicit
  ``expected_version`` preconditions are checked the same way.
* **Read invalidation (rw).**  For every read T performed (returned rows *and*
  its predicate over an empty or non-empty result), if an earlier commit
  changed a returned row in a column T projected or filtered on, or changed the
  predicate's truth value for any row (a phantom, disappearance, or new
  match), T is rejected.  This is checked across *all* tables T read, so a
  cross-table read-then-write dependency is caught.

This is the classical backward-validation rule for optimistic concurrency and
it is both sound and complete for commit-order serializability: all conflict
edges of the committed history point forward in commit order, so the history
equals its commit-order serial execution.  The rule is *not* "write sets
intersect": two transactions that read the same predicate but write different
rows conflict through read invalidation, while two transactions on the same
start view whose reads and writes concern disjoint key ranges have no edge and
both commit directly, with no replay.

Every rejection carries structured evidence: the actual transactions, the
identity or the predicate, the returned rows, and the dependency kind -- never
a bare "stale version".  ``dependency_graph``/``find_cycles`` expose the same
evidence as a graph for auditing three-way rings.

Durability
==========
``commit`` runs one short SQLite transaction that validates, appends the new
row versions, writes the write evidence, records the durable decision and
flips the transaction state.  A crash therefore leaves either the whole commit
or nothing; ``commit`` is idempotent per transaction id.  A new process can
always answer "committed or not" for a transaction id with
``transaction_status``.  SQLite ``busy``/``locked`` is retried and, if it
persists, raised as ``BusyError`` -- it is never a conflict decision.
"""

import hashlib
import hmac
import json
import os
import secrets
import time

from . import canonical
from . import references as refs_mod
from . import subscription as subscription_mod
from .errors import (ConflictError, SchemaConflictError, SchemaError,
                     TransactionError, TransactionNotFound)
from .store import Store, hist_table
from .values import (TYPE_BOOL, check_value, columns_used, compile_predicate,
                     returns_true)

ACTIVE = "active"
COMMITTED = "committed"
REJECTED = "rejected"
ABORTED = "aborted"
ABORTED_ORPHAN = "aborted_orphan"


def new_txid():
    return "tx-" + secrets.token_hex(12)


def _hash_token(token):
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _row_dict(row, columns):
    out = {}
    for name, ctype in columns.items():
        value = row[name]
        if ctype == TYPE_BOOL and value is not None:
            value = bool(value)
        out[name] = value
    return out


def _changed_columns(old_values, new_values):
    if old_values is None and new_values is None:
        return set()
    keys = set(old_values or {}) | set(new_values or {})
    return {k for k in keys if (old_values or {}).get(k) != (new_values or {}).get(k)}


def _predicate_holds(pred, row_id, values):
    """Truth of *pred* for an identity image; an absent row (None) is FALSE.

    This matters for deletes: a predicate such as ``row_id = "A"`` must stop
    holding once A is deleted, and a new insert is FALSE in the old state.
    """
    if values is None:
        return False
    row = dict(values)
    row["row_id"] = row_id
    return returns_true(pred, row)


def _proc_starttime(pid):
    """Process start time from /proc, so PID reuse cannot fake liveness."""
    try:
        with open("/proc/%d/stat" % pid, "r") as fh:
            data = fh.read()
        return data.rsplit(")", 1)[1].split()[19]
    except Exception:
        return None


class TransactionHandle:
    """Picklable, process-independent reference.  The token proves ownership."""

    __slots__ = ("txid", "token", "db_path")

    def __init__(self, txid, token, db_path):
        self.txid = txid
        self.token = token
        self.db_path = db_path

    def __repr__(self):
        return "TransactionHandle(%r, db=%r)" % (self.txid, self.db_path)

    def as_dict(self):
        return {"txid": self.txid, "token": self.token, "db_path": self.db_path}

    @classmethod
    def from_dict(cls, obj):
        return cls(obj["txid"], obj["token"], obj["db_path"])


class Transaction:
    """Client-side handle.  Not thread-safe; use one per thread/process."""

    def __init__(self, client, handle):
        self._client = client
        self.handle = handle
        self.txid = handle.txid
        self.closed = False

    def read(self, table, columns=None, where=None):
        self._alive()
        return self._client.read(self.handle, table, columns=columns, where=where)

    def stage_insert(self, table, row_id, values):
        self._alive()
        return self._client.stage_insert(self.handle, table, row_id, values)

    def stage_update(self, table, row_id, values, expected_version=None):
        self._alive()
        return self._client.stage_update(self.handle, table, row_id, values,
                                         expected_version=expected_version)

    def stage_delete(self, table, row_id, expected_version=None):
        self._alive()
        return self._client.stage_delete(self.handle, table, row_id,
                                         expected_version=expected_version)

    def commit(self):
        self._alive()
        result = self._client.commit(self.handle)
        self.closed = True
        return result

    def abort(self):
        self._alive()
        result = self._client.abort(self.handle)
        self.closed = True
        return result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if not self.closed:
            if exc_type is None:
                self.commit()
            else:
                try:
                    self.abort()
                except Exception:
                    pass
        return False

    def _alive(self):
        if self.closed:
            raise TransactionError("transaction %s is already closed" % self.txid)


class Client:
    """Public API: begin, read, stage_insert/update/delete, commit, abort."""

    def __init__(self, db_path, lease_seconds=60.0, recover_on_start=True,
                 busy_timeout_ms=3000):
        self.store = Store(db_path, busy_timeout_ms=busy_timeout_ms)
        self.db_path = self.store.path
        self.lease_seconds = lease_seconds
        if recover_on_start:
            self.recover()

    # -- catalog -------------------------------------------------------------

    def create_table(self, table, columns):
        return self.store.create_table(table, columns)

    def tables(self):
        return self.store.column_types()

    def add_column(self, table, column, ctype, default):
        """Add one column to a managed table while the repository stays online.

        Parameters
        ----------
        table : str
            An existing managed table.
        column : str
            The new column name (safe identifier; must not already exist and
            must not be ``row_id``).
        ctype : str
            One of the supported domains (``int64`` / ``text`` / ``bool`` /
            ``null``).
        default :
            A value valid for *ctype* (``None`` is allowed for every domain and
            is the only value allowed for ``null``).  Every row committed before
            the migration reads this default without being rewritten, and later
            inserts that omit the column receive the same default.

        Returns a durable record including the ordered source position
        (``commit_version``) at which the migration took effect.  Raises
        :class:`SchemaError` (with no side effects) for an unknown table, a
        conflicting column name or an ill-typed default.
        """
        return self.store.add_column(table, column, ctype, default)

    def schema_history(self, table=None):
        """Ordered structural-change records (online migrations) from the source.

        Each record is one entry of the same ordered history that carries row
        commits.  Returns every table's changes when *table* is omitted.
        """
        def _q(conn):
            if table is not None:
                if conn.execute("SELECT 1 FROM tables WHERE table_name=?",
                                (table,)).fetchone() is None:
                    raise SchemaError("unknown table %r" % (table,))
                sql = ("SELECT commit_version, table_name, schema_version, change_type, "
                       "column_name, column_type, has_default, default_json, "
                       "after_columns_json FROM schema_changes WHERE table_name=? "
                       "ORDER BY commit_version, id")
                params = (table,)
            else:
                sql = ("SELECT commit_version, table_name, schema_version, change_type, "
                       "column_name, column_type, has_default, default_json, "
                       "after_columns_json FROM schema_changes "
                       "ORDER BY commit_version, id")
                params = ()
            out = []
            for r in conn.execute(sql, params):
                out.append({
                    "commit_version": r["commit_version"], "table": r["table_name"],
                    "schema_version": r["schema_version"], "change_type": r["change_type"],
                    "column": r["column_name"], "type": r["column_type"],
                    "has_default": bool(r["has_default"]),
                    "default": json.loads(r["default_json"]),
                    "after_columns": [c["name"] for c in
                                      json.loads(r["after_columns_json"])]})
            return out
        return self.store.run_tx(_q, readonly=True)

    def table_schemas(self, table):
        """Every immutable schema version of *table* with its effective position."""
        def _q(conn):
            if conn.execute("SELECT 1 FROM tables WHERE table_name=?",
                            (table,)).fetchone() is None:
                raise SchemaError("unknown table %r" % (table,))
            out = []
            for r in conn.execute(
                    "SELECT schema_version, effective_version, columns_json FROM table_schemas "
                    "WHERE table_name=? ORDER BY schema_version", (table,)):
                out.append({"schema_version": r["schema_version"],
                            "effective_version": r["effective_version"],
                            "columns": json.loads(r["columns_json"])})
            return out
        return self.store.run_tx(_q, readonly=True)

    # -- managed reference constraints --------------------------------------

    def register_reference(self, name, parent_table, child_table, child_column,
                           on_delete):
        """Declare a durable managed reference constraint.

        ``child_table.child_column`` (a ``text`` column) references
        ``parent_table.row_id``; ``on_delete`` is ``"restrict"`` or
        ``"cascade"``.  Business ``None`` is "no reference".  Registration
        scans the already-committed data and is refused (with no half
        definition) on an existing orphan, a duplicate name or column binding,
        an unknown table/column, or a non-text column.  The rule is durable and
        applies to every later commit, including write transactions already in
        flight when the registration lands.
        """
        return refs_mod.register(self, name, parent_table, child_table,
                                 child_column, on_delete)

    def list_references(self):
        """Every durable reference constraint, ordered by its unique name."""
        return refs_mod.list_registered(self)

    def get_reference(self, name):
        """One durable constraint record, or ``None`` when the name is unknown."""
        return refs_mod.get_constraint(self, name)

    # -- begin ---------------------------------------------------------------

    def begin(self, txid=None):
        txid = txid or new_txid()
        token = secrets.token_hex(16)
        now = time.time()
        pid = os.getpid()
        starttime = _proc_starttime(pid)

        def _begin(conn):
            if conn.execute("SELECT 1 FROM transactions WHERE user_txid=?",
                            (txid,)).fetchone():
                raise TransactionError("transaction id %s already exists" % txid)
            base_version = Store.current_commit_version(conn)
            conn.execute(
                "INSERT INTO transactions(user_txid, state, base_version, token_hash, "
                "owner_pid, owner_start, start_wall, commit_version, local_counter) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 0)",
                (txid, ACTIVE, base_version, _hash_token(token), pid, starttime, now))
            conn.execute("INSERT OR REPLACE INTO gc_leases(user_txid, expires_wall) "
                         "VALUES (?, ?)", (txid, now + self.lease_seconds))
            return base_version
        self.store.run_tx(_begin)
        return Transaction(self, TransactionHandle(txid, token, self.db_path))

    # -- authentication ------------------------------------------------------

    def _load(self, conn, handle, need_active=False):
        row = conn.execute(
            "SELECT state, base_version, token_hash FROM transactions "
            "WHERE user_txid=?", (handle.txid,)).fetchone()
        if row is None:
            raise TransactionNotFound("transaction %s does not exist" % handle.txid)
        if not hmac.compare_digest(row["token_hash"], _hash_token(handle.token)):
            raise TransactionError("token does not match transaction %s" % handle.txid)
        if need_active and row["state"] != ACTIVE:
            raise TransactionError(
                "transaction %s is %s, not active; it cannot accept new operations"
                % (handle.txid, row["state"]))
        return row

    # -- materialized views --------------------------------------------------

    def _visible_rows(self, conn, table, version):
        """[(row_id, version, values)] visible at *version*, ordered by row_id.

        The values use the *column definition effective at* *version*: an old
        snapshot never sees columns added by a later online migration, while a
        new snapshot reads the added column's default straight out of the
        physical default on existing history rows (no row rewrite).
        """
        hist = hist_table(table)
        columns, _defaults = self.store.schema_at_conn(conn, table, version)
        rows = conn.execute(
            'SELECT h.* FROM "%s" h JOIN (SELECT row_id, MAX(version) AS mv FROM "%s" '
            'WHERE version <= ? GROUP BY row_id) v '
            'ON h.row_id = v.row_id AND h.version = v.mv ORDER BY h.row_id'
            % (hist, hist), (version,)).fetchall()
        out = []
        for row in rows:
            if row["deleted"]:
                continue
            out.append((row["row_id"], row["version"], _row_dict(row, columns)))
        return out

    @staticmethod
    def _staged_ids(conn, txid, table):
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT row_id FROM staged WHERE user_txid=? AND table_name=? "
            "ORDER BY row_id", (txid, table))]

    @staticmethod
    def _staged_effective(conn, txid, table, row_id):
        """Fold all staged ops on one identity into one effective, atomic change."""
        rows = conn.execute(
            "SELECT op, old_values, new_values, base_version, base_present, expected_version, "
            "result_version FROM staged WHERE user_txid=? AND table_name=? AND row_id=? "
            "ORDER BY seq", (txid, table, row_id)).fetchall()
        if not rows:
            return None
        first, last = rows[0], rows[-1]
        had_insert = any(r["op"] == "insert" for r in rows)
        had_delete = any(r["op"] == "delete" for r in rows)
        old_values = (canonical.values_from_json(first["old_values"])
                      if first["old_values"] is not None else None)
        if last["op"] == "delete":
            effective_op = "delete"
            new_values = None
        elif had_insert:
            effective_op = "insert"
            new_values = canonical.values_from_json(last["new_values"])
        else:
            effective_op = "update"
            new_values = canonical.values_from_json(last["new_values"])
        return {"op": effective_op, "old_values": old_values, "new_values": new_values,
                "base_version": first["base_version"],
                "base_present": bool(first["base_present"]),
                "expected_version": first["expected_version"],
                "result_version": last["result_version"],
                "had_insert": had_insert, "had_delete": had_delete}

    def _staged_for_table(self, conn, txid, table):
        return [(rid, self._staged_effective(conn, txid, table, rid))
                for rid in self._staged_ids(conn, txid, table)]

    def _view(self, conn, txid, table, version):
        """Identity -> {version, values} for snapshot *version* plus own staging."""
        view = {rid: {"version": ver, "values": vals}
                for rid, ver, vals in self._visible_rows(conn, table, version)}
        for rid, eff in self._staged_for_table(conn, txid, table):
            if eff["op"] == "delete":
                view.pop(rid, None)
            else:
                view[rid] = {"version": eff["result_version"],
                             "values": eff["new_values"]}
        return view

    # -- read ----------------------------------------------------------------

    def read(self, handle, table, columns=None, where=None):
        def _read(conn):
            tx = self._load(conn, handle, need_active=True)
            version = tx["base_version"]
            # Resolve the column definition fixed at the transaction's start
            # view.  A read never sees a column added by a later migration.
            columns_map, _defaults = self.store.schema_at_conn(conn, table, version)
            pred = compile_predicate(where, columns_map)
            if columns is None:
                selected = list(columns_map)
            else:
                if not isinstance(columns, (list, tuple)) or not columns:
                    raise SchemaError("columns must be a non-empty list of whitelisted names")
                selected = []
                for name in columns:
                    if name != "row_id" and name not in columns_map:
                        raise SchemaError("unknown column %r on %s" % (name, table))
                    if name not in selected:
                        selected.append(name)

            view = self._view(conn, handle.txid, table, version)
            out = []
            for rid in sorted(view):
                entry = view[rid]
                if not _predicate_holds(pred, rid, entry["values"]):
                    continue
                row = {"row_id": rid, "version": entry["version"]}
                for name in selected:
                    if name != "row_id":
                        row[name] = entry["values"][name]
                out.append(row)
            record = {"table": table,
                      "predicate": canonical.predicate_text(pred),
                      "description": canonical.describe(pred),
                      "columns": selected,
                      "returned": out,
                      "empty": not out,
                      "snapshot_version": version}
            conn.execute(
                "INSERT INTO predicate_reads(user_txid, table_name, predicate_json, "
                "description, rows_json, columns_json, returns_empty, snapshot_version, "
                "read_wall) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (handle.txid, table, record["predicate"],
                 json.dumps(record["description"], sort_keys=True),
                 canonical.values_to_json(out), json.dumps(selected),
                 1 if not out else 0, version, time.time()))
            self._log_op(conn, handle.txid, "read", table, record)
            self._touch_lease(conn, handle.txid)
            return out
        return self.store.run_tx(_read)

    @staticmethod
    def _log_op(conn, txid, kind, table, detail):
        row = conn.execute("SELECT COALESCE(MAX(ord), 0) + 1 FROM tx_ops WHERE user_txid=?",
                           (txid,)).fetchone()
        conn.execute("INSERT INTO tx_ops(user_txid, ord, kind, table_name, detail_json, "
                     "at_wall) VALUES (?, ?, ?, ?, ?, ?)",
                     (txid, row[0], kind, table,
                      json.dumps(detail, sort_keys=True), time.time()))

    def _touch_lease(self, conn, txid):
        conn.execute("UPDATE gc_leases SET expires_wall=? WHERE user_txid=?",
                     (time.time() + self.lease_seconds, txid))

    # -- staging -------------------------------------------------------------

    def stage_insert(self, handle, table, row_id, values):
        return self._stage(handle, table, row_id, values, None, "insert")

    def stage_update(self, handle, table, row_id, values, expected_version=None):
        return self._stage(handle, table, row_id, values, expected_version, "update")

    def stage_delete(self, handle, table, row_id, expected_version=None):
        return self._stage(handle, table, row_id, None, expected_version, "delete")

    def _stage(self, handle, table, row_id, values, expected_version, kind):
        if not isinstance(row_id, str) or not row_id:
            raise SchemaError("row_id must be a non-empty string")
        if expected_version is not None and (
                isinstance(expected_version, bool) or not isinstance(expected_version, int)
                or expected_version < 0):
            raise SchemaError("expected_version must be a non-negative integer")
        if kind in ("insert", "update"):
            if not isinstance(values, dict):
                raise SchemaError("%s requires a values dict" % kind)

        def _do(conn):
            self._load(conn, handle, need_active=True)
            base_version = self._load(conn, handle)["base_version"]
            # The transaction's structural start view: the column definition
            # (and declared defaults) fixed at begin time.
            columns_map, default_map = self.store.schema_at_conn(
                conn, table, base_version)
            live_types, _live_defaults, _cols = self.store.latest_schema_conn(
                conn, table)
            if kind in ("insert", "update"):
                unknown = set(values) - set(columns_map)
                if unknown:
                    raise SchemaError("unknown columns on %s: %s" % (table, sorted(unknown)))
                if kind == "insert":
                    payload = {}
                    for name, ctype in columns_map.items():
                        if name in values:
                            value = values[name]
                        else:
                            # An omitted column uses its declared default (the
                            # value an online migration gave existing rows).
                            value = default_map.get(name)
                        check_value(value, ctype, "%s.%s" % (table, name))
                        payload[name] = value
                else:
                    if not values:
                        raise SchemaError("update values must not be empty")
                    for name, value in values.items():
                        check_value(value, columns_map[name], "%s.%s" % (table, name))
                    payload = dict(values)
            else:
                payload = None

            # Structural conflict: the table's column definition changed after
            # this transaction's start view (an online add_column landed).  Its
            # reads keep the old definition, but a write against a migrated
            # table is refused explicitly and leaves no write behind.
            if set(columns_map) != set(live_types):
                self._raise_schema_conflict(conn, handle, table, base_version,
                                            columns_map, live_types)

            snap_view = {rid: {"version": ver, "values": vals}
                         for rid, ver, vals in self._visible_rows(conn, table, base_version)}
            staged = {rid: eff for rid, eff in self._staged_for_table(conn, handle.txid, table)}
            has_prior = row_id in staged
            prior = staged.get(row_id)
            if has_prior:
                base_present = prior["base_present"]
                base_row_version = prior["base_version"]
                old_values = prior["old_values"]
                current_present = prior["op"] != "delete"
                current_values = prior["new_values"]
                cur_had_insert = prior["had_insert"]
            else:
                snap = snap_view.get(row_id)
                base_present = snap is not None
                base_row_version = snap["version"] if snap else None
                old_values = snap["values"] if snap else None
                current_present = base_present
                current_values = snap["values"] if snap else None
                cur_had_insert = False

            if expected_version is not None and base_present and base_row_version != expected_version:
                evidence = {
                    "code": "stale_expected_version",
                    "transaction": handle.txid,
                    "operation": kind,
                    "table": table,
                    "row_id": row_id,
                    "expected_version": expected_version,
                    "observed_version_in_start_view": base_row_version,
                    "snapshot_version": base_version,
                    "detail": "the identity's version in the fixed start view differs from "
                              "the version the caller named as a precondition",
                }
                raise ConflictError(
                    "%s(%s/%s) expected version %s but the start view holds %s"
                    % (kind, table, row_id, expected_version, base_row_version),
                    "stale_expected_version", evidence)

            if kind == "insert":
                if current_present:
                    raise TransactionError("insert(%s/%s): identity already present in view"
                                           % (table, row_id))
                new_values, op = payload, "insert"
            elif kind == "update":
                if not current_present:
                    raise TransactionError(
                        "update(%s/%s): identity not present in this transaction's view"
                        % (table, row_id))
                merged = dict(current_values)
                merged.update(payload)
                new_values = merged
                op = "insert" if cur_had_insert else "update"
            else:
                if not current_present:
                    raise TransactionError(
                        "delete(%s/%s): identity not present in this transaction's view"
                        % (table, row_id))
                new_values, op = None, "delete"

            local = self._bump_local(conn, handle.txid)
            local_token = -local
            conn.execute(
                "INSERT INTO staged(user_txid, table_name, row_id, op, old_values, new_values, "
                "base_version, base_present, expected_version, result_version, has_prior) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (handle.txid, table, row_id, op,
                 canonical.values_to_json(old_values) if old_values is not None else None,
                 canonical.values_to_json(new_values) if new_values is not None else None,
                 base_row_version if base_present else None,
                 1 if base_present else 0,
                 expected_version if expected_version is not None else base_row_version,
                 local_token, 1 if has_prior else 0))
            # *prior_values* is the image this individual op replaced inside
            # the transaction (the previous staged image, or the snapshot
            # image for the first op).  The checker's replay needs it to
            # validate a chain of same-identity ops in user order.
            detail = {"op": op, "row_id": row_id,
                      "old_values": old_values, "new_values": new_values,
                      "prior_values": current_values if current_present else None,
                      "base_version": base_row_version,
                      "snapshot_version": base_version, "local_token": local_token}
            self._log_op(conn, handle.txid, "write", table, detail)
            self._touch_lease(conn, handle.txid)
            return {"table": table, "row_id": row_id, "op": op,
                    "base_version": base_row_version, "base_present": base_present,
                    "local_token": local_token}
        return self.store.run_tx(_do)

    @staticmethod
    def _bump_local(conn, txid):
        conn.execute("UPDATE transactions SET local_counter = local_counter + 1 "
                     "WHERE user_txid=?", (txid,))
        return conn.execute("SELECT local_counter FROM transactions WHERE user_txid=?",
                            (txid,)).fetchone()[0]

    def _raise_schema_conflict(self, conn, handle, table, base_version,
                               snapshot_columns, live_columns):
        """Build evidence for a write against a table migrated after begin.

        Nothing has been written when this runs; the exception itself is the
        explicit structural-conflict result and no staging row is produced.
        """
        changes = [dict(r) for r in conn.execute(
            "SELECT commit_version, table_name, schema_version, change_type, column_name, "
            "column_type, default_json, after_columns_json FROM schema_changes "
            "WHERE table_name=? AND commit_version > ? ORDER BY commit_version",
            (table, base_version))]
        for ch in changes:
            ch["default"] = json.loads(ch.pop("default_json"))
            ch["after_columns"] = [c["name"] for c in
                                   json.loads(ch.pop("after_columns_json"))]
        added = sorted(set(live_columns) - set(snapshot_columns))
        evidence = {
            "code": "schema_conflict",
            "transaction": handle.txid,
            "table": table,
            "snapshot_version": base_version,
            "current_version": Store.current_commit_version(conn),
            "columns_at_snapshot": list(snapshot_columns),
            "columns_now": list(live_columns),
            "added_columns": added,
            "migrations": changes,
            "summary": "table %r changed structure (added column(s) %s) after transaction %s "
                       "began at version %s; reads keep the old definition but the write is "
                       "refused with no effect"
                       % (table, added, handle.txid, base_version),
        }
        raise SchemaConflictError(
            "schema conflict: transaction %s writes %s whose columns changed "
            "after its start view" % (handle.txid, table),
            "schema_conflict", evidence)

    # -- commit --------------------------------------------------------------

    def commit(self, handle):
        """Validate, then record data + decision atomically.  Idempotent."""
        outcome = self.store.run_tx(lambda conn: self._commit_inner(conn, handle))
        if outcome["status"] == REJECTED:
            evidence = dict(outcome["evidence"])
            # A resend of a rejected commit reproduces the *same* durable
            # decision; mark it so the caller can tell it was a replay.
            evidence["replayed"] = outcome.get("replayed", False)
            raise ConflictError(
                "transaction %s rejected: %s" % (handle.txid, evidence["summary"]),
                evidence["code"], evidence)
        if outcome["status"] == ABORTED_ORPHAN:
            raise TransactionError(
                "transaction %s had no durable commit decision and was recovered as "
                "not committed" % handle.txid)
        return outcome

    def _commit_inner(self, conn, handle):
        tx = self._load(conn, handle)
        prior = conn.execute("SELECT decision, commit_version, evidence_json FROM decisions "
                             "WHERE user_txid=?", (handle.txid,)).fetchone()
        if prior is not None:
            prior_evidence = json.loads(prior["evidence_json"]) \
                if prior["evidence_json"] else None
            outcome = {"txid": handle.txid, "status": prior["decision"],
                       "commit_version": prior["commit_version"], "replayed": True,
                       "evidence": prior_evidence}
            if prior["decision"] == COMMITTED and prior_evidence is not None:
                # Reconstruct the same public result shape, including the
                # implicit deletes, so a re-sent commit cannot delete or advance
                # anything again but returns the identical durable decision.
                outcome["writes"] = prior_evidence.get("writes", [])
                outcome["cascade_deletes"] = prior_evidence.get("cascade_deletes", [])
            return outcome
        if tx["state"] != ACTIVE:
            raise TransactionError("transaction %s is %s and has no decision"
                                   % (handle.txid, tx["state"]))

        base_version = tx["base_version"]
        problems = self._validate(conn, handle, base_version)
        if problems:
            return self._record_rejection(conn, handle, base_version, problems)

        # Defensive structural guard: staging normally refuses a migrated table
        # already, but check again inside this atomic transaction so a migration
        # landing after the last stage still cannot produce a malformed write.
        staged_tables = [r[0] for r in conn.execute(
            "SELECT DISTINCT table_name FROM staged WHERE user_txid=? ORDER BY table_name",
            (handle.txid,))]
        for table in staged_tables:
            snap_types, _d = self.store.schema_at_conn(conn, table, base_version)
            live_types, _ld, _c = self.store.latest_schema_conn(conn, table)
            if set(snap_types) != set(live_types):
                self._raise_schema_conflict(conn, handle, table, base_version,
                                            snap_types, live_types)

        # Managed reference constraints are evaluated against the complete
        # state this commit would produce (not the staged operation order),
        # inside this same atomic transaction.  A returned problem rejects the
        # whole commit -- including every otherwise-valid staged write -- and
        # the returned cascade set becomes a set of real, implicit deletes
        # written together with the explicit writes below.  A read-only
        # transaction changes no business state, so it cannot affect or
        # violate a reference and skips the planner.
        if staged_tables:
            ref_problems, cascade = refs_mod.plan_commit(
                conn, self, handle.txid, base_version, staged_tables)
        else:
            ref_problems, cascade = [], []
        if ref_problems:
            return self._record_rejection(conn, handle, base_version, ref_problems)
        cascade_keys = {(item["table"], item["row_id"]) for item in cascade}

        commit_version = Store.bump_commit_version(conn)
        writes = []
        ordinal = 0
        # Tables that some open subscription currently covers.  Change-log
        # rows for exactly these tables are appended in this same atomic
        # commit, so an aborted/uncommitted staging can never enter the log.
        watched = subscription_mod.watched_tables(conn)
        tables = staged_tables
        # Cascade may reach tables the transaction never explicitly staged.
        for item in cascade:
            if item["table"] not in tables:
                tables.append(item["table"])
        for table in tables:
            # The guard above already proved this table did not change after
            # the transaction's start view, so its snapshot column set is the
            # one its staged values conform to.
            columns_map, _d = self.store.schema_at_conn(conn, table, base_version)
            for rid, eff in self._staged_for_table(conn, handle.txid, table):
                if (table, rid) in cascade_keys:
                    # A row this transaction itself inserted and the cascade
                    # then removes never reaches committed state: suppress the
                    # staged insert completely (no history row, no write
                    # evidence, no change event).  Its removal is reported once
                    # in cascade_deletes with ephemeral=True.
                    continue
                ordinal += 1
                new_values = eff["new_values"]
                deleted = 1 if eff["op"] == "delete" else 0
                placeholders = ["row_id", "version", "deleted"] + list(columns_map)
                values = [rid, commit_version, deleted]
                for name in columns_map:
                    values.append(None if deleted else new_values[name])
                conn.execute(
                    'INSERT INTO "%s" (%s) VALUES (%s)'
                    % (hist_table(table),
                       ", ".join('"%s"' % c for c in placeholders),
                       ", ".join("?" for _ in placeholders)), values)
                conn.execute(
                    "INSERT INTO committed_writes(ordinal, user_txid, table_name, row_id, op, "
                    "old_values, new_values, old_version, new_version, commit_version) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (ordinal, handle.txid, table, rid, eff["op"],
                     canonical.values_to_json(eff["old_values"])
                     if eff["old_values"] is not None else None,
                     canonical.values_to_json(new_values)
                     if new_values is not None else None,
                     eff["base_version"], commit_version, commit_version))
                if table in watched:
                    # A change with no net final effect (insert+delete of a
                    # brand-new identity inside one transaction) is omitted.
                    no_final_change = (
                        eff["old_values"] is None and new_values is None)
                    if not no_final_change:
                        conn.execute(
                            "INSERT INTO sub_changes(commit_version, user_txid, "
                            "table_name, row_id, old_present, old_values, old_version, "
                            "new_present, new_values, new_version) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (commit_version, handle.txid, table, rid,
                             1 if eff["old_values"] is not None else 0,
                             canonical.values_to_json(eff["old_values"])
                             if eff["old_values"] is not None else None,
                             eff["base_version"],
                             1 if new_values is not None else 0,
                             canonical.values_to_json(new_values)
                             if new_values is not None else None,
                             commit_version))
                writes.append({"ordinal": ordinal, "table": table, "row_id": rid,
                               "op": eff["op"], "base_version": eff["base_version"],
                               "old_values": eff["old_values"],
                               "new_values": new_values,
                               "commit_version": commit_version})

        # Implicit cascade deletes are real writes in the *same* source
        # commit: one appended tombstone version, one committed_writes evidence
        # row, and one subscription change (for a watched table) per deleted
        # identity.  An identity this transaction inserted and the cascade then
        # removes ("ephemeral") never existed in the committed state, so it gets
        # no fabricated history row and no change event.
        for item in cascade:
            table, rid = item["table"], item["row_id"]
            if item["ephemeral"]:
                continue
            ordinal += 1
            # A cascade can reach a table the transaction never explicitly
            # staged.  Its tombstone only names the managed identity columns,
            # so resolving the current (live) column set is always valid and
            # keeps this path independent of the transaction's start-view
            # schema for tables it never wrote.
            columns_map, _d = self.store.schema_at_conn(
                conn, table, Store.current_commit_version(conn))
            placeholders = ["row_id", "version", "deleted"] + list(columns_map)
            values = [rid, commit_version, 1] + [None for _ in columns_map]
            conn.execute(
                'INSERT INTO "%s" (%s) VALUES (%s)'
                % (hist_table(table),
                   ", ".join('"%s"' % c for c in placeholders),
                   ", ".join("?" for _ in placeholders)), values)
            conn.execute(
                "INSERT INTO committed_writes(ordinal, user_txid, table_name, row_id, op, "
                "old_values, new_values, old_version, new_version, commit_version) "
                "VALUES (?, ?, ?, ?, 'cascade_delete', ?, NULL, ?, ?, ?)",
                (ordinal, handle.txid, table, rid,
                 canonical.values_to_json(item["old_values"]),
                 item["old_version"], commit_version, commit_version))
            if table in watched:
                conn.execute(
                    "INSERT INTO sub_changes(commit_version, user_txid, "
                    "table_name, row_id, old_present, old_values, old_version, "
                    "new_present, new_values, new_version) "
                    "VALUES (?, ?, ?, ?, 1, ?, ?, 0, NULL, NULL)",
                    (commit_version, handle.txid, table, rid,
                     canonical.values_to_json(item["old_values"]),
                     item["old_version"]))
        cascade_report = [
            {"table": item["table"], "row_id": item["row_id"],
             "op": "cascade_delete", "constraint": item["constraint"],
             "parent_table": item["parent_table"], "parent_id": item["parent_id"],
             "depth": item["depth"], "ephemeral": item["ephemeral"],
             "old_version": item["old_version"], "commit_version": commit_version,
             "propagation_source": [item["parent_table"], item["parent_id"]]}
            for item in cascade]
        evidence = {"writes": writes, "cascade_deletes": cascade_report,
                    "base_version": base_version, "commit_version": commit_version}
        conn.execute("INSERT INTO decisions(user_txid, decision, commit_version, decided_wall, "
                     "evidence_json) VALUES (?, 'committed', ?, ?, ?)",
                     (handle.txid, commit_version, time.time(),
                      json.dumps(evidence, sort_keys=True)))
        conn.execute("UPDATE transactions SET state=?, commit_version=? WHERE user_txid=?",
                     (COMMITTED, commit_version, handle.txid))
        conn.execute("DELETE FROM staged WHERE user_txid=?", (handle.txid,))
        conn.execute("DELETE FROM gc_leases WHERE user_txid=?", (handle.txid,))
        return {"txid": handle.txid, "status": COMMITTED,
                "commit_version": commit_version, "writes": writes,
                "cascade_deletes": cascade_report, "replayed": False}

    def _record_rejection(self, conn, handle, base_version, problems):
        # Audit evidence covers every transaction with recorded reads or
        # writes, so a three-way ring is visible even though only the last
        # committer is rejected.
        ids = [r[0] for r in conn.execute(
            "SELECT user_txid FROM transactions ORDER BY start_wall, user_txid")]
        graph = self._dependency_edges(conn, ids)
        cycles = _cycles(ids, graph)
        evidence = {
            "code": problems[0]["code"],
            "transaction": handle.txid,
            "base_version": base_version,
            "current_version": Store.current_commit_version(conn),
            "summary": problems[0]["summary"],
            "conflicts": problems,
            "dependency_edges": graph,
            "dependency_cycles": cycles,
        }
        encoded = json.dumps(evidence, sort_keys=True)
        conn.execute("INSERT INTO rejection_log(user_txid, code, evidence_json, at_wall) "
                     "VALUES (?, ?, ?, ?)", (handle.txid, evidence["code"],
                                             encoded, time.time()))
        conn.execute("UPDATE transactions SET state=? WHERE user_txid=?",
                     (REJECTED, handle.txid))
        conn.execute("INSERT INTO decisions(user_txid, decision, commit_version, decided_wall, "
                     "evidence_json) VALUES (?, 'rejected', NULL, ?, ?)",
                     (handle.txid, time.time(), encoded))
        conn.execute("DELETE FROM staged WHERE user_txid=?", (handle.txid,))
        conn.execute("DELETE FROM gc_leases WHERE user_txid=?", (handle.txid,))
        return {"txid": handle.txid, "status": REJECTED, "commit_version": None,
                "evidence": evidence, "replayed": False}

    # -- validation ----------------------------------------------------------

    def _validate(self, conn, handle, base_version):
        current = Store.current_commit_version(conn)
        if current <= base_version:
            return []
        has_writes = conn.execute("SELECT 1 FROM staged WHERE user_txid=? LIMIT 1",
                                  (handle.txid,)).fetchone() is not None
        if not has_writes:
            # A read-only transaction cannot invalidate anyone.  It serializes
            # at its start view, exactly where it read, so commit order is
            # still a valid serial order.
            return []
        problems = []
        my_tables = [r[0] for r in conn.execute(
            "SELECT DISTINCT table_name FROM staged WHERE user_txid=? ORDER BY table_name",
            (handle.txid,))]
        my_writes = {t: self._staged_for_table(conn, handle.txid, t) for t in my_tables}

        # (1) Same-identity write conflicts and explicit version preconditions.
        for table, items in my_writes.items():
            for rid, eff in items:
                for w in conn.execute(
                        "SELECT user_txid, op, new_version, commit_version FROM committed_writes "
                        "WHERE table_name=? AND row_id=? AND commit_version > ? "
                        "ORDER BY commit_version", (table, rid, base_version)).fetchall():
                    if w["user_txid"] == handle.txid:
                        continue
                    problems.append({
                        "code": "write_conflict", "dependency": "ww",
                        "summary": "identity %s/%s was changed by %s at commit version %s "
                                   "(this write is based on version %s)"
                                   % (table, rid, w["user_txid"], w["commit_version"],
                                      eff["base_version"]),
                        "table": table, "row_id": rid,
                        "this_operation": eff["op"],
                        "row_version_at_snapshot": eff["base_version"],
                        "other_transaction": w["user_txid"], "other_operation": w["op"],
                        "other_commit_version": w["commit_version"],
                        "snapshot_version": base_version,
                        "witness": {"identity": [table, rid]}})
                if (eff["expected_version"] is not None and eff["base_version"] is not None
                        and eff["expected_version"] != eff["base_version"]):
                    problems.append({
                        "code": "precondition_failed", "dependency": "ww",
                        "summary": "expected version %s for %s/%s but the start view holds %s"
                                   % (eff["expected_version"], table, rid, eff["base_version"]),
                        "table": table, "row_id": rid,
                        "expected_version": eff["expected_version"],
                        "snapshot_version": base_version,
                        "witness": {"identity": [table, rid]}})

        # (2) Read validation against writes committed since the snapshot.
        reads = conn.execute(
            "SELECT seq, table_name, predicate_json, rows_json, columns_json "
            "FROM predicate_reads WHERE user_txid=? ORDER BY seq", (handle.txid,)).fetchall()
        for rd in reads:
            problems.extend(self._read_conflicts(conn, handle, base_version, rd))
        return problems

    def _read_conflicts(self, conn, handle, base_version, rd):
        table = rd["table_name"]
        pred = canonical.predicate_from_json(json.loads(rd["predicate_json"]))
        returned_rows = json.loads(rd["rows_json"])
        returned = {row["row_id"]: row for row in returned_rows}
        selected = set(json.loads(rd["columns_json"]))
        relevant = selected | columns_used(pred) | {"row_id"}
        problems = []
        writes = conn.execute(
            "SELECT user_txid, row_id, op, old_values, new_values, commit_version "
            "FROM committed_writes WHERE table_name=? AND commit_version > ? "
            "AND commit_version <= ? ORDER BY commit_version, id",
            (table, base_version, Store.current_commit_version(conn))).fetchall()
        for w in writes:
            if w["user_txid"] == handle.txid:
                continue
            rid = w["row_id"]
            old_values = (canonical.values_from_json(w["old_values"])
                          if w["old_values"] is not None else None)
            new_values = (canonical.values_from_json(w["new_values"])
                          if w["new_values"] is not None else None)
            changed = _changed_columns(old_values, new_values)
            was = _predicate_holds(pred, rid, old_values)
            now = _predicate_holds(pred, rid, new_values)
            if rid in returned:
                # The row was observed.  Any change to what was projected or
                # filtered on makes the observation stale.
                touched = changed & relevant
                if touched:
                    problems.append(self._rw_problem(
                        table, rid, rd, pred, w, was, now,
                        "returned row's observed columns changed",
                        sorted(touched), rid))
                elif was != now:
                    problems.append(self._rw_problem(
                        table, rid, rd, pred, w, was, now,
                        "returned row changed predicate membership", [], rid))
            else:
                # The row was not observed.  Only a change of predicate truth
                # (appearance / disappearance / new match) invalidates the read.
                if was != now and (was or now):
                    problems.append(self._rw_problem(
                        table, rid, rd, pred, w, was, now,
                        "phantom: row %s predicate membership" % (
                            "entered" if now else "left"), [], rid))
        return problems

    @staticmethod
    def _rw_problem(table, rid, rd, pred, w, was, now, reason, columns, witness_rid):
        return {
            "code": "read_conflict", "dependency": "rw",
            "summary": "read of %s (predicate %s) was invalidated by %s's commit %s to %s/%s: %s"
                       % (table, canonical.describe(pred)["expression"], w["user_txid"],
                          w["commit_version"], table, rid, reason),
            "table": table, "row_id": rid,
            "predicate": json.loads(rd["predicate_json"]),
            "predicate_expression": canonical.describe(pred)["expression"],
            "rows_returned": sorted(returned_ids(rd)),
            "other_transaction": w["user_txid"], "other_operation": w["op"],
            "other_commit_version": w["commit_version"],
            "matches_at_snapshot": was, "matches_after_other_commit": now,
            "changed_columns": columns,
            "witness": {"identity": [table, witness_rid],
                        "predicate": canonical.describe(pred)["expression"],
                        "dependency": "rw"}}

    # -- dependency graph / cycles (audit evidence) --------------------------

    def _tx_evidence(self, conn, txid):
        """Reads and writes of one transaction, committed or still staged."""
        tx = conn.execute("SELECT state, base_version, commit_version FROM transactions "
                          "WHERE user_txid=?", (txid,)).fetchone()
        reads = []
        for r in conn.execute(
                "SELECT table_name, predicate_json, rows_json, columns_json, snapshot_version "
                "FROM predicate_reads WHERE user_txid=? ORDER BY seq", (txid,)):
            reads.append({
                "table": r["table_name"],
                "predicate": canonical.predicate_from_json(json.loads(r["predicate_json"])),
                "returned": {row["row_id"]: row for row in json.loads(r["rows_json"])},
                "selected": set(json.loads(r["columns_json"])),
                "snapshot": r["snapshot_version"],
            })
        writes = []
        for r in conn.execute(
                "SELECT table_name, row_id, op, old_values, new_values, commit_version "
                "FROM committed_writes WHERE user_txid=? ORDER BY ordinal", (txid,)):
            writes.append({
                "table": r["table_name"], "row_id": r["row_id"], "op": r["op"],
                "old_values": canonical.values_from_json(r["old_values"])
                if r["old_values"] is not None else None,
                "new_values": canonical.values_from_json(r["new_values"])
                if r["new_values"] is not None else None,
                "commit_version": r["commit_version"], "pending": False,
            })
        if not writes:
            for table in [r[0] for r in conn.execute(
                    "SELECT DISTINCT table_name FROM staged WHERE user_txid=?", (txid,))]:
                for rid, eff in self._staged_for_table(conn, txid, table):
                    writes.append({"table": table, "row_id": rid, "op": eff["op"],
                                   "old_values": eff["old_values"],
                                   "new_values": eff["new_values"],
                                   "commit_version": None, "pending": True})
        return {
            "state": tx["state"] if tx else None,
            "snapshot": tx["base_version"] if tx else None,
            "commit_version": tx["commit_version"] if tx else None,
            "reads": reads, "writes": writes,
        }

    def _dependency_edges(self, conn, txids):
        """rw/wr/ww edges derived only from recorded evidence.

        * rw (anti-dependency) a -> b: something a read is changed by b's write
          that a did not see.  For a committed b that requires
          ``b.commit_version > a.snapshot``; a pending b serializes after a.
        * wr (read dependency) b -> a: a read a row image that b produced.
        * ww b -> a: both wrote one identity and b's change precedes a's.
        """
        ev = {txid: self._tx_evidence(conn, txid) for txid in txids}
        edges = []
        seen = set()

        def add(frm, to, kind, table=None, row_id=None, detail=None):
            key = (frm, to, kind, table, row_id)
            if frm == to or key in seen:
                return
            seen.add(key)
            edge = {"from": frm, "to": to, "kind": kind}
            if table is not None:
                edge["table"] = table
            if row_id is not None:
                edge["row_id"] = row_id
            if detail:
                edge.update(detail)
            edges.append(edge)

        for a in txids:
            ea = ev[a]
            for b in txids:
                if a == b:
                    continue
                eb = ev[b]
                # rw / wr through reads of a against writes of b
                for rd in ea["reads"]:
                    pred, table = rd["predicate"], rd["table"]
                    relevant = rd["selected"] | columns_used(pred) | {"row_id"}
                    for w in eb["writes"]:
                        if w["table"] != table:
                            continue
                        rid = w["row_id"]
                        was = _predicate_holds(pred, rid, w["old_values"])
                        now = _predicate_holds(pred, rid, w["new_values"])
                        changed = _changed_columns(w["old_values"], w["new_values"])
                        observed = rid in rd["returned"]
                        if not (changed & relevant) and was == now:
                            continue
                        if not (observed or was or now):
                            continue
                        if w["pending"]:
                            # pending writer commits after a's snapshot
                            add(a, b, "rw", table, rid,
                                {"predicate": canonical.describe(pred)["expression"],
                                 "witness": "row %s changed by a still-pending write" % rid})
                        else:
                            cv = w["commit_version"]
                            if cv is not None and ea["snapshot"] is not None \
                                    and cv > ea["snapshot"]:
                                add(a, b, "rw", table, rid,
                                    {"predicate": canonical.describe(pred)["expression"],
                                     "witness": "committed at version %s after the reader's "
                                                "snapshot %s" % (cv, ea["snapshot"])})
                            elif cv is not None and ea["snapshot"] is not None \
                                    and cv <= ea["snapshot"] and observed:
                                add(b, a, "wr", table, rid,
                                    {"witness": "read version produced by this writer"})
                # ww on shared identities, ordered by commit version
                for wa in ea["writes"]:
                    for wb in eb["writes"]:
                        if wa["table"] != wb["table"] or wa["row_id"] != wb["row_id"]:
                            continue
                        if wa["pending"] and not wb["pending"]:
                            add(b, a, "ww", wa["table"], wa["row_id"])
                        elif wb["pending"] and not wa["pending"]:
                            add(a, b, "ww", wa["table"], wa["row_id"])
                        elif not wa["pending"] and not wb["pending"]:
                            if wa["commit_version"] < wb["commit_version"]:
                                add(a, b, "ww", wa["table"], wa["row_id"])
                            elif wb["commit_version"] < wa["commit_version"]:
                                add(b, a, "ww", wa["table"], wa["row_id"])
                        else:
                            add(a, b, "ww", wa["table"], wa["row_id"])
        return edges

    def dependency_graph(self, txids=None):
        def _q(conn):
            if txids is None:
                ids = [r[0] for r in conn.execute(
                    "SELECT user_txid FROM transactions ORDER BY start_wall, user_txid")]
            else:
                ids = list(txids)
            nodes = []
            for txid in ids:
                row = conn.execute("SELECT state, base_version, commit_version FROM "
                                   "transactions WHERE user_txid=?", (txid,)).fetchone()
                dec = conn.execute("SELECT decision FROM decisions WHERE user_txid=?",
                                   (txid,)).fetchone()
                nodes.append({"txid": txid,
                              "state": row["state"] if row else None,
                              "decision": dec["decision"] if dec else None,
                              "snapshot_version": row["base_version"] if row else None,
                              "commit_version": row["commit_version"] if row else None})
            edges = self._dependency_edges(conn, ids)
            return {"nodes": nodes, "edges": edges, "cycles": _cycles(ids, edges)}
        return self.store.run_tx(_q, readonly=True)

    # -- abort ---------------------------------------------------------------

    def abort(self, handle):
        def _abort(conn):
            self._load(conn, handle)
            prior = conn.execute("SELECT decision, commit_version FROM decisions "
                                 "WHERE user_txid=?", (handle.txid,)).fetchone()
            if prior is not None:
                return {"txid": handle.txid, "status": prior["decision"], "replayed": True,
                        "commit_version": prior["commit_version"]}
            conn.execute("UPDATE transactions SET state=? WHERE user_txid=?",
                         (ABORTED, handle.txid))
            conn.execute(
                "INSERT INTO decisions(user_txid, decision, commit_version, decided_wall, "
                "evidence_json) VALUES (?, 'aborted', NULL, ?, '{}')",
                (handle.txid, time.time()))
            conn.execute("DELETE FROM staged WHERE user_txid=?", (handle.txid,))
            conn.execute("DELETE FROM gc_leases WHERE user_txid=?", (handle.txid,))
            return {"txid": handle.txid, "status": ABORTED, "replayed": False,
                    "commit_version": None}
        return self.store.run_tx(_abort)

    # -- recovery ------------------------------------------------------------

    def recover(self):
        """Resolve active transactions whose owner process is gone.

        A commit is atomic with its decision row and staged writes are never
        visible, so an undecided transaction has no visible writes.  If the
        owning process is dead (PID gone or start time changed) or its lease
        expired, we record an explicit ``aborted_orphan`` decision.  Queries
        then deterministically report "not committed", and a resend of
        ``commit`` cannot write anything.
        """
        def _recover(conn):
            now = time.time()
            rows = conn.execute(
                "SELECT t.user_txid, t.owner_pid, t.owner_start, l.expires_wall "
                "FROM transactions t LEFT JOIN decisions d ON d.user_txid=t.user_txid "
                "LEFT JOIN gc_leases l ON l.user_txid=t.user_txid "
                "WHERE t.state='active' AND d.user_txid IS NULL").fetchall()
            orphans = []
            for r in rows:
                dead = not _owner_alive(r["owner_pid"], r["owner_start"])
                expired = r["expires_wall"] is not None and r["expires_wall"] < now
                if dead or expired:
                    orphans.append(r["user_txid"])
            for txid in orphans:
                conn.execute(
                    "INSERT OR IGNORE INTO decisions(user_txid, decision, commit_version, "
                    "decided_wall, evidence_json) VALUES (?, 'aborted_orphan', NULL, ?, ?)",
                    (txid, now, json.dumps({"reason": "owner process gone or lease expired; "
                                                      "no durable commit decision"})))
                conn.execute("UPDATE transactions SET state=? WHERE user_txid=?",
                             (ABORTED_ORPHAN, txid))
                conn.execute("DELETE FROM staged WHERE user_txid=?", (txid,))
                conn.execute("DELETE FROM gc_leases WHERE user_txid=?", (txid,))
            return orphans
        return self.store.run_tx(_recover)

    # -- version cleanup -----------------------------------------------------

    def gc_versions(self):
        """Prune old history versions at a safe boundary.

        Safe boundary: let ``safe = min(base_version)`` over active
        transactions (``current`` if none).  For every identity, keep
        (a) the newest version ``<= safe`` -- the oldest live snapshot needs
        it, (b) every version ``> safe`` -- newer live snapshots may need any
        of them, and (c) the newest version overall.  Versions strictly below
        ``safe`` that are not the newest ``<= safe`` are unreachable by any
        live snapshot and are deleted.  Read validation never consults history
        (it uses ``committed_writes`` evidence), so pruning cannot change a
        commit decision.
        """
        def _gc(conn):
            active = conn.execute(
                "SELECT user_txid, base_version FROM transactions WHERE state=?", (ACTIVE,)
            ).fetchall()
            current = Store.current_commit_version(conn)
            live = []
            for r in active:
                if _owner_alive_by_txid(conn, r["user_txid"]):
                    live.append(r["base_version"])
            # A subscription still in its full phase reconstructs its cut
            # snapshot S directly from row history; that need is exactly like
            # a live read snapshot at S.
            full_cuts = subscription_mod.full_phase_snapshots(conn)
            live.extend(full_cuts)
            safe = min(live) if live else current
            removed = 0
            for (table,) in conn.execute("SELECT table_name FROM tables").fetchall():
                hist = hist_table(table)
                rows = conn.execute(
                    'SELECT row_id, version FROM "%s" ORDER BY row_id, version' % hist
                ).fetchall()
                per_row = {}
                for rid, ver in rows:
                    per_row.setdefault(rid, []).append(ver)
                for rid, versions in per_row.items():
                    keep = set()
                    le_safe = [v for v in versions if v <= safe]
                    if le_safe:
                        keep.add(max(le_safe))
                    keep.update(v for v in versions if v > safe)
                    keep.add(max(versions))
                    for v in versions:
                        if v not in keep:
                            conn.execute('DELETE FROM "%s" WHERE row_id=? AND version=?'
                                         % hist, (rid, v))
                            removed += 1
            return {"safe_version": safe, "pruned_versions": removed,
                    "active_snapshots": sorted(set(live)),
                    "full_phase_cuts": sorted(set(full_cuts))}
        return self.store.run_tx(_gc)

    # -- introspection -------------------------------------------------------

    def transaction_status(self, txid):
        """Durable committed / not-committed answer, callable from any process."""
        def _q(conn):
            tx = conn.execute(
                "SELECT state, base_version, start_wall, commit_version FROM transactions "
                "WHERE user_txid=?", (txid,)).fetchone()
            dec = conn.execute(
                "SELECT decision, commit_version, decided_wall, evidence_json FROM decisions "
                "WHERE user_txid=?", (txid,)).fetchone()
            if tx is None and dec is None:
                return None
            return {"txid": txid,
                    "state": tx["state"] if tx else None,
                    "decision": dec["decision"] if dec else None,
                    "committed": bool(dec and dec["decision"] == "committed"),
                    "commit_version": dec["commit_version"] if dec else None,
                    "snapshot_version": tx["base_version"] if tx else None,
                    "started_wall": tx["start_wall"] if tx else None,
                    "decided_wall": dec["decided_wall"] if dec else None}
        return self.store.run_tx(_q, readonly=True)

    def current_version(self):
        def _q(conn):
            return Store.current_commit_version(conn)
        return self.store.run_tx(_q, readonly=True)

    def rows(self, table):
        """Latest committed images, ordered by row_id (verification helper)."""
        def _q(conn):
            version = Store.current_commit_version(conn)
            out = []
            for rid, ver, vals in self._visible_rows(conn, table, version):
                row = {"row_id": rid, "version": ver}
                row.update(vals)
                out.append(row)
            return out
        return self.store.run_tx(_q, readonly=True)

    def history(self, table, row_id):
        def _q(conn):
            current = Store.current_commit_version(conn)
            columns, _defaults = self.store.schema_at_conn(conn, table, current)
            rows = conn.execute(
                'SELECT * FROM "%s" WHERE row_id=? ORDER BY version' % hist_table(table),
                (row_id,)).fetchall()
            out = []
            for r in rows:
                entry = {"row_id": r["row_id"], "version": r["version"],
                         "deleted": bool(r["deleted"])}
                if not r["deleted"]:
                    entry.update(_row_dict(r, columns))
                out.append(entry)
            return out
        return self.store.run_tx(_q, readonly=True)

    def operations(self, txid):
        def _q(conn):
            return [dict(r) for r in conn.execute(
                "SELECT ord, kind, table_name, detail_json FROM tx_ops WHERE user_txid=? "
                "ORDER BY ord", (txid,))]
        return self.store.run_tx(_q, readonly=True)

    def rejection_evidence(self, txid):
        def _q(conn):
            row = conn.execute(
                "SELECT code, evidence_json, at_wall FROM rejection_log WHERE user_txid=? "
                "ORDER BY id DESC LIMIT 1", (txid,)).fetchone()
            if row is None:
                return None
            return {"code": row["code"], "at_wall": row["at_wall"],
                    "evidence": json.loads(row["evidence_json"])}
        return self.store.run_tx(_q, readonly=True)

    # -- change subscriptions -------------------------------------------------

    def create_subscription(self, name, tables):
        """Create a subscription and fix its cut version atomically."""
        return subscription_mod.create(self, name, tables)

    def open_subscription(self, handle):
        """Re-open a persisted handle, resuming its durable progress."""
        return subscription_mod.open_subscription(self, handle)

    def subscription_status(self, handle):
        return subscription_mod.status(self, handle)

    def list_subscriptions(self):
        return subscription_mod.list_subscriptions(self)

    def pull(self, handle, max_rows=100):
        """Return the single pending batch, or durably produce the next one."""
        return subscription_mod.pull(self, handle, max_rows=max_rows)

    def ack(self, handle, batch_id):
        """Acknowledge only the batch id; the covered position is durable."""
        return subscription_mod.ack(self, handle, batch_id)

    def close_subscription(self, handle):
        return subscription_mod.close(self, handle)

    def rebuild_subscription(self, handle):
        return subscription_mod.rebuild(self, handle)

    def cleanup_sync_history(self):
        """Reclaim change-log entries no open subscription can still need."""
        return subscription_mod.cleanup_sync_history(self)


def returned_ids(rd):
    return [row["row_id"] for row in json.loads(rd["rows_json"])]


def _owner_alive(pid, start):
    """True only if *that exact process* is still running.

    Checking the /proc start time (not just the PID) prevents PID reuse from
    keeping a dead transaction artificially alive, which would otherwise pin
    the GC safe point forever.
    """
    if pid is None:
        return False
    if start is not None:
        current = _proc_starttime(pid)
        return current is not None and current == start
    if pid == os.getpid():
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


def _owner_alive_by_txid(conn, txid):
    row = conn.execute("SELECT owner_pid, owner_start FROM transactions WHERE user_txid=?",
                       (txid,)).fetchone()
    if row is None:
        return False
    return _owner_alive(row["owner_pid"], row["owner_start"])


def _cycles(nodes, edges):
    """Enumerate simple cycles (small graphs), each with its actual witnesses."""
    adjacency = {}
    for e in edges:
        adjacency.setdefault(e["from"], []).append(e)
    index = {n: i for i, n in enumerate(nodes)}
    found = {}

    def dfs(start, node, path, on_path):
        for e in adjacency.get(node, []):
            nxt = e["to"]
            if nxt == start:
                cycle = path + [e]
                key = frozenset(x["from"] for x in cycle)
                if key not in found:
                    found[key] = cycle
            elif nxt not in on_path and index.get(nxt, -1) >= index.get(start, 0):
                on_path.add(nxt)
                dfs(start, nxt, path + [e], on_path)
                on_path.discard(nxt)

    for start in nodes:
        dfs(start, start, [], {start})
    out = []
    for key, edge_list in found.items():
        out.append({
            "transactions": [e["from"] for e in edge_list],
            "closed": True,
            "total_orderable": False,
            "edges": [{"from": e["from"], "to": e["to"], "kind": e["kind"],
                       "table": e.get("table"), "row_id": e.get("row_id"),
                       "witness": e.get("witness"), "predicate": e.get("predicate")}
                      for e in edge_list],
        })
    return out
