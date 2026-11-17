"""Storage layer: schema, short SQLite transactions, bounded retry, recovery.

Properties
----------
* Rows live in append-only history tables ``<table>_rows``.  A reader with
  snapshot version V sees, per stable ``row_id``, the newest entry with
  ``version <= V``.  Long, genuinely overlapping user transactions therefore
  coexist without holding any lock.
* One user transaction = one atomic short SQLite transaction at commit time:
  validation, the new row versions, the durable *decision*, and the write
  evidence all land together.  A crash leaves either all of it or none.
* SQLite ``busy``/``locked`` is treated as an infrastructure condition, retried
  a bounded number of times, then raised as :class:`BusyError`.  It is never
  interpreted as a transaction conflict.
"""

import os
import json
import random
import re
import sqlite3
import time

from .errors import BusyError, InternalInvariantError, SchemaError
from .values import COLUMN_TYPES, check_value, TYPE_BOOL, TYPE_INT, TYPE_TEXT, TYPE_NULL

SCHEMA_VERSION = 5

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")

BASE_DDL = [
    "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS tables (table_name TEXT PRIMARY KEY, version INTEGER NOT NULL)",
    """
    CREATE TABLE IF NOT EXISTS table_columns (
        table_name TEXT NOT NULL, position INTEGER NOT NULL,
        column_name TEXT NOT NULL, column_type TEXT NOT NULL,
        PRIMARY KEY (table_name, column_name))
    """,
    """
    CREATE TABLE IF NOT EXISTS transactions (
        user_txid TEXT PRIMARY KEY,
        state TEXT NOT NULL,              -- active | committed | rejected | aborted
        base_version INTEGER NOT NULL,    -- fixed start view (snapshot at begin)
        token_hash TEXT NOT NULL,
        owner_pid INTEGER,
        owner_start TEXT,                 -- /proc starttime, guards against PID reuse
        start_wall REAL NOT NULL,
        commit_version INTEGER,
        local_counter INTEGER NOT NULL DEFAULT 0)
    """,
    """
    CREATE TABLE IF NOT EXISTS decisions (
        user_txid TEXT PRIMARY KEY,
        decision TEXT NOT NULL,           -- committed | rejected | aborted | aborted_orphan
        commit_version INTEGER,           -- NULL for every non-committed decision
        decided_wall REAL NOT NULL,
        evidence_json TEXT)
    """,
    """
    CREATE TABLE IF NOT EXISTS rejection_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_txid TEXT NOT NULL, code TEXT NOT NULL,
        evidence_json TEXT NOT NULL, at_wall REAL NOT NULL)
    """,
    "CREATE INDEX IF NOT EXISTS rejection_by_tx ON rejection_log(user_txid)",
    """
    CREATE TABLE IF NOT EXISTS staged (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        user_txid TEXT NOT NULL,
        table_name TEXT NOT NULL,
        row_id TEXT NOT NULL,
        op TEXT NOT NULL,                -- insert | update | delete
        old_values TEXT,                 -- snapshot image of the identity (JSON)
        new_values TEXT,                 -- effective image after this op (JSON)
        base_version INTEGER,            -- identity version at the fixed snapshot
        base_present INTEGER NOT NULL,
        expected_version INTEGER,
        result_version INTEGER NOT NULL, -- negative local token, never a commit version
        has_prior INTEGER NOT NULL)
    """,
    "CREATE INDEX IF NOT EXISTS staged_by_tx ON staged(user_txid, seq)",
    "CREATE INDEX IF NOT EXISTS staged_by_id ON staged(user_txid, table_name, row_id, seq)",
    """
    CREATE TABLE IF NOT EXISTS predicate_reads (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        user_txid TEXT NOT NULL,
        table_name TEXT NOT NULL,
        predicate_json TEXT NOT NULL,
        description TEXT NOT NULL,
        rows_json TEXT NOT NULL,         -- rows actually returned (own writes applied)
        columns_json TEXT NOT NULL,      -- whitelist of projected columns (row_id implicit)
        returns_empty INTEGER NOT NULL,
        snapshot_version INTEGER NOT NULL,
        read_wall REAL NOT NULL)
    """,
    "CREATE INDEX IF NOT EXISTS predicate_reads_by_tx ON predicate_reads(user_txid, seq)",
    """
    CREATE TABLE IF NOT EXISTS tx_ops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_txid TEXT NOT NULL,
        ord INTEGER NOT NULL,
        kind TEXT NOT NULL,              -- read | write
        table_name TEXT NOT NULL,
        detail_json TEXT NOT NULL,
        at_wall REAL NOT NULL)
    """,
    "CREATE INDEX IF NOT EXISTS tx_ops_by_tx ON tx_ops(user_txid, ord)",
    """
    CREATE TABLE IF NOT EXISTS committed_writes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ordinal INTEGER NOT NULL,
        user_txid TEXT NOT NULL,
        table_name TEXT NOT NULL,
        row_id TEXT NOT NULL,
        op TEXT NOT NULL,                -- insert | update | delete
        old_values TEXT,
        new_values TEXT,
        old_version INTEGER,
        new_version INTEGER NOT NULL,
        commit_version INTEGER NOT NULL)
    """,
    "CREATE INDEX IF NOT EXISTS cw_by_tx ON committed_writes(user_txid)",
    "CREATE INDEX IF NOT EXISTS cw_by_row ON committed_writes(table_name, row_id)",
    "CREATE INDEX IF NOT EXISTS cw_by_version ON committed_writes(commit_version)",
    """
    CREATE TABLE IF NOT EXISTS gc_leases (
        user_txid TEXT PRIMARY KEY, expires_wall REAL NOT NULL)
    """,
    """
    CREATE TABLE IF NOT EXISTS gc_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        safe_point INTEGER NOT NULL)
    """,
    # -- change subscriptions -------------------------------------------------
    # One durable row per subscription name.  ``generation`` changes only on an
    # explicit rebuild; batch ids and pulls from an older generation can never
    # advance this row again.  ``cut_version`` is the fixed snapshot S.
    """
    CREATE TABLE IF NOT EXISTS subscriptions (
        sub_id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        state TEXT NOT NULL,             -- active | closed
        generation INTEGER NOT NULL,
        cut_version INTEGER NOT NULL,
        tables_json TEXT NOT NULL,       -- code-point-sorted subscribed tables
        schema_json TEXT NOT NULL,       -- frozen descriptors of those tables
        schema_defaults_json TEXT NOT NULL DEFAULT '{}', -- frozen declared defaults
        token_hash TEXT NOT NULL,
        full_done INTEGER NOT NULL,      -- the S full load has been delivered
        ack_version INTEGER NOT NULL,    -- last acknowledged source position
        full_cursor_table TEXT,          -- next table in full pagination
        full_cursor_row TEXT,            -- next row_id (exclusive) in that table
        last_acked_batch TEXT,
        last_acked_position INTEGER,
        created_wall REAL NOT NULL,
        updated_wall REAL NOT NULL)
    """,
    # At most one unacknowledged batch per subscription.  Its identity and
    # business content are durable, so re-pulling (any budget, any process)
    # returns exactly the same batch.
    """
    CREATE TABLE IF NOT EXISTS subscription_pending (
        sub_id TEXT PRIMARY KEY,
        batch_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        phase TEXT NOT NULL,             -- full | incremental
        from_position INTEGER NOT NULL,
        position INTEGER NOT NULL,       -- source position covered by the batch
        full_complete INTEGER NOT NULL,
        manifest_json TEXT NOT NULL,
        created_wall REAL NOT NULL)
    """,
    """
    CREATE TABLE IF NOT EXISTS subscription_items (
        sub_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        item_json TEXT NOT NULL,
        PRIMARY KEY (sub_id, seq))
    """,
    # Source-commit change log for tables covered by at least one open
    # subscription.  Written in the same short commit transaction as the row
    # versions and the durable commit decision, so it can never contain an
    # aborted or uncommitted change.  Pruned by cleanup_sync_history() only up
    # to the slowest open subscription's acknowledged position.
    """
    CREATE TABLE IF NOT EXISTS sub_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        commit_version INTEGER NOT NULL,
        user_txid TEXT NOT NULL,
        table_name TEXT NOT NULL,
        row_id TEXT NOT NULL,
        old_present INTEGER NOT NULL,
        old_values TEXT,
        old_version INTEGER,
        new_present INTEGER NOT NULL,
        new_values TEXT,
        new_version INTEGER)
    """,
    "CREATE INDEX IF NOT EXISTS sub_changes_by_version ON sub_changes(commit_version, id)",
    "CREATE INDEX IF NOT EXISTS sub_changes_by_table ON sub_changes(table_name, commit_version)",
    # -- versioned column catalog (online add-column) -------------------------
    # One immutable row per *table schema version*.  The initial schema (the
    # create_table shape) is version 1; every online add_column appends a new
    # row with the next per-table version.  ``effective_version`` is the
    # monotonically ordered source commit version at which the schema took
    # effect, so a snapshot at V resolves the newest row whose effective
    # version <= V.  Business row history is never rewritten by a migration.
    """
    CREATE TABLE IF NOT EXISTS table_schemas (
        table_name TEXT NOT NULL,
        schema_version INTEGER NOT NULL,
        effective_version INTEGER NOT NULL,
        columns_json TEXT NOT NULL,        -- ordered [{name, type, has_default, default}]
        PRIMARY KEY (table_name, schema_version))
    """,
    # The ordered source history of structural changes.  Written in the same
    # atomic migration transaction that bumps commit_version, so a schema
    # change and the row commits share one ordering stream and a failed
    # migration can never leave an event (or a fabricated position) behind.
    """
    CREATE TABLE IF NOT EXISTS schema_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        commit_version INTEGER NOT NULL,
        table_name TEXT NOT NULL,
        schema_version INTEGER NOT NULL,
        change_type TEXT NOT NULL,        -- only "add_column" is ever written
        column_name TEXT NOT NULL,
        column_type TEXT NOT NULL,
        has_default INTEGER NOT NULL,
        default_json TEXT,
        after_columns_json TEXT NOT NULL)
    """,
    "CREATE INDEX IF NOT EXISTS schema_changes_by_version ON schema_changes(commit_version, id)",
    "CREATE INDEX IF NOT EXISTS schema_changes_by_table ON schema_changes(table_name, commit_version)",
    # -- managed reference constraints ---------------------------------------
    # A durable declaration: a text column of a child table references the
    # implicit row_id of a (possibly identical) parent table.  Only the
    # ``restrict`` and ``cascade`` delete strategies exist.  Rows are written
    # only by a fully validated registration transaction, so a constraint row
    # is authoritative the instant it becomes visible; there is no half
    # definition.  ``registered_version`` records the source position current at
    # registration time (informational; registration itself does not consume a
    # position), which pins which concurrent commits the new rule must cover.
    """
    CREATE TABLE IF NOT EXISTS reference_constraints (
        constraint_name TEXT PRIMARY KEY,
        parent_table TEXT NOT NULL,
        child_table TEXT NOT NULL,
        child_column TEXT NOT NULL,
        on_delete TEXT NOT NULL CHECK (on_delete IN ('restrict', 'cascade')),
        registered_version INTEGER NOT NULL,
        created_wall REAL NOT NULL,
        UNIQUE (child_table, child_column, constraint_name))
    """,
    "CREATE INDEX IF NOT EXISTS ref_by_parent ON reference_constraints(parent_table)",
    "CREATE INDEX IF NOT EXISTS ref_by_child ON reference_constraints(child_table)",
    # -- materialized join-aggregate summaries -------------------------------
    # One durable row per registered definition.  The declaration is frozen as
    # JSON (restricted references only -- no arbitrary SQL); ``state`` moves
    # defined -> building -> active (or -> cancelled / -> failed).  ``cut`` is
    # the single source position the paged initial build is anchored at;
    # ``maintained_version`` is the source position whose state the durable
    # output already reflects.  A row stays after cancellation so its name can
    # never silently acquire a different definition.
    """
    CREATE TABLE IF NOT EXISTS materialized_defs (
        mv_name TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        left_table TEXT NOT NULL,
        right_table TEXT NOT NULL,
        left_join_column TEXT NOT NULL,
        right_join_column TEXT NOT NULL,
        join_kind TEXT NOT NULL CHECK (join_kind IN ('inner', 'left')),
        definition_json TEXT NOT NULL,
        output_columns_json TEXT NOT NULL,
        cut_version INTEGER,
        maintained_version INTEGER NOT NULL DEFAULT 0,
        created_wall REAL NOT NULL,
        updated_wall REAL NOT NULL)
    """,
    "CREATE INDEX IF NOT EXISTS mv_defs_state ON materialized_defs(state)",
    # Resumable, paged build progress: one row per definition while building.
    # Both pagination cursors refer to the same frozen cut_version, so the two
    # source tables can never mix rows from different source positions.
    """
    CREATE TABLE IF NOT EXISTS materialized_builds (
        mv_name TEXT PRIMARY KEY,
        cut_version INTEGER NOT NULL,
        left_cursor_row TEXT NOT NULL,
        right_cursor_row TEXT NOT NULL,
        phase TEXT NOT NULL,             -- initial | catchup | publishing
        catchup_from_version INTEGER NOT NULL,
        updated_wall REAL NOT NULL)
    """,
    # The stable group identity mapping.  ``group_token`` is the managed
    # row_id of the output row; it is allocated once per distinct group tuple
    # and retained while the group is inactive, so a group that disappears and
    # reappears keeps the same identity and different groups never share one.
    """
    CREATE TABLE IF NOT EXISTS materialized_groups (
        mv_name TEXT NOT NULL,
        group_token TEXT NOT NULL,
        group_key_json TEXT NOT NULL,
        active INTEGER NOT NULL,
        PRIMARY KEY (mv_name, group_token))
    """,
    "CREATE INDEX IF NOT EXISTS mv_groups_lookup "
    "ON materialized_groups(mv_name, group_key_json)",
    # On-disk partial aggregates for the paged initial build and the SUM
    # non-null counts needed forever after activation.  No full join result is
    # ever held in memory; pages stream into these rows.
    """
    CREATE TABLE IF NOT EXISTS materialized_accum (
        mv_name TEXT NOT NULL,
        group_key_json TEXT NOT NULL,
        group_token TEXT,
        measures_json TEXT NOT NULL,
        PRIMARY KEY (mv_name, group_key_json))
    """,
    # The set of non-null join keys that currently feed each group, plus a
    # marker for the null-key left rows in left mode.  A group tuple is not
    # bound to one join key (different keys can land in the same group), so
    # maintenance expands an affected key to every group it touches and then
    # reloads *all* that group's keys, never just the one key that changed.
    """
    CREATE TABLE IF NOT EXISTS materialized_group_keys (
        mv_name TEXT NOT NULL,
        group_key_json TEXT NOT NULL,
        join_key TEXT,
        null_scope INTEGER NOT NULL,
        PRIMARY KEY (mv_name, group_key_json, join_key, null_scope))
    """,
    "CREATE INDEX IF NOT EXISTS mv_gk_lookup "
    "ON materialized_group_keys(mv_name, join_key)",
    # Join-column helper indexes are shared across definitions and reference
    # counted, so cancelling one build releases only its own responsibility.
    """
    CREATE TABLE IF NOT EXISTS materialized_indexes (
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        refcount INTEGER NOT NULL,
        PRIMARY KEY (table_name, column_name))
    """,
    # Verifiable maintenance statistics, one row per maintained source commit.
    # Reads performed only to check whether an output value changed are
    # counted too; the engine's own transaction validation is separate.
    """
    CREATE TABLE IF NOT EXISTS materialized_maintenance_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mv_name TEXT NOT NULL,
        commit_version INTEGER,
        phase TEXT NOT NULL,             -- catchup | maintenance | build
        source_rows_read INTEGER NOT NULL,
        helper_records_read INTEGER NOT NULL,
        groups_written INTEGER NOT NULL,
        detail_json TEXT NOT NULL)
    """,
    "CREATE INDEX IF NOT EXISTS mv_stats_by_mv "
    "ON materialized_maintenance_stats(mv_name, id)",
]


class Store:
    def __init__(self, path, busy_timeout_ms=3000, busy_retries=8):
        self.path = os.path.abspath(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.busy_retries = busy_retries
        parent = os.path.dirname(self.path)
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        self.init_schema()

    # -- connections ---------------------------------------------------------

    def _configure(self, conn):
        conn.execute("PRAGMA busy_timeout = %d" % self.busy_timeout_ms)
        conn.execute("PRAGMA foreign_keys = ON")

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=self.busy_timeout_ms / 1000.0,
                               isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _is_busy(exc):
        text = str(exc).lower()
        return "locked" in text or "busy" in text

    def run_tx(self, fn, readonly=False):
        """One short SQLite transaction with bounded retry.

        Writers take ``BEGIN IMMEDIATE``; readers take a deferred snapshot and
        ``PRAGMA query_only``.  Exhausting retries raises :class:`BusyError`
        (internal liveness), never a transaction conflict.
        """
        attempts = 4 if readonly else self.busy_retries
        last = None
        for attempt in range(attempts):
            conn = self.connect()
            try:
                self._configure(conn)
                # journal_mode is persistent and set once by init_schema; do
                # not set it here (SQLite would take a write lock to change it).
                if readonly:
                    conn.execute("PRAGMA query_only = ON")
                else:
                    conn.execute("PRAGMA synchronous = FULL")
                conn.execute("BEGIN" if readonly else "BEGIN IMMEDIATE")
                try:
                    result = fn(conn)
                except BaseException:
                    try:
                        conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    raise
                conn.execute("COMMIT")
                return result
            except sqlite3.OperationalError as exc:
                last = exc
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                if not self._is_busy(exc):
                    raise
                if attempt + 1 >= attempts:
                    raise BusyError(
                        "SQLite stayed busy after %d bounded short-transaction attempts. "
                        "This is an internal liveness condition, not a transaction "
                        "conflict; no commit decision was made: %s" % (attempts, exc)
                    ) from exc
                time.sleep((0.01 * (2 ** attempt)) * (0.5 + random.random()))
            finally:
                conn.close()
        raise BusyError("SQLite was busy: %s" % (last,))

    # -- schema --------------------------------------------------------------

    def _schema_is_current(self):
        """Cheap read-only probe, so opening an existing repo needs no write lock."""
        conn = self.connect()
        try:
            self._configure(conn)
            conn.execute("PRAGMA query_only = ON")
            conn.execute("BEGIN")
            try:
                row = conn.execute(
                    "SELECT value FROM meta WHERE key='schema_version'").fetchone()
                has_seq = conn.execute(
                    "SELECT 1 FROM meta WHERE key='seq'").fetchone() is not None
                has_cv = conn.execute(
                    "SELECT 1 FROM meta WHERE key='commit_version'").fetchone() is not None
            finally:
                conn.execute("COMMIT")
            return (row is not None and int(row[0]) == SCHEMA_VERSION
                    and has_seq and has_cv)
        except sqlite3.DatabaseError:
            return False
        finally:
            conn.close()

    def init_schema(self):
        if self._schema_is_current():
            return

        # ``journal_mode`` cannot be changed from inside a transaction.  On a
        # fresh database this is a no-op before any page exists, but on an
        # existing database that merely needs a schema upgrade it must happen
        # outside the short write transaction below.
        probe = self.connect()
        try:
            self._configure(probe)
            probe.execute("PRAGMA journal_mode = WAL")
        finally:
            probe.close()

        def _init(conn):
            for ddl in BASE_DDL:
                conn.execute(ddl)
            row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            if row is None:
                conn.execute("INSERT INTO meta(key, value) VALUES ('schema_version', ?)",
                             (str(SCHEMA_VERSION),))
                conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('seq', '0')")
                conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('commit_version', '0')")
            elif int(row[0]) in (1, 2, 3, 4):
                # v1 -> v2 added the subscription tables; v2 -> v3 adds the
                # versioned column catalog and the schema-change log.  New
                # tables are created above with IF NOT EXISTS, but an existing
                # subscriptions table still needs the defaults column added.
                if int(row[0]) in (1, 2):
                    sub_cols = {r[1] for r in conn.execute(
                        "PRAGMA table_info(subscriptions)")}
                    if "schema_defaults_json" not in sub_cols:
                        conn.execute(
                            "ALTER TABLE subscriptions ADD COLUMN "
                            "schema_defaults_json TEXT NOT NULL DEFAULT '{}'")
                    # Backfill one immutable schema row per existing table from the
                    # current catalog, so older repositories keep their history
                    # without a rewrite.
                    existing = {r[0] for r in conn.execute(
                        "SELECT table_name FROM table_schemas")}
                    for tname, in conn.execute(
                            "SELECT table_name FROM tables ORDER BY table_name"):
                        if tname in existing:
                            continue
                        cols = [{"name": r[0], "type": r[1], "has_default": False}
                                for r in conn.execute(
                                    "SELECT column_name, column_type FROM table_columns "
                                    "WHERE table_name=? ORDER BY position", (tname,))]
                        # The legacy table carried these columns for its entire
                        # history, so schema version 1 is visible at every snapshot
                        # (effective position 0), including in-flight transactions
                        # whose base_version predates this upgrade.
                        conn.execute(
                            "INSERT INTO table_schemas(table_name, schema_version, "
                            "effective_version, columns_json) VALUES (?, 1, 0, ?)",
                            (tname, json.dumps(cols, sort_keys=True)))
                        conn.execute("UPDATE tables SET version=1 WHERE table_name=?",
                                     (tname,))
                # v3 -> v4 added the managed reference-constraint catalog.  The
                # table is created above with IF NOT EXISTS; an existing
                # repository simply starts with zero declared constraints.
                # v4 -> v5 adds the materialized-summary catalog and on-disk
                # build structures; existing repositories simply have no
                # definitions, and the new tables already exist from the DDL.
                conn.execute("UPDATE meta SET value=? WHERE key='schema_version'",
                             (str(SCHEMA_VERSION),))
            elif int(row[0]) != SCHEMA_VERSION:
                raise InternalInvariantError(
                    "unsupported schema version %s (expected %d)" % (row[0], SCHEMA_VERSION))
        self.run_tx(_init)

    @staticmethod
    def check_ident(name, what="identifier"):
        if not isinstance(name, str) or not _IDENT_RE.match(name):
            raise SchemaError("%s %r is not a safe identifier ([A-Za-z_][A-Za-z0-9_]*)"
                              % (what, name))
        return name

    # -- managed relations ---------------------------------------------------

    def create_table(self, table, columns):
        """Create a managed relation; *columns* is an ordered list of (name, type)."""
        self.check_ident(table, "table name")
        if not columns:
            raise SchemaError("table %r must have at least one column" % table)
        seen = set()
        for name, ctype in columns:
            self.check_ident(name, "column name")
            if ctype not in COLUMN_TYPES:
                raise SchemaError("column %s.%s has unsupported type %r" % (table, name, ctype))
            if name == "row_id":
                raise SchemaError("row_id is the managed identity and is implicit")
            if name in seen:
                raise SchemaError("duplicate column %s.%s" % (table, name))
            seen.add(name)

        hist = table + "_rows"

        def _create(conn):
            if conn.execute("SELECT 1 FROM tables WHERE table_name=?", (table,)).fetchone():
                raise SchemaError("table %r already exists" % table)
            if conn.execute("SELECT 1 FROM materialized_defs WHERE mv_name=?",
                            (table,)).fetchone() is not None:
                raise SchemaError(
                    "name %r is already used by a materialized summary" % table)
            coldefs = ["row_id TEXT NOT NULL", "version INTEGER NOT NULL",
                       "deleted INTEGER NOT NULL"]
            for name, ctype in columns:
                decl = {"int64": "INTEGER", "text": "TEXT", "bool": "INTEGER", "null": ""}[ctype]
                coldefs.append('"%s" %s' % (name, decl) if decl else '"%s"' % name)
            coldefs.append("PRIMARY KEY (row_id, version)")
            conn.execute('CREATE TABLE "%s" (%s)' % (hist, ", ".join(coldefs)))
            conn.execute('CREATE INDEX IF NOT EXISTS "%s_v" ON "%s"(row_id, version DESC)'
                         % (hist, hist))
            conn.execute("INSERT INTO tables(table_name, version) VALUES (?, 1)", (table,))
            for pos, (name, ctype) in enumerate(columns):
                conn.execute(
                    "INSERT INTO table_columns(table_name, position, column_name, column_type) "
                    "VALUES (?, ?, ?, ?)", (table, pos, name, ctype))
            # The immutable schema version 1 is visible from the very first
            # snapshot (effective position 0).  create_table does not consume a
            # commit-version position, and a transaction beginning immediately
            # afterwards has base_version 0, so the table must resolve there.
            schema_cols = [{"name": name, "type": ctype, "has_default": False}
                           for name, ctype in columns]
            conn.execute(
                "INSERT INTO table_schemas(table_name, schema_version, effective_version, "
                "columns_json) VALUES (?, 1, 0, ?)",
                (table, json.dumps(schema_cols, sort_keys=True)))
        self.run_tx(_create)
        return self.table_info(table)

    def column_types(self):
        """Current (latest) declared column types for every managed table."""
        def _read(conn):
            out = {}
            for r in conn.execute(
                    "SELECT table_name, columns_json FROM table_schemas ts "
                    "WHERE schema_version = (SELECT MAX(schema_version) FROM table_schemas "
                    "WHERE table_name = ts.table_name) ORDER BY table_name"):
                cols = json.loads(r["columns_json"])
                out[r["table_name"]] = {c["name"]: c["type"] for c in cols}
            return out
        return self.run_tx(_read, readonly=True)

    def table_info(self, table):
        cols = self.column_types()
        if table not in cols:
            raise SchemaError("unknown table %r" % (table,))
        return cols[table]

    # -- versioned column catalog --------------------------------------------

    @staticmethod
    def _schema_columns(conn, table, version):
        """Ordered column descriptors of *table* visible at snapshot *version*.

        Returns ``None`` when the table has no schema effective at that
        position (it did not exist yet).
        """
        row = conn.execute(
            "SELECT columns_json FROM table_schemas WHERE table_name=? AND effective_version <= ? "
            "ORDER BY schema_version DESC LIMIT 1", (table, version)).fetchone()
        if row is None:
            return None
        return json.loads(row["columns_json"])

    @staticmethod
    def _schema_parts(cols):
        types = {c["name"]: c["type"] for c in cols}
        defaults = {c["name"]: c.get("default") for c in cols if c.get("has_default")}
        return types, defaults

    def schema_at_conn(self, conn, table, version):
        """Connection-local version of :meth:`schema_at` (no new transaction)."""
        cols = self._schema_columns(conn, table, version)
        if cols is None:
            raise SchemaError("unknown table %r at version %s" % (table, version))
        return self._schema_parts(cols)

    def latest_schema_conn(self, conn, table):
        """Connection-local ``(types, defaults, descriptors)`` for the table."""
        if conn.execute("SELECT 1 FROM tables WHERE table_name=?",
                        (table,)).fetchone() is None:
            raise SchemaError("unknown table %r" % (table,))
        cols = self._schema_columns(conn, table,
                                    Store.current_commit_version(conn))
        types, defaults = self._schema_parts(cols)
        return types, defaults, cols

    def schema_at(self, table, version):
        """``(ordered {name: type}, {name: default_value})`` at *version*.

        The type map preserves declaration order; the default map contains
        exactly the columns whose default was declared (which, in this product,
        are precisely the columns added online).  Raises :class:`SchemaError`
        if the table does not exist at that snapshot.
        """
        def _q(conn):
            return self.schema_at_conn(conn, table, version)
        return self.run_tx(_q, readonly=True)

    def latest_schema(self, table):
        def _q(conn):
            return self.latest_schema_conn(conn, table)
        return self.run_tx(_q, readonly=True)

    @staticmethod
    def _sql_literal(value, ctype):
        """Render an already-validated Python default as a SQL literal.

        Identifiers are quoted separately; only validated, JSON-typed domain
        values ever reach here, so the literal cannot carry SQL text.
        """
        if value is None:
            return "NULL"
        if ctype == TYPE_TEXT:
            return "'" + value.replace("'", "''") + "'"
        if ctype == TYPE_BOOL:
            return "1" if value else "0"
        if ctype == TYPE_INT:
            return str(int(value))
        raise SchemaError("type %s cannot carry a non-null default" % ctype)

    def add_column(self, table, column, ctype, default):
        """Online add-column migration in one atomic short transaction.

        Appends one immutable schema version, physically extends the append-only
        history table with a constant default, records an ordered
        ``schema_changes`` event and bumps the source position -- all together,
        so a failure rolls back the whole migration and leaves no half column,
        no event and no fabricated source position.  Existing row images keep
        their business versions; the new column reads back as *default* without
        any row being rewritten.
        """
        self.check_ident(table, "table name")
        self.check_ident(column, "column name")
        if ctype not in COLUMN_TYPES:
            raise SchemaError("unsupported column type %r" % (ctype,))
        # Validate the default against the declared domain *before* opening any
        # write transaction, so a bad default has no side effects at all.
        check_value(default, ctype, "default for %s.%s" % (table, column))

        def _migrate(conn):
            if conn.execute("SELECT 1 FROM materialized_defs WHERE mv_name=?",
                            (table,)).fetchone() is not None:
                from .errors import MaterializedError
                raise MaterializedError(
                    "materialized summary %r is read-only; online add_column is not "
                    "allowed" % table)
            if conn.execute("SELECT 1 FROM tables WHERE table_name=?",
                            (table,)).fetchone() is None:
                raise SchemaError("unknown table %r" % (table,))
            latest = conn.execute(
                "SELECT schema_version, columns_json FROM table_schemas "
                "WHERE table_name=? ORDER BY schema_version DESC LIMIT 1",
                (table,)).fetchone()
            if latest is None:
                raise InternalInvariantError(
                    "table %r has no versioned schema row; the repository catalog "
                    "is inconsistent" % table)
            cols = json.loads(latest["columns_json"])
            names = {c["name"] for c in cols}
            if column in names or column == "row_id":
                raise SchemaError("column %s.%s already exists" % (table, column))
            next_schema_version = int(latest["schema_version"]) + 1
            new_cols = list(cols) + [{"name": column, "type": ctype,
                                      "has_default": True, "default": default}]
            # One ordered position in the shared source history.
            effective = Store.bump_commit_version(conn)
            decl = {TYPE_INT: "INTEGER", TYPE_TEXT: "TEXT", TYPE_BOOL: "INTEGER",
                    TYPE_NULL: ""}[ctype]
            literal = self._sql_literal(default, ctype)
            ddl = 'ALTER TABLE "%s_rows" ADD COLUMN "%s" %s DEFAULT %s' % (
                table, column, decl, literal)
            conn.execute(ddl)
            conn.execute(
                "INSERT INTO table_schemas(table_name, schema_version, effective_version, "
                "columns_json) VALUES (?, ?, ?, ?)",
                (table, next_schema_version, effective,
                 json.dumps(new_cols, sort_keys=True)))
            conn.execute("UPDATE tables SET version=? WHERE table_name=?",
                         (next_schema_version, table))
            # Keep the flat catalog current for existing tooling.
            conn.execute(
                "INSERT INTO table_columns(table_name, position, column_name, column_type) "
                "VALUES (?, ?, ?, ?)",
                (table, len(cols), column, ctype))
            conn.execute(
                "INSERT INTO schema_changes(commit_version, table_name, schema_version, "
                "change_type, column_name, column_type, has_default, default_json, "
                "after_columns_json) VALUES (?, ?, ?, 'add_column', ?, ?, 1, ?, ?)",
                (effective, table, next_schema_version, column, ctype,
                 json.dumps(default, sort_keys=True),
                 json.dumps(new_cols, sort_keys=True)))
            return {"table": table, "column": column, "type": ctype,
                    "default": default, "schema_version": next_schema_version,
                    "commit_version": effective}
        return self.run_tx(_migrate)

    # -- counters ------------------------------------------------------------

    @staticmethod
    def current_commit_version(conn):
        row = conn.execute("SELECT value FROM meta WHERE key='commit_version'").fetchone()
        return 0 if row is None else int(row[0])

    @staticmethod
    def bump_commit_version(conn):
        new = Store.current_commit_version(conn) + 1
        conn.execute("UPDATE meta SET value=? WHERE key='commit_version'", (str(new),))
        return new

    # -- recovery ------------------------------------------------------------

    def recover(self, force=False):
        """Resolve active transactions that never wrote a decision.

        Because the decision is atomic with the data, an undecided transaction
        has no visible writes.  Expired orphans get an explicit
        ``aborted_orphan`` decision and their staging is dropped.  ``force``
        must only be used when no user process is alive.
        """
        def _recover(conn):
            now = time.time()
            if force:
                sql = ("SELECT t.user_txid FROM transactions t "
                       "LEFT JOIN decisions d ON d.user_txid=t.user_txid "
                       "WHERE t.state='active' AND d.user_txid IS NULL")
                params = ()
            else:
                sql = ("SELECT t.user_txid FROM transactions t "
                       "LEFT JOIN decisions d ON d.user_txid=t.user_txid "
                       "LEFT JOIN gc_leases l ON l.user_txid=t.user_txid "
                       "WHERE t.state='active' AND d.user_txid IS NULL "
                       "AND (l.user_txid IS NULL OR l.expires_wall < ?)")
                params = (now,)
            orphans = [r[0] for r in conn.execute(sql, params)]
            for txid in orphans:
                conn.execute("UPDATE transactions SET state='aborted' WHERE user_txid=?",
                             (txid,))
                conn.execute(
                    "INSERT OR IGNORE INTO decisions(user_txid, decision, commit_version, "
                    "decided_wall, evidence_json) VALUES (?, 'aborted_orphan', NULL, ?, ?)",
                    (txid, now, '{"reason":"no durable commit decision"}'))
                self.drop_staging(conn, txid)
            return orphans
        return self.run_tx(_recover)

    @staticmethod
    def drop_staging(conn, txid):
        conn.execute("DELETE FROM staged WHERE user_txid=?", (txid,))
        conn.execute("DELETE FROM gc_leases WHERE user_txid=?", (txid,))


def hist_table(table):
    return table + "_rows"
