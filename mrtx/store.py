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
import random
import re
import sqlite3
import time

from .errors import BusyError, InternalInvariantError, SchemaError
from .values import COLUMN_TYPES

SCHEMA_VERSION = 2

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
            elif int(row[0]) == 1:
                # v1 -> v2 only adds the subscription tables; BASE_DDL has
                # already created them with IF NOT EXISTS.
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
            coldefs = ["row_id TEXT NOT NULL", "version INTEGER NOT NULL",
                       "deleted INTEGER NOT NULL"]
            for name, ctype in columns:
                decl = {"int64": "INTEGER", "text": "TEXT", "bool": "INTEGER", "null": ""}[ctype]
                coldefs.append('"%s" %s' % (name, decl) if decl else '"%s"' % name)
            coldefs.append("PRIMARY KEY (row_id, version)")
            conn.execute('CREATE TABLE "%s" (%s)' % (hist, ", ".join(coldefs)))
            conn.execute('CREATE INDEX IF NOT EXISTS "%s_v" ON "%s"(row_id, version DESC)'
                         % (hist, hist))
            conn.execute("INSERT INTO tables(table_name, version) VALUES (?, 0)", (table,))
            for pos, (name, ctype) in enumerate(columns):
                conn.execute(
                    "INSERT INTO table_columns(table_name, position, column_name, column_type) "
                    "VALUES (?, ?, ?, ?)", (table, pos, name, ctype))
        self.run_tx(_create)
        return self.table_info(table)

    def column_types(self):
        def _read(conn):
            out = {}
            for tname, cname, ctype in conn.execute(
                    "SELECT table_name, column_name, column_type FROM table_columns "
                    "ORDER BY table_name, position"):
                out.setdefault(tname, {})[cname] = ctype
            return out
        return self.run_tx(_read, readonly=True)

    def table_info(self, table):
        cols = self.column_types()
        if table not in cols:
            raise SchemaError("unknown table %r" % (table,))
        return cols[table]

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
