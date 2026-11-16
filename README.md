# Managed-repository transaction layer (mrtx)

A from-scratch, SQLite-backed transaction layer with a public
`begin / read / stage_insert / stage_update / stage_delete / commit / abort`
API.  It provides genuine snapshot isolation for user transactions that can
live in different OS processes, a conservative but *evidence-carrying*
serializability check, durable all-or-nothing commit decisions, and safe
version cleanup.

It manages only data written through this module.  It does not execute
arbitrary SQL and does not implement distributed transactions.

---

## 1. Install and run

Requirements: Python 3.8+ (developed and tested on 3.11), standard library
only (no third-party dependencies).  SQLite is the `sqlite3` module's bundled
library.

```bash
# from the directory containing the mrtx/ package
python3 -c "import mrtx; print(mrtx.Client)"

# full test suite (stdlib unittest, 53 tests)
cd tests && python3 -m unittest discover -s . -p 'test_*.py'

# the runnable end-to-end drill
python3 -m mrtx.drill
python3 -m mrtx.drill --db /tmp/keep.db     # keep the database
```

The drill exits 0 only if every expectation holds; it prints the actual read
results, commit decisions, conflict evidence and final rows.

---

## 2. Data model

Managed relations have a **stable, non-empty text `row_id`** (implicit, never
declared by the caller) and columns whose declared domains are:

| type    | accepted values                                   |
|---------|---------------------------------------------------|
| `int64` | Python `int` in [-2^63, 2^63-1], excluding `bool`  |
| `text`  | `str`, compared by Unicode code point (no locale)  |
| `bool`  | `True`/`False` only, distinct from integers        |
| `null`  | `None` only                                        |

Every domain also accepts `None` (the typed `null` value).  `bool` and `int`
are disjoint even though `bool` subclasses `int`; all validation checks
`bool` first.

```python
from mrtx import Client

c = Client("repo.db")
c.create_table("accounts", [("owner", "text"), ("balance", "int64"),
                            ("active", "bool"), ("note", "null")])
```

### Predicates

`read` accepts a whitelist of columns and a predicate built from
`eq/lt/le/is_null` with `and_/or_/not_` and the `TRUE`/`FALSE` literals.
Evaluation uses SQL **three-valued logic**; only rows whose predicate is
TRUE are returned.  Predicates are validated against the table's declared
types, so an ill-typed comparison can never silently evaluate to NULL.

```python
from mrtx import eq, lt, is_null, and_, or_, not_

rows = tx.read("accounts", columns=["row_id", "owner", "balance"],
               where=and_(eq("active", True), lt("balance", 100)))
```

Reads return **complete rows in a deterministic order** (sorted by `row_id`),
each with its version (`version`), and are checked against the transaction's
fixed start view.

---

## 3. Isolation model

### Fixed start view

`begin` records `base_version` — the committed-version counter at that moment.
A read materializes, per `row_id`, the newest history entry with
`version <= base_version`, then overlays the transaction's own staged writes
and deletes.  Therefore:

* the snapshot is **fixed** for the transaction's lifetime;
* the transaction **sees its own writes** immediately, with a negative local
  version token that is never a commit version;
* other transactions' uncommitted staging is invisible;
* no SQLite write lock is held between operations, and two long user
  transactions in different processes genuinely overlap (a test asserts
  `begin` and independent commits proceed while another transaction is open).

`begin` itself takes a short write transaction (to allocate the transaction
row and snapshot); it does not queue behind other user transactions.

### Commit decision

`commit` is one short SQLite transaction.  A transaction is committed iff it
is serializable at a serialization point consistent with real time.  Committed
transactions serialize in **commit order**, except that a transaction with no
writes serializes at its start view (it cannot invalidate anyone).  When a
transaction with writes commits, it is validated against every write committed
after its snapshot:

1. **Same identity (ww / precondition).**  If another transaction committed a
   change to an identity this transaction writes, the commit is refused.  A
   write never silently overwrites another committed change to the same
   identity.  Explicit `expected_version` preconditions are enforced the same
   way (and immediately at `stage_*` when the start view already disagrees).
2. **Read invalidation (rw).**  For every read (including empty results), if a
   later commit changed a returned row in a projected or filtered column, or
   changed the predicate's truth value for any row — a phantom, a
   disappearance, or a new match — the commit is refused.  This is checked
   across all tables the transaction read, so cross-table read→write
   dependencies are covered.

Multiple operations on the same identity inside one transaction are applied in
**user order** and committed as one atomic change.

This rule is not "write sets intersect": two transactions that read the same
predicate but update different rows conflict through read invalidation, while
two transactions on the same start view whose reads and writes concern
**disjoint key ranges** have no dependency and both commit directly, with no
client replay.

### Evidence, not "stale version"

Every refusal carries structured, durably stored evidence naming the actual
transactions, the row or predicate, and the dependency kind:

```json
{
  "code": "read_conflict",
  "transaction": "tx-…",
  "summary": "read of accounts (predicate active = true) was invalidated by tx-…'s commit 2 to accounts/A",
  "conflicts": [{
    "dependency": "rw",
    "table": "accounts", "row_id": "A",
    "predicate_expression": "active = true",
    "other_transaction": "tx-…",
    "other_commit_version": 2,
    "matches_at_snapshot": true, "matches_after_other_commit": false,
    "witness": {"identity": ["accounts", "A"]}
  }]
}
```

A three-transaction ring over two tables is reported in
`evidence["dependency_cycles"]` with each edge's witness; inspect any history
with `c.dependency_graph()`.

### Errors

| exception              | meaning                                                    |
|------------------------|------------------------------------------------------------|
| `ConflictError`        | serializability/precondition refusal, with `.code` and `.evidence` |
| `TransactionError`     | invalid operation for the transaction's state (closed, identity already visible, …) |
| `TransactionNotFound`  | unknown transaction id                                     |
| `SchemaError`          | unknown table/column, wrong type, malformed schema         |
| `PredicateError`       | malformed or ill-typed predicate                           |
| `BusyError`            | internal SQLite lock contention after bounded retries — **never a conflict** |

SQLite `busy`/`locked` is retried with bounded backoff and then surfaced as
`BusyError` (an internal liveness condition).  It never produces a conflict
decision and leaves no commit decision behind.

---

## 4. Processes and crash recovery

`TransactionHandle` is a small JSON-serializable value
(`{"txid", "token", "db_path"}`).  The token proves ownership; everything else
lives in SQLite.  A handle can be sent to another process, so a transaction can
outlive the process that began it.

* The commit decision and the new row versions are written in the **same**
  short SQLite transaction as the transaction's state change, so a crash
  leaves either the whole commit or nothing.
* `c.transaction_status(txid)` is answerable from any process at any time and
  returns `committed: true/false` plus the commit version.
* Re-sending `commit` with the same transaction id is **idempotent**: it
  returns `{"replayed": true, ...}` and writes nothing.
* If a process dies before deciding, `Client.recover()` (called on start by
  default) records an explicit `aborted_orphan` decision once the owner process
  is gone (checked via PID **and** `/proc` start time, so PID reuse cannot pin a
  transaction forever) or its lease has expired.  The transaction then
  deterministically reports "not committed", its staging is dropped, and a
  resent commit cannot write.

The transaction identity is bound to its token; it cannot be rebound to a
different operation set.

---

## 5. Version cleanup boundaries

Rows are stored append-only (`<table>_rows`), so old versions accumulate.
`c.gc_versions()` prunes them at a safe boundary:

* `safe = min(base_version)` over **live** active transactions (owner process
  alive), or the current version if none;
* for each identity keep (a) the newest version `<= safe` (the oldest live
  snapshot needs it), (b) every version `> safe` (newer live snapshots may need
  any of them), and (c) the newest version overall;
* delete all other versions.

Read validation never consults history (it uses the durable
`committed_writes`/`predicate_reads` evidence), so pruning **cannot change a
commit decision**.  A test verifies a conflict is still detected identically
after a GC pass.

Safety rules: never call `gc_versions()` with a fabricated or stale
`safe_point`; only live transactions (as judged by owner-process liveness)
raise the safe point.  `Client.recover()` is the only cleanup of *undecided*
transactions; `force_recover()` exists for test teardown with no live user
process and must not be used while clients are running.

---

## 6. Guarantees and limits

Guaranteed:

* fixed snapshot per transaction; read-your-own-writes; staging invisible to
  others;
* atomic, durable, idempotent commit decisions; crash-recoverable status;
* every successful history is serializable at a point consistent with real
  time (commit order for read-write transactions; start view for read-only
  ones), and disjoint-range transactions on one snapshot both commit without
  replay;
* a rejected commit returns concrete evidence, not a bare version mismatch.

Limits: only data written through `mrtx` is managed; schema changes are
supported only through `create_table`; this is not an arbitrary-SQL engine and
not a distributed transaction protocol.  Readers keep their snapshot version
alive; a client that never commits or aborts a transaction will delay GC until
its process exits or its lease expires — so always close transactions.

Cost model: each `read`/`stage_*` is one short SQLite transaction that
re-evaluates the fixed snapshot for the affected table, so an operation is
`O(rows in that table)` rather than `O(matches)`.  This is a deliberate
simplicity/robustness trade-off for small managed relations; it does not take
a database-wide write lock and does not serialize independent processes.  Under
heavy contention on one identity, committing writers queue only for the
duration of their short commit transaction; a writer that loses the race is
rejected with evidence rather than retried internally.

---

## 7. Repository layout

```
mrtx/
  __init__.py     public API and predicate builders
  values.py       typed domains, AST, 3-valued evaluation
  canonical.py    stable predicate/row encodings for evidence
  store.py        SQLite schema, short transactions, bounded retry, recovery
  engine.py       snapshots, staging, OCC validation, commit/abort, GC
  errors.py       exception types
  drill.py        runnable end-to-end demonstration
tests/
  support.py      temp-repo fixtures
  procutil.py     real OS-process harness (JSON-line pipes)
  worker.py       child process command loop
  serialcheck.py  independent serializability checker (not used by mrtx)
  test_*.py       unit, concurrency, process, liveness, randomized tests
docs/
  DESIGN.md       design notes and the serializability argument
```

---

## 8. API quick reference

```python
from mrtx import Client, eq, lt, le, is_null, and_, or_, not_, TRUE, FALSE
from mrtx import ConflictError, TransactionError, SchemaError, PredicateError, BusyError

c = Client(db_path)                      # opens/creates schema, recovers orphans
c.create_table(name, [(col, type), ...]) # int64 | text | bool | null
c.tables()                               # {table: {column: type}}

tx = c.begin(txid=None)                  # fixes the start view; returns Transaction
rows = tx.read(table, columns=None, where=None)
                                         # [{row_id, version, <projected cols>}, ...]
tx.stage_insert(table, row_id, values)   # values: {col: value}
tx.stage_update(table, row_id, values, expected_version=None)
tx.stage_delete(table, row_id, expected_version=None)
tx.commit()                              # -> {"status": "committed", "commit_version": N, ...}
tx.abort()                               # -> {"status": "aborted", ...}

tx.handle                                # picklable {"txid","token","db_path"} for re-sends

c.transaction_status(txid)               # {"decision","committed","commit_version",...}
c.rejection_evidence(txid)               # {"code","evidence"} or None
c.dependency_graph()                     # {"nodes","edges","cycles"} from evidence
c.rows(table)                            # latest committed images
c.history(table, row_id)                 # committed versions of one identity
c.operations(txid)                       # ordered read/write record
c.gc_versions()                          # prune unreachable versions safely
c.recover()                              # decide orphans (dead owners / expired leases)
```

`with c.begin() as tx:` commits on clean exit and aborts on an exception.
