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

# full test suite (stdlib unittest, 184 tests)
cd tests && python3 -m unittest discover -s . -p 'test_*.py'

# the runnable end-to-end drills
python3 -m mrtx.drill                       # transactions: isolation/conflicts
python3 -m mrtx.drill --db /tmp/keep.db     # keep the database
python3 -m mrtx.sync_drill                  # subscriptions + SQLite replica
python3 -m mrtx.sync_drill --keep /tmp/sync # keep both databases
python3 -m mrtx.online_add_column_drill     # online add-column migration
python3 -m mrtx.reference_drill             # reference constraints + cascades
python3 -m mrtx.reference_drill --keep /tmp/ref
```

Each drill exits 0 only if every expectation holds; it prints the actual read
results, commit decisions, conflict evidence, batch contents, cleanup reports
and row-by-row replica comparisons.

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
| `ConflictError`        | serializability/precondition/reference refusal, with `.code` and `.evidence` (codes include `write_conflict`, `read_conflict`, `reference_violation`, `restrict_violation`, `cascade_conflict`) |
| `TransactionError`     | invalid operation for the transaction's state (closed, identity already visible, …) |
| `TransactionNotFound`  | unknown transaction id                                     |
| `SchemaError`          | unknown table/column, wrong type, malformed schema         |
| `ReferenceError`       | malformed/unsupported reference registration or an existing orphan; a `SchemaError` subclass, so existing schema-error handling still catches it |
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

Limits: only data written through `mrtx` is managed; the only supported
schema changes are `create_table` and the single-column online
`add_column` described in the next section (no drop/rename/retype, no
arbitrary SQL).  This is not an arbitrary-SQL engine and not a distributed
transaction protocol.  Readers keep their snapshot version alive; a client
that never commits or aborts a transaction will delay GC until its process
exits or its lease expires — so always close transactions.

Cost model: each `read`/`stage_*` is one short SQLite transaction that
re-evaluates the fixed snapshot for the affected table, so an operation is
`O(rows in that table)` rather than `O(matches)`.  This is a deliberate
simplicity/robustness trade-off for small managed relations; it does not take
a database-wide write lock and does not serialize independent processes.  Under
heavy contention on one identity, committing writers queue only for the
duration of their short commit transaction; a writer that loses the race is
rejected with evidence rather than retried internally.

---

## 7. Change subscriptions (full-then-incremental)

A subscription is a durable server-side cursor that delivers the committed
content of a fixed set of managed tables at a fixed cut version `S`, then the
commits that follow.  It is the building block for a local replica; it never
runs arbitrary SQL, does no row/column filtering, and assumes no network or
external message system.

### Creating and using

```python
from mrtx import Client, Replica

c = Client("source.db")
sub, S = c.create_subscription("replica-1", ["accounts", "ledger"])

# Full phase: pages of complete S images, ordered by table then row_id.
batch = c.pull(sub.handle, max_rows=100)["batch"]
#   batch["phase"]      -> "full"
#   batch["position"]   -> S (the source position covered)
#   batch["items"]      -> [{"table","row_id","version","present":True,"values"}]
#   batch["full_complete"], batch["cursor"], batch["tables"]
c.ack(sub.handle, batch["batch_id"])          # advance to this batch's position

# After the final full page, pull yields per-commit incremental batches:
#   batch["phase"]  -> "incremental"; batch["position"] is the covered commit
#   item -> {"table","row_id","before":{"present",...},"after":{"present",...}}
# then
#   {"batch": None, "caught_up": True, "acknowledged_position": ...}
```

`S` (returned by `create_subscription`, also in `subscription_status`) is
captured **atomically** with the subscription row, so no concurrent cleanup
can slip between choosing `S` and taking retention responsibility for it.

### Contract

* **Full load is exactly the S snapshot.** Each row is the newest image with
  `version <= S`, with `row_id`, its image version and its original typed
  values (`int64`/`text`/`bool`/`null`).  Concurrent updates, deletes and
  re-inserts of rows not yet delivered do not change the pages.  Empty
  subscribed tables are still described (their columns are in the batch's
  `tables` list and in `subscription_status()["schema"]`).
* **Pagination** is by `(table name, row_id)` in Unicode code-point order and
  stops at an explicit `full_complete: true` boundary.  `max_rows` is a soft
  upper bound for full pages.
* **Incremental delivery is per source commit**, in commit order.  A commit
  that changes two subscribed tables arrives as one batch and is applied by a
  replica in one local transaction.  Multiple staging operations on one
  identity collapse to the commit's **net** before/after images.  An absent
  side is tagged `"present": false` with `values: null` and no version — never
  confused with a business `null`.  A commit with no final row change may be
  omitted; a value-identical update that bumps the row version is a change.
  Aborts, orphaned/uncommitted staging and re-sent commits never produce an
  event.  Unrelated-only and read-only commits neither stall nor skip later
  relevant commits: a batch's `position` covers them implicitly.
* **One pending batch.** A batch's `batch_id`, covered position and payload are
  durable.  Re-pulling from the same or another process, with any budget,
  returns byte-identical content until it is acknowledged.
* **Acknowledgement is by batch id only.** There is no client-supplied
  position.  The advanced position is the one durably recorded when the batch
  was produced; re-sending an ack is a no-op; a forged, stale, cross-subscription
  or cross-source credential is rejected.
* **Generation.** `rebuild_subscription` takes a new `S`, increments the
  generation and rotates the token; old-generation pull/ack credentials are
  rejected.  `close_subscription` releases retention responsibility; re-opening
  a closed subscription raises `SubscriptionClosed`, and a rebuild is the only
  way to revive it.
* **Retention and cleanup.** `gc_versions()` retains every version needed to
  reconstruct `S` while a subscription is in its full phase.
  `cleanup_sync_history()` prunes the change log up to the **slowest open
  subscription's** acknowledged position and reports
  `{"reclaimed_changes", "retention_boundary", "remaining_changes", ...}`.
  Closed subscriptions and their private pending copies are released.  No
  long-lived SQLite connection, read transaction or write lock is held while
  offline or between pages; other processes commit normally between pages.

### Replica

`mrtx.Replica` populates a **separate** SQLite database:

```python
rep = Replica("replica.db", sub.handle)
rep.run(c, max_rows=200)          # consume full then incremental

rep.state()["state"]              # "receiving" while the full load streams
rep.rows("accounts")              # raises ReplicaNotReady until "ready"
```

* Business reads are refused (`ReplicaNotReady`) until the final full page is
  applied; then the state becomes `ready`.
* Each batch is applied in **one local transaction** that also records its
  `batch_id`.  The source is acknowledged only after that transaction is
  durable.  A crash between local apply and acknowledgement re-delivers the
  same batch; the replica recognises it and skips re-applying it.  A version
  guard also prevents a stale re-delivery from regressing a newer row.
* This is **not** a cross-database distributed transaction: it is at-least-once
  delivery plus idempotent local application.  See `Replica.apply_local` /
  `Replica.acknowledge` to model the crash seam.
* A changed subscription generation is refused unless you call
  `prepare_generation(handle)` or `consume(..., reinitialize=True)`, so an old
  full load is never mixed with a new one.

---

## 8. Online add-column migration

`Client.add_column(table, column, ctype, default)` adds **one** column to a
managed table while the repository stays online — no table rebuild, no
downtime, and no rewriting of existing rows.  This is deliberately the *only*
schema mutation beyond `create_table`: there is no drop, rename or type change,
and no arbitrary SQL.

```python
result = c.add_column("accounts", "source_group", "text", "organic")
# -> {"table": "accounts", "column": "source_group", "type": "text",
#     "default": "organic", "schema_version": 2, "commit_version": 7}
```

### What existing and new data see

* The migration takes one position in the **same ordered source history** that
  carries row commits (it bumps `commit_version`).  Existing row images keep
  their business `version`; the new column physically exists on the
  append-only history table with a constant `DEFAULT`, so an old row reads the
  default without being updated.  The migration is never disguised as an
  update to every row.
* A transaction that begins after the migration sees the new column immediately.
  An `insert` that omits it, or an `update` that does not name it, receives the
  same default.  An explicitly supplied value is validated by the usual typed
  domain rules (an ill-typed value is rejected).
* A transaction that began **before** the migration keeps reading under its
  start-view column definition: it cannot see, name or filter on the new
  column.  It may still complete its reads.  If it tries to *write* a table
  whose structure changed after it began, the commit is refused with an
  explicit `SchemaConflictError` (`code == "schema_conflict"`, carrying the
  added column(s) and the migration records) and **leaves no write behind**.
  A read-only transaction still commits (it serializes at its start view).
* Validation is all-or-nothing.  An unknown table, a conflicting column name
  (including `row_id`), an unsupported type, or a default that does not match
  the domain raises `SchemaError` and leaves no half column, no catalog entry,
  no fabricated source position and no replica-visible event.

### Versioned catalog

Each table's column definitions are stored as an immutable, ordered
`table_schemas` history; every snapshot version resolves the schema effective
at that point.  `c.table_schemas(table)` lists the schema versions and the
position at which each took effect, and `c.schema_history(table)` returns the
ordered `add_column` events (column, type, default and position).

### How replicas transition without recopying the table

* A subscription's full load is frozen to its cut.  If a migration lands while
  the full load is still paging, the unread pages keep the **old** columns and
  old values — they never gain the new column mid-stream.
* After the final full page, the subscription emits a dedicated
  `schema_change` incremental batch (with the column type and default) **before
  any row commits that follow the migration**, so a consumer switches structure
  first and only then applies new-structure rows.  A subscription created after
  the migration simply describes the new columns directly in its full load.
* On the replica, the schema switch (the local `ALTER TABLE … DEFAULT`, the
  recorded type/default, the current position and the applied-batch id) is one
  local transaction.  The source is acknowledged only after that commit.  If
  the process is killed in between, reopening re-pulls the same batch; the
  replica recognises the batch id and does not add the column twice, lose the
  default, or interpret later rows under the old structure.
* Schema events share the change log's retention: `cleanup_sync_history()`
  reclaims only events at or below the slowest open subscription's
  acknowledged position, so an unfinished full load or an unacknowledged old
  batch always keeps the versions it still needs.  No long-lived read
  transaction or write lock is held across pages.

A runnable, real-process demonstration (paused full load, mid-stream migration,
structural-conflict transaction, invalid-migration rollback and the
crash/replay seam) is in `python3 -m mrtx.online_add_column_drill`.

---

## 9. Managed reference constraints and commit-time cascades

A **reference constraint** declares that one `text` column of a child table
references the implicit `row_id` of a parent table.  The two tables may be the
same (self reference), several tables may form a ring, and one row may point at
several parents through different columns.  A business `null` means "no
reference"; every other string must match exactly one **live** parent row,
compared by Unicode code point with no normalization and case folding.  Two
delete strategies exist: `restrict` and `cascade`.  The feature does not use
SQLite's native foreign-key switch — the repository manages logical,
append-only, versioned history itself.

### Registration and querying

```python
c.register_reference(
    "entry_account",            # unique constraint name
    parent_table="accounts",
    child_table="entries",
    child_column="acct_id",     # must be a declared text column
    on_delete="cascade")        # "restrict" | "cascade"

c.list_references()             # every durable declaration, ordered by name
c.get_reference("entry_account")
```

Registration is one atomic short write transaction.  It is explicitly refused,
leaving **no half definition**, when:

* the name is already used, or the same child column already carries a
  constraint (one managed reference per column);
* a named table or column does not exist, or the column is not `text`;
* `on_delete` is neither `restrict` nor `cascade`;
* the already-committed data contains an orphan (a non-null value with no live
  parent).  The error names the first offending child row and missing parent.

The declaration is durable: it survives process restart and governs every
later commit.  A write transaction that began **before** registration and
commits **after** it is still validated by the new rule; a read-only
transaction that began earlier keeps its historical start view and completes
unchanged.  Constraint deletion, arbitrary SQL, other key types and
background-built constraints are intentionally not supported.

### Deferred, final-state checking

Reference validation runs at `commit`, never while staging.  One transaction
may therefore insert the child before the parent, hold a transient orphan and
repair it, or delete a parent and insert it back under the same `row_id` —
none of these intermediate states can reject a staging call.  The decision is
made against the **complete business state the commit would produce**.  If the
final state still contains an invalid reference, the whole transaction fails:
the refusal evidence (`ConflictError`, code `reference_violation`) names the
constraint, the child identity and the missing parent, and every otherwise
valid write staged in that transaction is rolled back.  Re-sending the commit
returns the same durable decision.

### Cascade and restrict at commit time

When a parent identity is explicitly deleted and does not reappear in the
final state:

* every `cascade` edge deletes the still-pointing child rows, then propagates
  from those identities transitively.  The delete set is a fixpoint computed
  with a worklist and a visited set, so an identity reached through a self
  loop, a multi-table ring or a diamond is deleted **exactly once**, and every
  implicit delete carries its auditable propagation source
  (`constraint`, `parent_table`, `parent_id`, `depth`);
* a parent deleted and re-inserted with the same `row_id` is not a cascade
  root, and a child re-pointed at another live parent inside the transaction
  is not deleted because of its old reference;
* `restrict` is evaluated **after** the full cascade set is known.  A
  surviving row that still references a deleted identity rejects the commit
  (`restrict_violation`), but a referrer that is itself deleted — explicitly or
  by the cascade — never blocks it.  A `restrict` edge is never silently
  executed as a cascade.

Implicit cascade deletes are real writes inside the **same** atomic commit as
the user's explicit writes: one shared source position, one appended
tombstone version, one `committed_writes` evidence row and one subscription
change per identity.  They obey the ordinary version precondition: a child row
inserted or changed by another transaction after this snapshot cannot be
silently removed — the later committer is rejected with
`cascade_conflict` evidence naming the concurrent writer.  A row that this
same transaction inserted and the cascade then removes never existed in the
committed state, so it leaves no fabricated history row or change event (it is
reported once with `ephemeral: true`).

### What the commit result and subscriptions show

```python
result = tx.commit()
# result["writes"]          explicit user writes, op insert|update|delete
# result["cascade_deletes"] implicit deletes, op "cascade_delete", each with
#                            constraint, propagation_source, depth, ephemeral
# every entry in both lists shares result["commit_version"]
```

Reads before the commit keep the existing "start view plus own staged
operations" behavior; they never predict cascade results.  A subscription that
covers an affected table receives the whole net change — explicit and implicit
deletes together — as **one** incremental batch at one source position, even
when it exceeds the pull budget.  The replica applies it in one local
transaction and never re-derives references or cascades itself.  A committed
decision is durable before the response: reopening and re-sending the commit,
or re-delivering an unacknowledged replica batch, neither deletes twice nor
advances the source position again.

A runnable demonstration of every rule (hand-computed delete sets, barrier
races in real processes, the replica mirror and both replay seams) is in
`python3 -m mrtx.reference_drill`.

---

## 10. Repository layout

```
mrtx/
  __init__.py     public API and predicate builders
  values.py       typed domains, AST, 3-valued evaluation
  canonical.py    stable predicate/row encodings for evidence
  store.py        SQLite schema, short transactions, bounded retry, recovery,
                  versioned column catalog and online add_column
  engine.py       snapshots, staging, OCC validation, commit/abort, GC
  references.py   durable reference catalog, deferred checks, cascade planner
  subscription.py durable full-then-incremental cursor, schema-change stream
  replica.py      separate-SQLite replica with idempotent local apply
  errors.py       exception types
  drill.py        runnable transaction-level demonstration
  sync_drill.py   runnable subscription + replica demonstration
  online_add_column_drill.py  runnable online-migration demonstration
  reference_drill.py          runnable reference-constraint demonstration
tests/
  support.py      temp-repo fixtures and drain helpers
  procutil.py     real OS-process harness (JSON-line pipes)
  worker.py       child process command loop (transactions and subscriptions)
  serialcheck.py  independent serializability checker (not used by mrtx)
  test_*.py       unit, concurrency, process, subscription, replica, migration,
                  reference-constraint tests
docs/
  DESIGN.md       design notes, serializability argument, sync retention notes
```

---

## 11. API quick reference

```python
from mrtx import Client, eq, lt, le, is_null, and_, or_, not_, TRUE, FALSE
from mrtx import (ConflictError, TransactionError, SchemaError,
                  SchemaConflictError, ReferenceError, PredicateError,
                  BusyError)

c = Client(db_path)                      # opens/creates schema, recovers orphans
c.create_table(name, [(col, type), ...]) # int64 | text | bool | null
c.tables()                               # {table: {column: type}}
c.add_column(table, column, ctype, default)
                                         # online one-column migration; returns
                                         # {schema_version, commit_version, ...}
c.table_schemas(table)                   # immutable schema versions + positions
c.schema_history(table=None)             # ordered add_column events

# --- managed reference constraints ----------------------------------------
c.register_reference(name, parent_table, child_table, child_column, on_delete)
                                         # on_delete: "restrict" | "cascade";
                                         # validates existing data, then persists
c.list_references()                      # durable declarations, ordered by name
c.get_reference(name)                    # one declaration, or None

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

# --- change subscriptions -------------------------------------------------
from mrtx import (Subscription, SubscriptionHandle, Replica,
                  SubscriptionError, SubscriptionNotFound, SubscriptionClosed,
                  StaleSubscriptionGeneration, ReplicaNotReady,
                  ReplicaGenerationError)

sub, S = c.create_subscription(name, [table, ...])  # fixed cut S, generation 1
sub = c.open_subscription(handle)        # resume durable progress; closed -> error
sub = c.rebuild_subscription(handle)     # new cut, new generation, new token
c.subscription_status(handle)            # phase, cut, ack position, pending batch
c.list_subscriptions()

result = c.pull(handle, max_rows=100)    # {"batch": {...}|None, "caught_up": bool}
c.ack(handle, batch_id)                  # advance to the batch's recorded position
c.close_subscription(handle)             # release retention responsibility
c.cleanup_sync_history()                 # prune by slowest open ack; returns report

rep = Replica(replica_path, handle)      # separate SQLite database
rep.run(c, max_rows=200)                 # full then incremental, idempotently
rep.apply_local(c) / rep.acknowledge(c)  # expose the crash seam explicitly
rep.rows(table) / rep.tables() / rep.state()
rep.prepare_generation(handle)           # explicit reinit on a generation change
```

`SubscriptionHandle` (`{sub_id, generation, token, db_path}`) is picklable and
proves ownership; it is bound to the source path and generation.  Batch ids are
the only acknowledgement credential; clients never supply positions.

`with c.begin() as tx:` commits on clean exit and aborts on an exception.
