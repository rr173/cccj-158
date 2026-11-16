# Design notes

## 1. Physical layout

```
meta(key, value)                    schema_version, seq, commit_version
tables(table_name, version)         managed relations
table_columns(table, pos, name, type)
transactions(txid, state, base_version, token_hash, owner_pid, owner_start, …)
decisions(txid, decision, commit_version, decided_wall, evidence_json)
rejection_log(txid, code, evidence_json, at_wall)
staged(seq, txid, table, row_id, op, old_values, new_values, base_version, …)
predicate_reads(seq, txid, table, predicate_json, rows_json, columns_json,
                snapshot_version)
tx_ops(txid, ord, kind, table, detail_json)      ordered read/write record
committed_writes(ordinal, txid, table, row_id, op, old/new values,
                 old/new version, commit_version)
gc_leases(txid, expires_wall)
<relation>_rows(row_id, version, deleted, <declared columns>)  history
```

`commit_version` is a monotonically increasing counter.  Every committed
transaction gets the next value, and each of its row images is stamped with it.
A snapshot at version V is: for each `row_id`, the newest history entry with
`version <= V`.

Why history instead of in-place rows: user transactions are long and overlap,
and a snapshot must remain stable while later commits happen.  Append-only
versions give that without any long-lived SQLite lock.

## 2. Serializability argument

Let transactions serialize at points `p(T)`:

* if T has writes, `p(T) = commit_version(T)` (commit order);
* if T has no writes, `p(T) = base_version(T)` and it orders with phase 1 so it
  follows commits exactly at its snapshot.

Claim: for every commit the engine accepts, replaying the committed
transactions in `p` order reproduces each transaction's recorded read results
exactly.

Proof sketch.  Take a committing T with writes, and consider any other
committed U with `p(U) > p(T)` (so U committed after T's snapshot and before
T's commit).  Three cases cover every way U could change what T observed:

1. *Same identity.* If U wrote an identity T writes, validation case (1)
   rejects T.
2. *T's read.* If U changed a row T returned, in a column T projected or
   filtered, or changed the predicate's truth for any identity, validation
   case (2) rejects T.  Because U's commit is a single atomic change over its
   final per-identity image, checking U's `committed_writes` against T's
   recorded predicates is exact.
3. *Otherwise* U has no causal influence on anything T read, so T's
   observations are the same whether U runs before or after T; the order at
   `p(U)` is consistent.

For a read-only T, no other transaction observes its result, so its placement
at `base_version` is always valid; the engine never rejects such a
transaction (this is why the phantom tests make the reader also write — a
read-only reader cannot be forced to reject, and need not be).

Conversely, validation rejects only when one of cases 1–2 actually holds, so
two transactions with disjoint key-range reads/writes have no edge and both
commit.  The rule is conservative at the level of a predicate's *space*
(changing a filtered column invalidates the reader even if the value would not
alter the result set), which is sound because it only ever refuses, never
accepts a history lacking a serial order.

The independent checker in `tests/serialcheck.py` does not use this argument;
it enumerates permutations, replays operations in recorded user order
(including read-your-own-writes), enforces real-time precedence from durable
timestamps, and compares observed reads.  Randomized histories assert that a
serial order exists whenever the engine committed at least one transaction,
and that the claimed serialization points replay exactly.

## 3. Atomic commit and recovery

`commit` opens one `BEGIN IMMEDIATE` transaction and inside it:

1. reads the durable decision; if present, returns it (idempotent replay);
2. validates;
3. appends row versions, writes `committed_writes` evidence;
4. writes `decisions` (decision + version) and flips `transactions.state`;
5. drops staging and the lease.

SQLite commits all of that atomically.  Crash cases:

* before commit: nothing durable → `recover()` later writes
  `aborted_orphan`, status is deterministically "not committed";
* after commit: decision and data both present → any process can answer
  `committed`, and a resent commit replays.

Ownership liveness uses PID + `/proc/<pid>/stat` start time, so PID reuse does
not keep a dead transaction alive (which would otherwise pin GC's safe point).
A periodic lease (`gc_leases`) is a backstop for machines without `/proc`; it is
extended on each operation.

## 4. GC safety

`safe = min(base_version)` over active transactions whose owner is alive (or
current version if none).  Keep per identity: newest version `<= safe`, all
versions `> safe`, newest overall.  Anything else cannot be read by a live
snapshot.  Validation reads `committed_writes`, never history, so GC cannot
change a commit decision.  A stale safe point derived from a dead owner cannot
occur because dead owners are excluded from the minimum.

## 5. Busy handling

Every staging and commit operation is a short `BEGIN IMMEDIATE` transaction;
reads are deferred snapshots with `PRAGMA query_only`.  `busy`/`locked`
errors are retried with jittered exponential backoff and then raised as
`BusyError`, which has no `evidence` attribute and creates no decision.  A
test holds an external SQLite write lock to confirm this behaviour.
