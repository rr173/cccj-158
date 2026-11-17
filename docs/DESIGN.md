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
reference_constraints(constraint_name, parent_table, child_table,
                      child_column, on_delete, registered_version, created_wall)
materialized_defs(mv_name, state, left_table, right_table, join columns,
                  join_kind, definition_json, output_columns_json, cut_version,
                  maintained_version)
materialized_builds(mv_name, cut_version, left/right cursor, phase,
                    catchup_from_version)
materialized_groups(mv_name, group_token, group_key_json, active)
materialized_accum(mv_name, group_key_json, group_token, measures_json)
materialized_group_keys(mv_name, group_key_json, join_key, null_scope)
materialized_indexes(table_name, column_name, refcount)
materialized_maintenance_stats(mv_name, commit_version, phase, source rows read,
                               helper records read, groups written)
<relation>_rows(row_id, version, deleted, <declared columns>)  history
<summary>_rows(row_id, version, deleted, <output columns>)     summary history
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

## 6. Change subscriptions: full-then-incremental hand-off

### Durable cursor and the two-phase delivery

A subscription row fixes three things in one `BEGIN IMMEDIATE` transaction:
its `sub_id`/token, generation 1, and the cut `S = meta.commit_version`.  Doing
all three together closes the window in which a concurrent commit could land
*after* `S` was read but *before* retention responsibility existed: because
the write lock is held, a concurrent committer either commits entirely before
`S` (and is part of the full load) or entirely after the subscription row is
durable (and is logged for the incremental phase).

The full phase is a keyset scan over the append-only history: for each
subscribed table, per `row_id`, the newest version `<= S`, tombstones removed,
ordered by `(row_id)` and tables in code-point order.  Because it filters on
`S` rather than "now", updates/deletes/re-inserts that happen between pages do
not alter not-yet-delivered rows.  The scan is one short read transaction per
page; no connection or lock spans pages, so other processes commit normally
between pages.

The incremental phase reads `sub_changes`, the commit-time change log.  The
engine writes one row per identity per committed transaction *in the same
atomic commit* that appends the row versions and the durable decision, but
only for tables covered by some open subscription.  A transaction that staged
the same identity several times has already been folded to one effective
image by `_staged_effective`, so the log holds the commit's net before/after
images.  A pure insert-then-delete of a new identity is omitted (no final
change).  Because the log is written with the commit, aborts, orphans and
resent commits can never appear in it.

Batches are produced one source commit at a time.  The batch's `position` is
that commit's `commit_version`; its `from_position` is the acknowledged
position it extends.  Commits that changed no subscribed table (unrelated or
read-only) are skipped by the `MIN(commit_version) ... table_name IN (...)`
query, so they neither block nor cause a skip of a later relevant commit — the
later batch's position simply covers them.

### Single pending batch and acknowledgement

`subscription_pending` + `subscription_items` hold at most one batch per
subscription.  A pull first returns that batch unchanged; only after an
acknowledgement is it deleted and the next one built.  An acknowledgement is
`(sub_id, batch_id)` only.  The position advanced is read from the pending
row's durable `position` — a client cannot name a position.  The token proves
ownership and is bound to the source path and generation.  Re-sending an ack
for the last acknowledged batch hits the replay branch and writes nothing.

`rebuild_subscription` (explicit only) deletes the pending copy, increments
the generation, rotates the token and captures a new `S`; the old handle's
generation no longer matches, so old pull/ack credentials fail.  `close`
sets the state to `closed` and drops the private pending copy; a closed
subscription is excluded from retention calculations and from the commit-time
watch set, and `open_subscription` refuses it while `rebuild` can revive it.

### Retention

Two needs are derived from open subscriptions:

1. **Full phase needs row history at `S`.**  `gc_versions` treats each
   unfinalised subscription's cut like a live read snapshot at `S` and includes
   it in the `min` that defines the safe point.  When the full load finishes,
   that need disappears.
2. **Incremental phase needs `sub_changes > ack`.**  `cleanup_sync_history`
   computes `boundary = min(ack_version)` over all open subscriptions and
   deletes `sub_changes` with `commit_version <= boundary`; with no open
   subscription the whole log is reclaimable.  Using the minimum (not the
   fastest consumer) means a slow consumer never loses a change.  The report
   states the reclaimed count and the honoured boundary; cleanup is real
   reclamation, not a "never delete" policy.

There is no private full-load copy: the full load reads the same history that
`gc_versions` already knows how to retain, so there is nothing extra to release
at the end.  The only private per-subscription state is the (at most one)
pending batch, which is deleted on ack and on close.

### Replica recovery

A replica is a separate SQLite file.  Applying a batch is one local
`BEGIN IMMEDIATE` that writes the row images and inserts the `batch_id` into
`replica_applied`.  The source ack happens only after that commit.  If the
process dies in between, the source still has the batch pending, so the same
batch is re-pulled; the replica sees the `batch_id` already present, records
that it replayed it, and does not re-apply.  This is at-least-once delivery
plus an idempotent local apply — not a cross-database atomic commit.  A
version guard in the row writer is a second line of defence: an older image
never overwrites a newer one.  A generation change is refused unless the
caller explicitly reinitialises, so the old full load and a new full load are
never interleaved.

## 7. Online add-column migration

### Versioned column catalog

The flat `table_columns` catalog is retained for tooling, but the authority for
what a snapshot may see is `table_schemas`, an append-only, per-table history of
immutable column descriptors.  Each row fixes an ordered column list (name,
type, and any declared default) and the `effective_version` — the source
position at which that shape took effect.  Resolving the schema of a table at
snapshot `V` is "the newest `table_schemas` row with `effective_version <= V`".
The initial `create_table` shape is schema version 1 effective at position 0, so
a transaction beginning on an empty repository already resolves its tables.

Because every read materializes rows through the schema effective at the
transaction's `base_version` rather than the live catalog, a transaction keeps
its start-view columns for its whole life.  No long-lived lock is involved: the
resolution is a point query inside the same short transaction that performs the
read.

### Why no row rewrite

`add_column` runs one `BEGIN IMMEDIATE` transaction that (1) validates the table
and column name and the default against the declared domain, (2)
`ALTER TABLE … ADD COLUMN … DEFAULT <literal>` on the append-only history
table, (3) appends the next immutable `table_schemas` row, (4) inserts a
`schema_changes` event, and (5) bumps `commit_version`.  SQLite fills the new
physical column with the constant default for existing rows without touching
their content or their `version`, so an old image is still stamped with its
original business version yet reads the default under a new enough snapshot.
The migration therefore cannot be mistaken for an update to every row, and a
failed validation rolls back the entire short transaction, leaving no half
column, event or position.

The default literal is built only from a value already validated against one of
the four domains; identifiers are quoted, so no free-form SQL can reach the
`ALTER`.  Only `add_column` is ever written to `schema_changes`; the product
exposes no drop, rename or retype.

### Structural conflicts and the serial order

A schema change occupies an ordinary position in the same ordered stream as
row commits.  Consider a transaction `T` that began at `s(T)` and a migration
`M` at position `m > s(T)` on a table `T` writes.  `T`'s staged values conform
to the old column set; letting them commit after `M` would write an image that
silently predates the current structure.  Instead the engine compares the
column set effective at `s(T)` with the live set when `T` stages a write (and
defensively again inside the atomic commit): if they differ it raises a
`schema_conflict` with evidence naming the migration and the added columns, and
writes nothing.  Read-only transactions and reads inside `T` are unaffected and
still serialize at the start view, so the rule refuses only actions that cannot
be placed in a single consistent serial order.  Writers to other tables and
non-conflicting data commits proceed normally while migration runs, since the
migration holds only a short write lock.

Inserts that omit the new column are filled with the declared default at
staging time, so every committed image is complete under its own schema; an
explicit value goes through the normal `check_value` validation.  This keeps
the append-only writer (which names every declared column) unchanged.

### Subscription and replica hand-off

A subscription freezes both the column types and the declared defaults at its
cut.  A full load therefore always describes the cut's shape, and a migration
landing mid-pagination cannot alter unread pages: they are computed from history
at `S` with the frozen descriptor.  After the full load completes, the
incremental builder consults `sub_changes` and `schema_changes` together and
emits whichever event has the earliest position.  A migration commit never also
carries row changes, so the earliest position is unambiguously one kind, and the
consumer always receives the structural switch before the row commits that
follow it.  A subscription created after the migration freezes the new shape and
never needs the event.

On the replica the schema event is applied as one local transaction: the
idempotent `ALTER TABLE … ADD COLUMN … DEFAULT`, an update of the locally
recorded schema/default metadata, the new position and the `batch_id` all commit
together, and the source ack follows only afterwards.  A crash before the ack
redelivers the same batch; the recorded `batch_id` makes the local apply a
no-op and the column is never added twice, while later row batches are written
through the replica's current (post-switch) schema.  Both change logs share one
retention boundary — the minimum acknowledged position of open subscriptions —
so a slow full load, a pending old batch and an in-flight old transaction keep
exactly the structural and row versions they still need, and explicit cleanup
reclaims the rest and reports the boundary.

## 8. Managed reference constraints

### Durable catalog and registration atomicity

`reference_constraints` is an ordinary durable catalog table (schema v4): one
row per unique constraint name recording the parent table, the child table and
the single `text` child column, and the `restrict`/`cascade` strategy.  The
SQLite native foreign-key switch is deliberately irrelevant: the product
manages logical, append-only, versioned history, so validity is defined over
live logical rows rather than physical row existence.

`register_reference` validates the declaration shape before opening any write
transaction, then runs one `BEGIN IMMEDIATE` transaction that (1) re-checks the
unique name and the one-constraint-per-column rule against the durable rows,
(2) scans the newest committed live images of the child table and rejects the
registration if any non-null value has no live parent, and (3) inserts the
catalog row.  Because the scan and the insert share one write-locked
transaction, a concurrent committer cannot land an orphan in the gap, and any
failure rolls back the whole short transaction — there is never a half
definition.  Registration does not itself consume a `commit_version`
position; it records the position current at registration time for
informational purposes.

### Why in-flight transactions cannot escape a new rule

Consider transaction `T` that began before a registration and commits after it.
`T`'s reference check does not run at `begin` or while staging; it runs inside
`T`'s own commit transaction, where the durable `reference_constraints` rows
are read anew.  Two cases cover every interleaving:

1. `T` commits before the registration transaction takes the write lock: the
   catalog row is not yet visible, so `T` is correctly checked against the
   rules that existed in real time before its serialization point.
2. The registration commits first: when `T` later enters its atomic commit it
   reads the new catalog row and plans against the current committed state, so
   an orphan staged before registration is rejected at commit.  A read-only
   `T` has no write set and serializes at its start view, so it never
   revalidates and its historical reads are untouched.

This is the same lock-and-recheck structure that makes schema migrations and
subscriptions safe; no background build task or table lock is needed.

### Final-state planning inside the commit transaction

`references.plan_commit` is invoked after the ordinary OCC validation and
structural guard, inside the same short SQLite transaction.  It materializes
the *final* business state: for every table reachable through a constraint,
the newest committed live images (at the current version, so concurrent
commits are visible) overlaid with the committing transaction's folded staged
changes.  All subsequent decisions are pure functions of that state and the
durable catalog, which makes a re-sent commit reconstruct the identical
decision and cascade set.

1. *Roots.* An identity is a root when its folded staged op is a delete and it
   is absent from the final view.  A delete followed by a same-id re-insert is
   not a root (the parent exists at the end); an insert-then-delete of a brand
   new identity *is* a root, because the rule is evaluated on the final state.
2. *Cascade fixpoint.* Starting from the roots, each `cascade` edge adds the
   currently pointing children of every deleted identity to a worklist,
   guarded by a visited set.  The visited set is exactly the de-duplication
   argument for self loops, multi-table rings and diamonds: an identity is
   expanded at most once, so the traversal always terminates and each identity
   is deleted once.  The record kept for an identity names the first path that
   reached it, i.e. its auditable propagation source and depth.
3. *Evidence and ephemeral rows.* Each implicit delete captures the true
   committed prior image and version for history.  When the committing
   transaction itself inserted that child and the cascade removes it
   (`base_present` false), the row never existed in committed state: the
   engine suppresses its staged insert entirely and emits no tombstone or
   change event, reporting it once with `ephemeral: true`.
4. *Restrict after the fixpoint.* A `restrict` edge fires only when a
   *surviving* child (not in the cascade set, not explicitly deleted) still
   points at a deleted identity.  Evaluating it after step 2 means a referrer
   removed in the same batch cannot block the commit, while a genuine surviving
   referrer always rejects it — `restrict` is never executed as `cascade`.
5. *Generic validity.* Finally every surviving non-null reference in the final
   state must resolve to a live parent; this rejects outright orphans (a parent
   that never existed) independently of the strategy.  Every failure produces
   evidence naming the constraint, the child identity and the missing parent,
   and the engine's ordinary all-or-nothing rejection rolls back every staged
   write.

### Cascade deletes are first-class writes

An implicit delete participates in every mechanism that protects an explicit
one, all within the committing transaction rather than a follow-up
transaction:

* **Version precondition (ww).** The planner queries `committed_writes` for any
  change to a cascade-target identity committed after the transaction's
  snapshot and records a `cascade_conflict`.  A reference row inserted or
  re-parented by a concurrent transaction therefore cannot be silently removed;
  the later committer loses with evidence, while two disjoint parent/child
  groups have no intersecting identities and both commit.
* **History and evidence.** A non-ephemeral implicit delete appends one
  tombstone history row and one `committed_writes` row with the distinct
  `cascade_delete` op, preserving the real before/after versions, all stamped
  with the commit's single `commit_version`.
* **Subscriptions.** Watched tables receive one `sub_changes` row per implicit
  delete in the same atomic commit, so a subscriber sees the explicit and
  implicit net changes as one per-commit batch; the replica only applies the
  delivered images and never runs reference logic.
* **Idempotence and recovery.** The full decision, including
  `cascade_deletes`, is stored in `decisions.evidence_json`.  A resend returns
  that durable record without writing, deleting or advancing the position
  again; a crash before the decision leaves only invisible staging, which
  `recover()` resolves exactly as before.

Reads deliberately do not predict cascades: pre-commit `read` keeps the
"start view plus own staged operations" semantics, because a transaction is
entitled to reason about the state it staged, and only the atomic commit
decides the final reference-valid state and its propagation.

## 9. Materialized join-aggregate summaries

### Restricted declaration and physical output

`materialized_defs` stores one frozen JSON declaration per named summary: two
different ordinary tables, one text join column per side, an `inner`/`left`
join, typed group references with output aliases, and `count_star` /
`count` / `sum(int64)` aggregates.  Validation is exhaustive before
persistence, so no half definition exists; the name is reserved in both the
materialized catalog and the ordinary table catalog, and a name can never
silently change definition.

The output is an append-only history table `<name>_rows` with the exact
`(row_id, version, deleted, <output columns>)` shape of an ordinary managed
table, plus a frozen descriptor in `materialized_defs.output_columns_json`.
Reads therefore go through the normal snapshot materialization, predicate
compilation, `predicate_reads` evidence and read-invalidation path; writes,
online `add_column` and reference-constraint endpoints explicitly reject a
materialized name.  Group tuples are encoded as ordered JSON of the original
typed values (`canonical.values_to_json`), which preserves bool/int and
null/"" distinctions without separator concatenation.

### Paged build pinned at one cut

`start_materialized_build` reads `commit_version` into the build row's
`cut_version` inside one write transaction, so the cut and the build's
retention responsibility appear atomically.  The cut scan keysets the left
history at that cut (`row_id > cursor LIMIT budget`); every page is one short
transaction and both cursors reference the same cut, so pages can never mix
source positions.  Join-column helper indexes
(`materialized_indexes`, reference-counted and shared across definitions) make
the right-side lookup per page an indexed key query, and the nested join loop
streams straight into on-disk `materialized_accum` rows — the full join result
is never held in memory, which is what keeps a large one-key fan-out bounded
by the number of groups.

Commits landing while the build runs are incorporated by the catch-up phase,
one source position per step.  Publishing allocates stable tokens in
`materialized_groups`, writes the first output versions and consumes one
explicit source position V by bumping `commit_version`; the summary is
unreadable (`MaterializedNotReady`) until that transaction commits, and its
first visible state is exactly the source state at V.  Cancellation drops the
build row, accumulator, group and output structures and releases the join
indexes and the GC pin; the definition remains as a tombstoned name.

### Key-scoped maintenance and the group-key membership index

The hard part of incremental join maintenance is that group tuples and join
keys are many-to-many: different join keys can feed the same group.
`materialized_group_keys` records, per active group, exactly the non-null join
keys and the left-null marker whose current join rows feed it.  Maintenance
then has an exact, converging scope:

1. seed with the commit's changed non-null keys and (left mode) the null scope;
2. expand to the groups those keys feed via `materialized_group_keys`;
3. expand again to the full persisted key universe of those groups, plus the
   changed keys (which brand-new groups do not yet reference);
4. load both sides at the pre- and post-commit versions over that universe via
   indexed key queries, fold the commit's net change differently into each
   state, and recompute partial aggregates;
5. iterate to a fixpoint so a row migrating into an already-existing group
   pulls in that group's other keys, then rebuild each affected group's
   membership from the actual recomputed rows (membership-filtered, so a key
   whose rows moved out is not retained).

Affected groups are replaced wholesale rather than adjusted by a subtract/add
delta, which keeps the on-disk accumulator self-consistent across group
migrations.  Because every query is key-scoped, the verifiable counters in
`materialized_maintenance_stats` (source rows read, helper records read,
groups written) stay flat as unrelated keys are added — maintenance never
scans a whole source table.

### Identity, versioning, overflow and atomicity

`materialized_groups` keeps one row per group tuple forever (with an `active`
flag), and tokens are allocated from the maximum numeric suffix ever used, so
an inactive group reappears with its original `row_id` and two groups can never
share one within a commit.  Output writes happen through the same history
append, `committed_writes` evidence and (for watched summaries) `sub_changes`
machinery as ordinary tables; an unchanged business value appends no version
and no event, while a disappearing group appends a tombstone.

Overflow is checked only on the *final* recomputed totals; all intermediate
arithmetic is unbounded Python int arithmetic, so a valid final result cannot
trip a false alarm mid-subtract.  A final count or sum outside the int64 range
produces a `materialized_overflow` conflict before the source position is
consumed, and the whole commit — source rows and summary rows — rolls back.
The maintenance plan is computed before the version bump and applied in the
committer's single atomic transaction together with the source history,
cascade tombstones, evidence and change log, so a crash leaves either the old
consistent state or the new one, never a half-maintained summary, and a
re-sent commit replays the durable decision without re-applying anything.


