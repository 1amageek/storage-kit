# Storage Component

## Purpose and Scope

The Storage component owns the platform-neutral engine, transaction, key/value,
range, mutation, cancellation, and lifecycle contracts implemented by the
StorageKit module. It is a child of [`../DESIGN.md`](../DESIGN.md) and the
package design at [`../../../DESIGN.md`](../../../DESIGN.md).

## Responsibilities and Boundaries

The component owns backend-neutral storage behavior and the state machine for a
transaction range cursor. It owns cursor cleanup, scope validation, cancellation,
and lifetime coordination. It does not own tuple encoding, query or schema
meaning, authorization policy, backend deployment, or caller resource policy.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [`StorageKit module`](../DESIGN.md) | parent | Module composition and child boundary | Composes this component with Tuple | Keep cursor details here rather than in the module index. |
| [`StorageKit package`](../../../DESIGN.md) | ancestor | Package storage ownership | Defines the package boundary | Backend adapters must preserve this component's behavior. |
| Database framework Read consumer (`Sources/DatabaseEngine/Read/DESIGN.md`) | dependent consumer | Scoped range access uses `KeyValueCursor.validatingScope` | The framework consumer supplies the read-scope validation callback and lifetime owner | Recheck the consumer pin and cursor integration when this contract changes. |

## Architecture

```text
Transaction range request
    -> one KeyValueCursor state
        ├─ one backend cursor
        ├─ one advance/finish boundary
        ├─ one lifetime owner
        └─ zero-copy key/value results
```

The public `KeyValueCursor` erases the backend result while its private actor
state owns ordered suspension and cleanup. Scope validation is passed into that
state; no second cursor state is introduced at the caller boundary.

## Contracts and Invariants

- `KeyValueCursor.validatingScope` composes the caller's synchronous validation
  with the same cursor state. It does not copy returned key/value bytes.
- Every scoped `next()` follows exactly this order:

  ```text
  pre-validation
      -> publish the advancing boundary
      -> cooperative executor handoff
      -> cancellation recheck
      -> one backend advance
      -> cancellation check
      -> post-validation
      -> publish ready/exhaustion or resolve finish boundary
  ```

  The handoff is common to every target and occurs only after the advancing
  boundary is visible. It bounds immediate actor-hop stack growth without
  changing advance exclusivity; cancellation observed after the handoff enters
  the existing terminal cleanup path before backend advance.

- Post-validation is completed before a row becomes observable, before the
  ready state is published, and before a concurrent finish boundary resolves.
- A cursor issued by a bound Partition access belongs to the binding scope that
  issued it. Closing that scope completes the cursor's backend cleanup, so an
  escaped cursor has no outstanding capability — cleanup included — once the
  binding closes, and no cleanup is left to run against a transaction its owner
  has already closed.
- A commit whose outcome is unknown is reported as that unknown outcome. The
  transaction owner does not cancel, clean up, or wrap it into a different
  failure, because the transaction may already have been applied. This holds in
  every transaction owner, including the backends that override transaction
  execution.
- `finish()` waits for an in-flight advance boundary. Cursor lifetime owners
  are released only after backend cleanup and post-validation have completed.
- A validation, cancellation, iteration, or cleanup failure remains typed.
  When iteration and cleanup both fail, the existing cleanup error preserves
  both failures; cleanup is attempted exactly once.
- Returned key/value buffers retain their backend owners after cursor cleanup.
  The cursor does not materialize or retain a second copy.

## Runtime Flows

```text
next()
  -> scope admission
  -> backend cursor.next()
  -> task cancellation
  -> scope validation
      ├─ failure -> backend finish -> release owner -> typed failure
      └─ success
          ├─ row -> publish row only after validation
          └─ exhaustion -> backend finish -> release owner
              -> resolve waiting finish
```

Concurrent `finish()` observes the advancing state, requests the same advance
boundary to finish, and waits for the advance to perform post-validation and
backend cleanup. It never starts an independent backend cleanup path.

## State, Ownership, and Lifecycle

The actor state owns the backend cursor, its advance/finish boundary, and
terminal cleanup. `KeyValueCursorLifetime` owns attached scope or transaction
owners until terminal cleanup completes. Validation callbacks are retained by
the `KeyValueCursor` for its lifetime and invoked synchronously at the defined
boundaries. Backend-owned byte buffers are returned unchanged and remain owned
by their result values.

## Failure, Concurrency, and Constraints

Actor isolation orders the cooperative executor handoff, suspendable backend
I/O, and finish coordination; no mutex is held across `await`. A concurrent
advance or finish during the handoff observes the existing advancing boundary.
Cancellation and scope revocation are terminal for iteration and await backend
cleanup before escaping. Cleanup failures retain the `StorageRangeCleanupError`
and `StorageRangeTerminalCleanupError` contracts. The state has one backend
advance per returned row and no ambient authority or module-level registry.

## Verification and Change Impact

`StorageKitTests/TransactionRangeCleanupTests` owns the behavioral proof for
pre/post ordering, one backend advance per row, cancellation and revocation,
admission before backend open, finish coordination, typed cleanup composition,
exactly-once cleanup, and zero-copy byte ownership. It also proves cancellation
and finish behavior across the cooperative handoff. The deterministic finish
test observes the actor's advancing state before resuming backend advance and
proves that backend finish and owner release occur after post-validation. A
WASM immediate-completion range probe on the default runtime stack proves that
repeated advances do not accumulate actor-hop stack frames.

Changes to this component require its focused tests and a dependent
database-framework cursor check. Backend adapter tests are required when a
storage protocol or backend implementation changes.

## Planned Transaction Checkpoints (SQL Compatibility S12)

Status: design contract only. `TransactionAccess` and
`TransactionCapabilities` currently expose no portable checkpoint capability.
Existing SQLite nested savepoints and PostgreSQL nested write buffers are
backend-specific behavior, not proof of this contract. Implementation belongs
to S12 of the [workspace task](../../../../PROGRESS.md).

The storage abstraction is a checkpointable, deferred-write transaction, not a
SQL execution mode. The lifecycle owner selects it before exposing access or
accepting mutations; ordinary transaction paths retain their current behavior.
Checkpoint authority is an attenuated transaction capability: it neither commits
nor cancels the owning transaction. The Framework maps SQL names to opaque
checkpoint tokens and restores its own logical schema, constraint, mutation,
and runtime-generation state; StorageKit owns only byte operations and storage
view state.

```text
concrete transaction
  -> one ordered mutation journal + read overlay
       ├─ data / index / catalog operations
       ├─ Directory metadata and allocation operations
       └─ opaque checkpoint offsets
  -> commit: replay surviving operations -> one authoritative backend commit
```

| Contract | Guarantee |
|---|---|
| Admission | Checkpoint support must be declared only after full adapter semantics exist. Unsupported admission fails typed before any write; no native-only fallback. |
| Tokens | Tokens belong to one transaction attempt and generation. Foreign, released, terminal, and superseded tokens fail typed. Rollback retains the target checkpoint and invalidates later ones; release discards that checkpoint and later markers without discarding their mutations. |
| Journal | Preserve accepted order for set, clear, clear-range, and atomic operations, including operation-specific options and write-conflict suppression. Do not coalesce operations unless byte values, conflict semantics, versionstamps, and ordering are equivalent. |
| Read overlay | Point reads, bounded reads, selectors, forward/reverse and limited ranges, pagination, size metrics, and split-point reads observe precisely the surviving ordered operations over the same backend snapshot. Overlay rows participate before selectors and limits are resolved. |
| Atomic operations | Overlay application uses the same storage atomic algebra as commit, including missing values and byte widths. Versionstamped mutations retain their unresolved commit semantics and backend-specific limitations; rollback cannot manufacture a resolved versionstamp. |
| Rollback | Discard mutations after the marker and restore the view before it. No discarded data, catalog, Directory node, allocator update, or explicit write conflict is replayed at commit. Read conflicts already observed remain conservatively retained, and the backend read version never rewinds. |
| Stable boundary | Checkpoint operations drain admitted operations and finish active range cursors before changing the journal view. Closed cursors cannot continue against a new view. Issued Directory/lease values must be revalidated through the existing existence/generation checks after rollback. |
| Commit and retry | Replay only after checkpoint access is sealed. Each backend sees one commit, with no earlier journal flush caused by a read. A retry starts a fresh attempt, journal, and tokens; unknown commit outcome remains unknown and must not be replayed automatically. |
| Cleanup | Cancel/shutdown await authoritative backend closure and release retained journal/overlay owners exactly once. Operation and cleanup failures remain separately observable. |

The journal retains immutable `ByteString` owners and bounded views, never
borrowed pointers. Journal entry count, checkpoint depth, overlay work, and
retained backing bytes are bounded before allocation. Mutation work admission
remains cumulative across rollback; retained-memory charges are released with
discarded owners. Limits are configured and validated by the storage owner and
caller budget, not inferred from SQL syntax. Overlay output materializes bytes
only when atomic evaluation or an external ownership boundary requires it;
large-copy claims require measurements.

Native, WASM, and Embedded use the same stored state, `Sendable` capability,
mutex/actor boundaries, and mutation entry points. Short non-suspending journal
admission uses a mutex; suspended backend reads and checkpoint/commit barriers
use the transaction's ordered state machine without holding a mutex over I/O.
Every adapter must prevent concurrent native and journal paths from bypassing
that state machine.

### Checkpoint Verification and Change Impact

S12 introduces one shared conformance fixture consumed by all adapter targets
and the InMemory reference. It must prove nested rollback/release, point and
selector reads, reverse/limited/paged ranges, repeated clear-range and atomics,
Directory create/move/remove with nested Partitions, root bootstrap, mixed
metadata/data rollback, conflicts, retries, stale tokens, admission limits,
cancel/commit races, owner retention, and cleanup. Failure cases verify committed
state from a new transaction, not only journal shape. Existing cursor and
transaction lifecycle tests remain valid only where their assumptions persist.
FoundationDB evidence must execute the real native Directory Layer; Durable
Object evidence must include real host commit/rollback and restart. Native,
WASM, and Embedded compile/link and executable target behavior are separate
proofs. Checkpoint implementation is incomplete until all production adapters
and the Framework logical-state integration pass their owned behavior tests.
