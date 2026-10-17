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

### Failure Preservation

Adapters normalize backend-native failures to `StorageError`, and existing
unsupported storage primitives retain `StorageError.unsupportedOperation`.
Caller-operation errors, checkpoint admission/identity failures, provenance
failures, and composed cleanup errors retain the typed causes of their semantic
owner. A cleanup failure preserves both itself and the initiating failure;
conversion never replaces those causes with one generic storage error.
Transaction lifecycle state, shared terminal completions, and versionstamp
waiters retain the same owning error for leaders, followers, and repeated
callers. `storageFailure` projects only a genuine `StorageError`; it does not
fabricate a backend failure for a checkpoint or composed cleanup cause.

`KeyValueRangeResult.Cursor.next()` uses untyped `throws`, and the result stores
`any Error` and may retain an overlay cursor. This carries checkpoint,
provenance, cancellation, and composed cleanup failures with their original
dynamic types; declaring `Failure = StorageError` would incorrectly narrow that
contract. Ordinary stored `StorageError` values remain the same cases and
payloads, and neither array nor overlay failures are converted into another
error or an empty result. Callers that inspect storage-specific fields catch
`StorageError` explicitly and handle other typed failures separately.

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

## Transaction Checkpoints (SQL Compatibility S12)

Status: API and implementation contract; production implementation is pending.
The checkpoint mode is an opt-in storage transaction capability selected by its
lifecycle owner before exposing access, starting reads, or admitting mutations.
Normal transactions preserve their existing paths. SQL names and logical
schema, constraint, mutation, and runtime-generation restoration belong to the
Framework; this component owns byte operations and the transaction view.

```text
concrete native transaction + one common TransactionJournal
  -> operation admission lease / native snapshot reads
  -> ordered byte mutations + explicit write conflicts + read overlay
  -> checkpoints: journal position / option state / Directory provenance position
  -> barrier: close admission -> drain operations -> finish cursors
  -> commit seal -> replay surviving entries -> one native commit
```

### Public Capability and Adapter API

The following signatures are the S12 API contract.
`TransactionCheckpointError` distinguishes invalid limits, configuration after
admission, foreign or invalid tokens, busy/closed admission, reentrant barriers,
and exceeded entry/depth/retained-byte/work limits. Unsupported backend or
unresolved versionstamp behavior uses typed `StorageError.unsupportedOperation`. All methods invoked through
an existential are protocol requirements, including properties and the
result-free operation scope. Concrete generic executors restore typed results
above result-free boundaries, as `StorageTransactionExecutor` already does.

```swift
public struct TransactionCheckpoint: Sendable, Hashable { /* opaque */ }

public struct TransactionCheckpointLimits: Sendable, Hashable {
    public init(
        maximumEntries: Int,
        maximumDepth: Int,
        maximumRetainedBytes: Int,
        maximumWorkUnits: Int,
        maximumReadPageRows: Int
    ) throws
}

public protocol TransactionCheckpointAccess: Sendable {
    func createCheckpoint() async throws -> TransactionCheckpoint
    func rollback(to checkpoint: TransactionCheckpoint) async throws
    func release(_ checkpoint: TransactionCheckpoint) async throws
}

// Requirements added to the existing protocols:
// TransactionReadAccess:
func performOperation(
    _ operation: @escaping @Sendable () async throws -> Void
) async throws
// TransactionAccess:
var checkpoints: (any TransactionCheckpointAccess)? { get }
// Transaction:
func configureCheckpoints(limits: TransactionCheckpointLimits) throws
// TransactionCapabilities:
var transactionCheckpoints: Bool { get }

public enum TransactionJournalBoundary: Sendable, Hashable {
    case unbounded
    case inclusive(ByteString)
    case exclusive(ByteString)
}

public struct TransactionJournalRange: Sendable, Hashable {
    public let lower: TransactionJournalBoundary
    public let upper: TransactionJournalBoundary
    public init(
        lower: TransactionJournalBoundary,
        upper: TransactionJournalBoundary
    )
}

public protocol TransactionJournalBackendAccess: Sendable {
    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString?
    func rangeCursor(
        in range: TransactionJournalRange,
        reverse: Bool,
        snapshot: Bool,
        pageSize: Int,
        maximumByteCount: Int
    ) -> KeyValueCursor
}

public enum TransactionJournalMutation: Sendable {
    case set(key: ByteString, value: ByteString, suppressWriteConflict: Bool)
    case clear(key: ByteString, suppressWriteConflict: Bool)
    case clearRange(begin: ByteString, end: ByteString, suppressWriteConflict: Bool)
    case atomic(key: ByteString, parameter: ByteString, type: MutationType,
                suppressWriteConflict: Bool)
    case writeConflict(begin: ByteString, end: ByteString)
}
```

`checkpoints` belongs only to `TransactionAccess`; a read-only capability
never grants rollback or release authority. `TransactionReadAccess.performOperation`
only groups operations already authorized by captured capabilities, so Directory
reads can join the same barrier without acquiring mutation authority. Its
default calls the body directly; checkpoint adapters forward it to the journal.
The package typed `TransactionOperationExecutor` supplies typed results above
this result-free requirement. Ordinary transactions return nil checkpoints. `configureCheckpoints` defaults to typed unsupported
and no backend advertises support before its full adapter semantics exist.
Configuration after any admitted operation fails before changing state.

`TransactionJournal` is a public concrete adapter integration owner, not an
alternate `Transaction` or a transport wrapper. Its frozen integration surface
is:

| Operation | Signature / result |
|---|---|
| Construct | `init(limits: TransactionCheckpointLimits, backend: StorageBackend, mutationByteMeter: TransactionMutationByteMeter)` |
| Access | conforms to `TransactionCheckpointAccess`; adapters expose that same instance through `checkpoints` |
| Operation scope | `performOperation(_ operation: @escaping @Sendable () async throws -> Void) async throws`; adapters forward the read-access requirement to this owner. `performSynchronousOperation<Result>(_ operation: () throws -> Result) throws -> Result` admits synchronous option/read-conflict work on the same gate |
| Admit writes | `append(_ mutation: TransactionJournalMutation) throws`; `append(copyingByteCount: Int, _ makeMutation: () throws -> TransactionJournalMutation) throws` reserves adapter copies before invoking the materializer outside locks |
| One-shot option | `suppressNextWriteConflict() throws`; append consumes this pending flag into the next mutation entry |
| Point overlay | `getValue(for:snapshot:maximumByteCount:using:) async throws -> ByteString?`, with optional bound and `using: any TransactionJournalBackendAccess` |
| Selector overlay | `getKey(selector:snapshot:using:) async throws -> ByteString?` |
| Range overlay | `rangeCursor(from:to:limit:reverse:snapshot:streamingMode:using:) -> KeyValueCursor` |
| Seal | `sealForCommit() async throws -> [TransactionJournalMutation]` |
| Closure | `close(committed: Bool) async throws`; false invalidates unpublished provenance, true promotes survivors |
| Unknown commit | `closeWithUnknownCommit() async throws`; closes access and invalidates unpublished provenance without interpreting or replacing the backend outcome |

Raw backend access is supplied per read/cursor call, never retained as a stored
journal property. The raw adapter owns native handles, connection/coordinator
lifetime, identifiers, or immutable snapshots. It must not retain its parent
transaction or call its public read methods. SQLite supplies `writes: []`,
PostgreSQL uses the same pinned connection with `flush: false`, FDB calls its
native transaction directly, Durable Object calls the host and records the
observed read version/conflicts, and InMemory reads its immutable snapshot.
This prevents overlay recursion, read-triggered replay, and ownership cycles.
Raw point reads use their existing bounded native path without flushing.
The journal skips a raw read when a surviving set/clear determines the base.
Otherwise it passes the caller bound when the base is the final value, or an
admitted atomic workspace bound from the remaining journal retained budget.
A shrinking atomic is not incorrectly rejected against the final result bound.
The final result is checked against the caller bound after ordered replay.
Raw range cursors honor explicit inclusive/exclusive/unbounded edges, page
size, and the supplied transfer-byte bound before journal materialization.
Native fetch/result allocations remain governed by the backend's existing
request/result limits; journal byte bounds cover transferred owners and
overlay workspace, not unknown memory already owned by the backend. They never resolve a selector against a view missing local mutations.

Adapter-specific native nested transaction creation and physical compaction are
typed unsupported before writes while checkpoint mode is active. Normal nested
transactions and compaction are unchanged. PostgreSQL admits checkpoint mode
only under configured SERIALIZABLE isolation; selection does not change the
engine's default isolation.

### Journal, Overlay, and Checkpoint Invariants

| Contract | Guarantee |
|---|---|
| Tokens | Opaque attempt identity plus a never-reused marker identity. Foreign, released, terminal, and superseded tokens fail with `TransactionCheckpointError`. Rollback retains its target and invalidates every later marker. Release removes its target and later markers without discarding mutations. |
| Journal order | Preserve accepted order of set, clear, clear-range, atomic, and explicit write-conflict entries. One-shot write-conflict suppression is attached to the next write; a checkpoint also saves any pending suppression flag. Do not coalesce unless values, conflicts, options, versionstamps, and ordering are equivalent. |
| Read overlay | Point and bounded point reads, selectors, forward/reverse/limited/paged ranges, exact size metrics, and split points use the surviving journal over the same backend snapshot. Sorted local candidates merge with bounded raw pages before selector resolution and limit application. Clear ranges filter raw rows without collecting the whole backend range. |
| Atomics | Apply the existing `MutationType` algebra in accepted order, including missing values and widths. Result-size admission precedes atomic allocation. Unresolved versionstamped mutations remain opaque at replay; while one survives, overlay reads fail typed unsupported instead of fabricating a commit version. Rollback can restore readability by discarding it. |
| Rollback | Truncate mutations and explicit write conflicts and restore pending one-shot options. Native read conflicts already observed and backend read position remain; there is no read-version rewind. Directory allocation and metadata writes join the same journal. |
| Stable barrier | Close external operation admission, drain all admitted operations, then await authoritative cleanup of every registered cursor before changing view or sealing. Cleanup failure poisons the attempt and leaves the view unchanged; it never reopens a partially closed scope. |
| Commit | Seal access before replay. Replay surviving mutations once, then perform the existing single authoritative backend commit. FDB measures native commit footprint after replay. Replay never causes a second mutation-admission charge. |
| Retry | The Framework starts a new native transaction, journal, attempt-owned `TransactionMutationByteMeter`, provenance scope, and token identities. Journal work limits are attempt-local; Framework request work and deadlines remain cumulative across attempts and are never reset or refunded by this component. `commitUnknownResult` remains unknown, is never automatically replayed, and is not replaced by a cleanup failure. |
| Closure | Cancel/shutdown close journal admission, drain operations/cursors, and await existing authoritative native cleanup. Body and cleanup failures remain separately observable. |

### Admission, Concurrency, and Resource Ownership

The journal entry/marker/option/provenance/cursor state and its standalone
admission/operation/budget activity each have one `Mutex<State>` on every
target. Cursors retain the activity without retaining their journal; the
journal owns cursor registrations, preventing a registration ownership cycle.
Adapter operation admission outlives external materialization and is drained
by terminal barriers. Entry reservations precede external owner detachment,
which runs outside every adapter and journal mutex; the final append linearizes order and consumes one-shot options. No platform branch
removes isolation or weakens Sendable. No mutex is held across I/O, callbacks,
cursor cleanup, or suspension. A serialized asynchronous barrier coordinates
checkpoint, commit seal, and closure transitions.

`performOperation` gives a compound Directory operation one admission lease.
Nested KV calls recognize its TaskLocal attempt identity and active lease and
may finish while the barrier is draining. Attempting checkpoint/barrier control
from inside that same admitted operation fails typed before closing admission,
rather than waiting for its own lease. Independent synchronous writes during
a barrier fail typed busy before mutation admission. A scope closes admission
for inherited tasks when its body ends and drains admitted nested calls before
releasing its lease. A retained inherited TaskLocal lease is not permission to
perform work after the scope closes. This prevents a barrier splitting catalog
allocation from its edge write without deadlocking continuation of an already
admitted Directory operation.

All cursors are registered at creation, including unopened cursors. Their
validation and authoritative cleanup use the existing `KeyValueCursor` state;
a rollback does not manufacture a second iterator lifetime. Barrier closure
invalidates advancement before cleanup and waits for any in-flight advance.
Rows never become observable against a discarded view. Completed cursor
registrations release their retained journal snapshot and raw owners.

Every limit is positive and supplied explicitly by the storage owner/caller;
there are no correctness-changing inferred values. Entry count includes
explicit write conflicts, marker count includes live checkpoints, and work
charges include journal admission, selector traversal, raw rows examined, and
atomic replay. The journal work meter and the existing logical mutation meter
are attempt-owned and cumulative across rollback; upper request meters own
aggregate work/deadline enforcement across retries. Retained-memory charges shrink only when the last retained
journal/cursor owner is discarded or closed.

Known self-contained `ByteString` owners are retained without copying and are
charged by `retainedByteCount`, conservatively once per retained operand when
owner identity cannot be deduplicated. Unknown or enclosing external backing
is detached at this explicit journal/overlay ownership boundary after admitting
the visible byte count, then charged by the resulting exact owner capacity.
The visible count is never reported as the unknown owner's retained size.
Before detachment, admission reserves both any known source backing retained
through the transfer and the destination allocation; unknown source memory
remains owned by the caller/backend and is not claimed as journal-bounded.
Adapter-returned page owners remain backend-budgeted until transferred.
Copies are documented at the transfer site and counted in focused retention
verification. Array/dictionary control storage is bounded by entry/page/depth
limits independently of payload-byte charges.

### Directory Provenance

Directory allocation/projection provenance is owned by the
[Directory component](../Directory/DESIGN.md#checkpoint-provenance-sql-s12).
Every journal-mediated resolution receives a provenance witness tied to the
journal prefix that produced it. Rollback invalidates discarded issuance
witnesses even if allocator bytes later recreate the same address and prefix.
Survivors remain valid after successful commit; cancellation and unknown commit
invalidate unpublished witnesses. Parent operations and lease issuance/bind
validate witnesses before interpreting stored prefixes.

### Verification and Change Impact

One shared `StorageKitConformance.TransactionCheckpointConformanceCase` is
consumed by InMemory, SQLite, PostgreSQL, FDB, and Durable Object test targets.
The falsifiable groups are:

| Case ID | Required behavior |
|---|---|
| CP-01 | nested/non-LIFO rollback and release; target retention; foreign/released/stale/terminal token failure |
| CP-02 | point/bounded/selector/reverse/limited/paged reads and metrics agree before and after rollback, including repeated clear-range and atomics |
| CP-03 | root bootstrap, create/move/remove, nested Partitions, allocation rollback, mixed metadata/data, and ABA provenance rejection verified from a new transaction after commit |
| CP-04 | native read conflicts survive; discarded explicit write conflicts do not; one-shot options and unknown commit preserve their exact semantics |
| CP-05 | operation drain, unopened/active cursor cleanup, cancellation/commit/barrier races, inherited scope escape, and cleanup failure |
| CP-06 | cumulative work versus released retained bytes, bounds before allocation, unknown-backing detachment, retry-fresh identities, and unresolved versionstamp fail-closed |

Native, WASM, and Embedded compile/link/runtime are separate proofs. FDB
checkpoint evidence must execute its native Directory Layer; full FDB regression
remains explicitly user-deferred to SQL-S18.02.01 and is not passing evidence.
Durable Object proof includes real host commit/rollback and restart; the Swift
InMemory client and JavaScript InMemorySQLiteStorage suites alone do not prove
that path. Existing cursor and transaction lifecycle proofs remain valid only
where their premises remain unchanged. Changing this contract rechecks every
adapter, Directory provenance, Framework transaction/logical-state restoration,
and SQL block ownership; upper designs link here instead of duplicating it.

The CP-06 budget case also requires an exact owning work-limit cause at
commit, repeated commit observation, rejection of access after terminal failure,
and authoritative journal closure. Shared terminal-completion regression tests
cover pending followers and composed cleanup payloads without backend mapping.
