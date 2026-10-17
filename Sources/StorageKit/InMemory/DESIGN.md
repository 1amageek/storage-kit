# InMemory

## Purpose and Scope

This component owns `InMemoryEngine` and `InMemoryTransaction`, the conforming
reference and standalone backend inside the [StorageKit module](../DESIGN.md).
It has no children. It proves storage semantics without providing durability,
process-restart, or production-adapter evidence.

## Responsibilities and Boundaries

The engine owns the sorted store, commit version, active snapshot registrations,
and retained conflict history. Each transaction owns one immutable snapshot,
one mutable `StateOwner`, its mutation meter, and completion/versionstamp state.
Checkpoint mode composes the shared journal; normal mode keeps its existing
write buffer and snapshot overlay. Directory layout and provenance meaning
remain owned by the Directory component, and SQL state remains above StorageKit.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [StorageKit module](../DESIGN.md) | parent | Reference backend composition | Exposes the engine and transaction through the module | This backend does not establish another adapter's behavior. |
| [Storage](../Storage/DESIGN.md#transaction-checkpoints-sql-compatibility-s12) | depends on | Transaction capabilities, checkpoint journal, raw reader, operation scope, resource limits, authoritative closure | Owns the shared checkpoint contract | Changes to the journal or raw-reader assumptions require these reference cases. |
| [Storage failure contract](../Storage/DESIGN.md#failure-preservation) | depends on | Typed failure conversion and cause preservation | Defines failure propagation | No local replacement taxonomy. |
| [Directory](../Directory/DESIGN.md#checkpoint-provenance-sql-s12) | coordinates with | `KeyValueDirectoryCatalog`, generation provenance, lease validation | Runs catalog byte operations through the selected transaction view | Allocation rollback must not revive discarded Directory values. |

## Architecture

```text
InMemoryEngine
  -> Mutex<StoreState>: sorted bytes / commit version / snapshot registrations
  -> KeyValueDirectoryCatalog

InMemoryTransaction
  -> immutable snapshot + snapshot version
  -> StateOwner -> Mutex<MutableState>: lifecycle / conflicts / normal buffer
  -> optional TransactionJournal
       -> RawCheckpointAccess(snapshot, StateOwner)
            -> bounded snapshot cursor, no transaction back-reference
       -> seal surviving entries -> CommitPayload -> engine store lock
```

## Contracts and Invariants

- A transaction's snapshot and version are captured together under the engine
  lock. The snapshot remains unchanged until its retained owners are released.
- Checkpoint selection occurs before admitted access. Configured capabilities
  expose the same journal that reads, writes, Directory operations, and closure
  consume; the adapter does not construct an outer transaction wrapper.
- Raw point reads enforce their supplied bound. Raw ranges binary-search exact
  physical edges and traverse snapshot entries directly; they do not copy or
  materialize the entire store. Non-snapshot raw range reads conservatively
  register the entire keyspace as a read-conflict region.
- Commit applies the final operation payload and version/history publication
  under the same engine lock. A conflicting payload does not partially apply.
- Directory allocation, metadata, and content share the same byte journal.
  Provenance follows the linked Directory contract rather than prefix reuse.
- Shared state uses the same `Mutex` owners and access paths on Native, WASM,
  and Embedded. No target branch removes synchronization or Sendable.

## Runtime Flows

```text
checkpoint commit election
  -> lifecycle = committing; share the existing completion
  -> journal seal: drain admitted operations and finish cursors
  -> surviving journal operations + latest native read conflicts -> CommitPayload
  -> one engine-store conflict check and atomic application
  -> close journal with outcome; publish surviving provenance only on success
  -> publish transaction/versionstamp completion

checkpoint cancellation election
  -> lifecycle = cancelling
  -> journal close(false): drain operations/cursors and invalidate unpublished values
  -> release snapshot registration -> cancelled completion
```

An operation already admitted by that same journal may finish while the native
lifecycle is committing or cancelling. The active attempt scope identifies this
continuation; external work cannot use that exception. Normal-mode lifecycle
admission is unchanged.

## State, Ownership, and Lifecycle

| State | Owner | Lifetime / release |
|---|---|---|
| Store, version, conflict history, snapshot registry | engine `Mutex<StoreState>` | engine; history pruned as snapshot registrations leave |
| Immutable snapshot and its version | transaction; raw cursors retain the snapshot backing | transaction/cursor lifetime |
| Native lifecycle and read/write conflict regions | transaction `StateOwner` with one `Mutex` | raw readers share this owner without retaining the transaction |
| Normal pending writes | transaction mutable state | normal commit or cancellation |
| Checkpoint journal, markers, payload charges, pending provenance events | shared journal retained by transaction | shared seal/closure contract |
| Published or invalid Directory witnesses | issued Directory/Partition values | independent of the transaction; governed by Directory design |
| Snapshot registration | transaction identifier in the engine | authoritative commit/cancel; deinit releases it idempotently |

## Failure, Concurrency, and Constraints

The component follows the linked Storage failure contract. Store conflicts and
native lifecycle failures use `StorageError`; checkpoint controls and overlay
causes retain their owning types in lifecycle state, shared terminal completion,
and versionstamp waiters, including later callers after failure. Mutation-byte and journal resource admission
remain separate from engine conflict detection. Unsupported historical snapshot
changes and versionstamped mutations are explicit capability failures rather
than fallback behavior. InMemory provides committed versionstamp completion,
which does not imply support for versionstamped mutations.

The journal gate orders suspended reads and barriers. Short native state/store
locks never cross `await`, cursor cleanup, or external byte-owner callbacks.
Raw reader ownership avoids a transaction → journal → reader → transaction
cycle, including when a registered cursor is still unopened.

## Verification and Change Impact

| Contract | Behavioral owner |
|---|---|
| Shared checkpoint semantics | `Tests/StorageKitTests/TransactionCheckpointConformanceTests.swift`, six cases consuming `StorageKitConformance.TransactionCheckpointConformanceCase` |
| Journal admission, retention, typed failure, scope escape, and actual InMemory commit-drain continuation | `Tests/StorageKitTests/TransactionJournalTests.swift`, nine focused cases |
| Unpublished Directory generations and surviving values | `Tests/StorageKitTests/DirectoryCheckpointTests.swift`, four focused cases |
| Existing normal transaction/conflict/versionstamp behavior | `Tests/StorageKitTests/InMemoryTransactionContractTests.swift` and existing StorageKit tests |
| Existing Directory contract | `Tests/StorageKitTests/InMemoryDirectoryConformanceTests.swift` |

The S12 native focused lane executed those 19 checkpoint cases with 19 passes
and zero failures, skips, expected failures, runtime warnings, or internal
errors. This evidence covers that native snapshot only; it does not establish
WASM/Embedded execution, the full StorageKit target, production backend behavior,
or durability. Raw logs and captured inputs are retained by the task harness.
Changes to snapshot ownership, native election, conflict regions, or checkpoint
integration recheck the listed behavioral owners and the directly consuming
Storage/Directory contracts. Shared public contract changes also recheck the
module and production adapter consumers under their own verification lanes.
