# SQLiteStorage

## Purpose and Scope

`SQLiteStorage` is the native SQLite adapter of storage-kit. It realizes the
StorageKit `StorageEngine`, transaction, cursor, compaction, and
`DirectoryAccess` contracts over one SQLite connection, and it realizes
Directories and Partitions as a transactional catalog in the same database as
required by SPEC §7.3.

| Field | Value |
|---|---|
| Level | module |
| Parent | [storage-kit package](../../DESIGN.md) |
| Children | none; `SQLiteStorageEngine`, `SQLiteTransactionCoordinator`, `SQLiteStorageTransaction`, and the range cursor types form one design unit |
| Sources | `Sources/SQLiteStorage/` |
| Tests | `Tests/SQLiteStorageTests/` |

## Responsibilities and Boundaries

| Owns | Does not own |
|---|---|
| `SQLiteConnectionHandle`: one connection per engine, `kv_store` schema bootstrap, `PRAGMA journal_mode=WAL`, `auto_vacuum=INCREMENTAL`, `busy_timeout`, native error conversion | The SQLite library and its file locking model |
| `SQLiteTransactionCoordinator` (actor): FIFO connection lease, `BEGIN IMMEDIATE`, the savepoint stack for nested transactions, terminal cleanup | Directory contract semantics D-1…D-12 and lease semantics L-1…L-8 ([Directory component](../StorageKit/Directory/DESIGN.md)) |
| `SQLiteStorageTransaction`: buffered synchronous mutations, lazy coordinator entry on the first asynchronous operation, commit-at-most-once, cancel, `StorageCompactionTransaction` | The catalog algorithm and root bootstrap (`KeyValueDirectoryCatalog`, StorageKit) |
| `SQLiteRangeResult`, `SQLiteRangeIteratorState`, `SQLiteRangeCursorLifetime`: statement-backed lazy cursors with explicit terminal finish | `PartitionLeaseRegistry` and `PartitionLease` (StorageKit) |
| Catalog placement: the engine instantiates `KeyValueDirectoryCatalog(transactionDomain:backend: .sqlite)` bound to its domain | Framework binding of `#Directory` declarations |

Authority: the catalog rows inside `kv_store` are the sole existence authority
for Directories and Partitions of this backend (SPEC §12.3, package invariant
P-4). The engine adds no SQLite-specific metadata table.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [storage-kit package](../../DESIGN.md) | parent | package invariants P-1…P-7 | Module graph and package-wide invariants. | Public contract changes propagate to database-framework. |
| [StorageKit module](../StorageKit/DESIGN.md) | depends on | `StorageEngine`, `TransactionReadAccess`, `TransactionAccess`, `Transaction`, `StorageCompactionTransaction`, `StorageEngineLifecycle`, `ActiveTransactionContext`, `StorageError` | Supplies the contracts this adapter realizes and the nested-transaction context. | `createTransaction()` consults `ActiveTransactionContext`; a child is created only for a parent of the same `StorageTransactionDomain`. |
| [Directory component](../StorageKit/Directory/DESIGN.md) | depends on | `DirectoryAccess`, `KeyValueDirectoryCatalog`, `StorageTransactionDomain`, `DirectoryLimits` | The catalog realization used unchanged by this module. | Catalog keys share `kv_store` with data (SQ-5); a catalog layout change is a data layout change for every SQLite database. |
| [StorageKitConformance](../StorageKitConformance/DESIGN.md) | used by | `DirectoryConformanceCase` | Shared fixture executed by `SQLiteDirectoryConformanceTests`. | The foreign-root step applies; it plants foreign keys through the engine's own transactions. |

## Architecture

```text
SQLiteStorageEngine
  ├─ connection: SQLiteConnectionHandle        serialized native calls, PRAGMAs, kv_store
  ├─ lifetime: SQLiteStorageLifetime            engine-owned resource lifetime
  ├─ coordinator: SQLiteTransactionCoordinator  actor: FIFO lease, BEGIN IMMEDIATE, savepoints
  ├─ transactionDomain: StorageTransactionDomain identity + PartitionLeaseRegistry
  ├─ directoryAccess: KeyValueDirectoryCatalog  (StorageKit) reads/writes kv_store via the transaction
  └─ creates SQLiteStorageTransaction
           ├─ root: owns BEGIN IMMEDIATE … COMMIT/ROLLBACK through the coordinator
           ├─ child: SAVEPOINT … RELEASE/ROLLBACK TO, strict LIFO
           └─ SQLiteRangeResult -> SQLiteRangeIteratorState (prepared statement, finish())
```

Dependency direction: `SQLiteStorage -> StorageKit`, `SQLiteStorage -> DatabaseTypes`,
`SQLiteStorage -> SQLite3` (system library). Nothing inside this package depends on
`SQLiteStorage`.

## Contracts and Invariants

### Configuration

| Field | Contract |
|---|---|
| `path: String` | Database file path; `Configuration.inMemory` uses `":memory:"`, `Configuration.file(_:busyTimeoutMilliseconds:)` a file. |
| `busyTimeoutMilliseconds: Int32` | `PRAGMA busy_timeout` budget (default 100 ms). Bounds how long one synchronous SQLite call inside the coordinator may block on another connection; beyond it the engine fails with `StorageError.transactionBusy` (`retryDisposition == .safe`). `0` fails immediately on contention. |

### Directory realization

| Element | Realization |
|---|---|
| Catalog | `KeyValueDirectoryCatalog(transactionDomain:backend: .sqlite)` |
| Catalog keys | allocator `[0xFE, 0x61]`, node keys `[0xFE, 0x6E] + Tuple(parentPrefix, kind, name)`; stored in `kv_store` beside data |
| Directory root prefix | catalog-allocated `Tuple(Int64).pack()`; root Directory uses number `0` |
| Root bootstrap | the catalog's allocator witness and unbounded emptiness probe (Directory component); no SQLite-specific state |

### Invariants

| ID | Invariant |
|---|---|
| SQ-1 | One native connection per engine; every native call runs inside the coordinator actor or the connection handle's serialized entry, and no lock is held across a suspension point. |
| SQ-2 | Transactions acquire the connection lease in FIFO order; a root transaction owns `BEGIN IMMEDIATE` from its first asynchronous operation through its terminal commit or rollback. |
| SQ-3 | `createTransaction()` under an active `ActiveTransactionContext` transaction of the same domain returns a savepoint-backed child; children complete in strict LIFO order via `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT`. |
| SQ-4 | Catalog mutations and data mutations of one transaction commit in the same native transaction; a Directory create, move, or removal is never visible without the data written beside it. |
| SQ-5 | Catalog keys begin with `0xFE`; Directory and Partition root prefixes are Tuple-encoded integers and never begin with `0xFE`, so catalog rows and data rows in `kv_store` are disjoint. |
| SQ-6 | `SQLITE_BUSY` and `SQLITE_LOCKED` past `busy_timeout` map to `transactionBusy`; the adapter performs no internal retry. |
| SQ-7 | `requestShutdown()` closes lease issuance (`transactionDomain.leases.requestShutdown()`) and transaction admission before backend cleanup; admitted transactions keep the resources needed for their own terminal cleanup; `waitUntilShutdown()` awaits the coordinator and connection release. |
| SQ-8 | A range cursor holds its prepared statement until `finish()`; an abandoned iterator is finalized synchronously through the connection handle by `SQLiteRangeCursorLifetime`, never by an unstructured task. |
| SQ-9 | `StorageCompactionTransaction` runs `PRAGMA incremental_vacuum` inside the coordinator window and reports `SQLiteIncrementalCompactionMetrics`; capability presence is a type-level fact, not a runtime probe. |

## Runtime Flows

Transaction lifecycle:

```text
createTransaction()   admission check -> identifier -> (context child? SQ-3)
  -> setValue/clear   buffered synchronously, no SQLite call
  -> first getValue / rangeCursor / compaction / commit
       -> coordinator.beginRoot   FIFO lease -> BEGIN IMMEDIATE (SQ-2)
       -> flush buffered mutations, execute statement
  -> commit           COMMIT (root) or RELEASE SAVEPOINT (child), lease to next waiter
  -> cancel           ROLLBACK / ROLLBACK TO SAVEPOINT, same lease handoff
```

Directory operation (any of operations 1–5):

```text
KeyValueDirectoryCatalog -> transaction.getValue / setValue / clear on catalog keys
  -> same coordinator window as the transaction's data operations (SQ-4)
```

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| SQLite connection | `SQLiteConnectionHandle` via `SQLiteStorageLifetime` | from `init` until shutdown cleanup completes |
| Connection lease, transaction/savepoint stack, waiter queue | `SQLiteTransactionCoordinator` | engine lifetime; entries live from lease acquisition to terminal cleanup |
| Buffered mutations, lifecycle phase | `SQLiteStorageTransaction` | from creation to terminal commit or cancel |
| Prepared range statement | `SQLiteRangeIteratorState` guarded by `SQLiteRangeCursorLifetime` | until `finish()` or iterator abandonment |
| Lease registrations and intents | `PartitionLeaseRegistry` (StorageKit) | released by lease end of lifetime / transaction completion |

## Failure, Concurrency, and Constraints

- All native failures cross the boundary as `StorageError` with `backend == .sqlite`.
- Two engines or processes on one file serialize through SQLite's own locks; only
  `busy_timeout` and `transactionBusy` are observable from StorageKit.
- Key, value, mutation, and Directory bounds are the StorageKit bounds; SQLite adds
  none of its own. Range cursors are single-consumer.
- Nested transactions require the parent to be alive; a child that outlives its
  parent's terminal state fails typed.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| D-1…D-12, operations 1–5, root bootstrap, L-1…L-3, L-7, L-8 | `Tests/SQLiteStorageTests/SQLiteDirectoryConformanceTests.swift` (shared `DirectoryConformanceCase` steps, including `foreignRootRejection`) |
| SQ-2, SQ-3 | `SQLiteNestedTransactionTests`, `SQLiteStorageEngineTests` |
| SQ-6 | `SQLiteBusyTimeoutTests` |
| SQ-1, SQ-7 | `SQLiteConnectionOwnershipTests`, `SQLiteStorageEngineTests` |
| SQ-8, cursors, selectors, bounded reads | `SQLiteLazyRangeCursorTests`, `SQLiteKeySelectorTests`, `SQLiteBoundedPointReadTests` |
| SQ-9 | `SQLiteStorageCompactionTests` |

Changing the PRAGMA set, the `kv_store` schema, or the catalog placement is a
persisted-layout change: update this design, the Directory component's adapter
notes, and the package design's change-impact table before implementation.
