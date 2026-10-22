# SQLiteStorage

## Purpose and Scope

`SQLiteStorage` is the native SQLite adapter of storage-kit. It realizes the
StorageKit `StorageEngine`, transaction, cursor, compaction, and
`DirectoryAccess` contracts over an explicitly bounded set of SQLite connections, and it realizes
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
| `SQLiteConnectionHandle`: one synchronized handle per configured connection, `kv_store` schema bootstrap, `PRAGMA journal_mode=WAL`, `auto_vacuum=INCREMENTAL`, `busy_timeout`, native error conversion | The SQLite library and its file locking model |
| `SQLiteTransactionCoordinator` (actor): FIFO connection lease, native transaction mode, the savepoint stack for nested transactions, terminal cleanup | Directory contract semantics D-1…D-12 and lease semantics L-1…L-8 ([Directory component](../StorageKit/Directory/DESIGN.md)) |
| `SQLiteStorageTransaction`: buffered synchronous mutations, lazy coordinator entry on the first asynchronous operation, commit-at-most-once, cancel, `StorageCompactionTransaction` | The catalog algorithm and root bootstrap (`KeyValueDirectoryCatalog`, StorageKit) |
| `SQLiteRangeResult`, `SQLiteRangeIteratorState`, `SQLiteRangeCursorLifetime`: statement-backed lazy cursors with explicit terminal finish | `PartitionLease` (StorageKit) |
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
  ├─ connections: immutable connection/coordinator pairs, configured count
  ├─ lifetime: SQLiteStorageLifetime            engine-owned resource lifetime
  │    └─ SQLiteTransactionCoordinator actor: FIFO lease and savepoints per handle
  ├─ transactionDomain: StorageTransactionDomain identity + lease issuance gate
  ├─ directoryAccess: KeyValueDirectoryCatalog  (StorageKit) reads/writes kv_store via the transaction
  └─ creates SQLiteStorageTransaction
           ├─ root: owns one selected connection through COMMIT/ROLLBACK
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

### Explicit parallel transactions

`Configuration.connectionCount` is a positive caller-owned resource budget,
with default 1. It fixes both the number of opened handles and the maximum
number of simultaneously active native root transactions. The caller selects
the count from measured latency/throughput/memory; no hardware guess changes
it. Additional roots queue FIFO at their selected connection. Allocation opens
handles incrementally and closes every already opened handle on failure.

A count above 1 requires an ordinary file path, a file-backed WAL database and SQLite 3.51.3 or later.
URI filenames are rejected before opening: resolving them to plain filenames
must not discard access options such as `mode=ro` for subsequent handles.
The native connection validates the running library before opening, then
validates the actual journal mode and retained database filename; private
in-memory/temporary databases cannot silently become separate stores. Further
handles open the same resolved filename. The version floor avoids the documented
[WAL-reset race](https://sqlite.org/wal.html#walresetbug); older backports are not
part of this mode's supported baseline. Default one-connection behavior is
unchanged.

Each root may read and write on its connection. A deferred first read establishes
SQLite's committed snapshot; a later mutation keeps read-your-writes or fails
with retryable `transactionBusy` if that snapshot cannot upgrade. Extended busy
codes retain that classification. The adapter does not retry or move a running
root to another connection. SQLite serializes writers. Nested transactions and
checkpoints stay on the parent's handle and retain strict-LIFO savepoints.
See [SQLite isolation](https://sqlite.org/isolation.html) and
[transaction control](https://sqlite.org/lang_transaction.html).

Shutdown first seals the shared lifetime and every coordinator, then closes all
handles, including queued-root and active-cursor cleanup. One awaited engine
shutdown remains the authoritative completion for all connections. A host must
not replace an open database file or separate it from its WAL/SHM files.

| State | Storage owner / isolation | Read and mutation | Release |
|---|---|---|---|
| Identifier and next connection index | Engine `Mutex<EngineState>` | Synchronous allocation/selection only | Engine |
| Handle and prepared cursors | `SQLiteConnectionHandle` `Mutex<SQLiteConnection?>` | Existing serialized C boundary | All coordinators sealed, then close |
| Root, waiters and savepoints | One coordinator actor per handle | Existing asynchronous FIFO entries | Await shutdown/transaction terminal |
| Admission and lifetime | Existing engine lifecycle and shared lifetime | Same gate for every connection | One authoritative awaited shutdown |

These owners have no target-specific synchronization branches. SQLiteStorage
is a native adapter; portable framework execution uses its existing storage
adapters and does not infer SQLite filesystem capability from Embedded mode.

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
| SQ-1 | A positive configured number of native connections per engine; every native call runs inside the coordinator actor or the connection handle's serialized entry, and no lock is held across a suspension point. |
| SQ-2 | Root admission selects connections in round-robin order; each connection retains its FIFO lease. A root owns its selected connection from first asynchronous operation through terminal cleanup. One-connection mode uses `BEGIN IMMEDIATE`; explicit parallel mode uses `BEGIN DEFERRED`. |
| SQ-3 | `createTransaction()` under an active `ActiveTransactionContext` transaction of the same domain returns a savepoint-backed child; children complete in strict LIFO order via `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT`. |
| SQ-4 | Catalog mutations and data mutations of one transaction commit in the same native transaction; a Directory create, move, or removal is never visible without the data written beside it. |
| SQ-5 | Catalog keys begin with `0xFE`; Directory and Partition root prefixes are Tuple-encoded integers and never begin with `0xFE`, so catalog rows and data rows in `kv_store` are disjoint. |
| SQ-6 | `SQLITE_BUSY` and `SQLITE_LOCKED` past `busy_timeout` map to `transactionBusy`; the adapter performs no internal retry. |
| SQ-7 | `requestShutdown()` closes lease issuance (`transactionDomain.requestShutdown()`) and transaction admission before backend cleanup; admitted transactions keep the resources needed for their own terminal cleanup; `waitUntilShutdown()` awaits the coordinator and connection release. |
| SQ-8 | A range cursor holds its prepared statement until `finish()`; an abandoned iterator is finalized synchronously through the connection handle by `SQLiteRangeCursorLifetime`, never by an unstructured task. |
| SQ-9 | `StorageCompactionTransaction` runs `PRAGMA incremental_vacuum` inside the coordinator window and reports `SQLiteIncrementalCompactionMetrics`; capability presence is a type-level fact, not a runtime probe. |

## Runtime Flows

Transaction lifecycle:

```text
createTransaction()   admission check -> identifier -> (context child? SQ-3)
  -> setValue/clear   buffered synchronously, no SQLite call
  -> first getValue / rangeCursor / compaction / commit
       -> coordinator.beginRoot   FIFO selected lease -> configured BEGIN mode (SQ-2)
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
| Lease registration | `LeaseRegistration` (StorageKit) | released at lease end of lifetime |

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

## Checkpoint Integration (S12)

The [Storage checkpoint contract](../StorageKit/Storage/DESIGN.md#transaction-checkpoints-sql-compatibility-s12)
owns journal, token, barrier, provenance, and budget semantics. The concrete
transaction selects the common journal before its first operation. Its raw
reader retains the coordinator, connection lifetime, and native identifiers;
it never retains the parent transaction or flushes writes. Native range reads
check each row's transferred key/value bytes before copying SQLite columns.

Normal transactions retain their buffered flush and native-savepoint paths.
Checkpoint transactions reject native child creation and physical compaction
before either can change native state. Commit seals and drains the journal,
replays surviving operations once into the existing root commit path, and
awaits native cleanup before closing the journal. Directory catalog operations
use the same overlay as data. File persistence, busy handling, and FIFO
connection ownership retain SQ-1, SQ-2, SQ-4, and SQ-6 through SQ-8.

Verification adds shared checkpoint conformance, file-backed rollback and
commit/reopen, native-child and compaction rejection, and normal nested
transaction regression. Source changes do not establish this evidence alone.

Checkpoint writes select the immutable journal under the transaction-state
mutex, then acquire journal operation admission before byte-owner materialization
outside every mutex. Seal rejects writes not yet admitted and drains admitted
writes before replay.
Terminal checkpoint and cleanup causes follow the linked failure contract,
including shared completion and repeated commit observation.

Parallel-mode behavioral evidence is owned by
`SQLiteConcurrentTransactionTests`: simultaneous read admission, old/new snapshot
visibility, stale upgrade refusal, read-your-writes, nested savepoint isolation,
queued and active shutdown, and invalid/private-memory/URI configuration. The full
SQLiteStorageTests suite preserves the default single-connection contracts.
The task's file-backed framework workload measures counts 1/2/4/8/16 with
validated SQL results and process RSS; performance acceptance remains pending
until matched measurements complete.
