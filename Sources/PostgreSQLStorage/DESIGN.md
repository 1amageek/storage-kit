# PostgreSQLStorage

## Purpose and Scope

`PostgreSQLStorage` is the PostgreSQL adapter of storage-kit. It realizes the
StorageKit `StorageEngine`, transaction, cursor, and `DirectoryAccess`
contracts over `postgres-nio`, and it realizes Directories and Partitions as
transactional catalog rows in the same key-value relation as required by
SPEC §7.3.

| Field | Value |
|---|---|
| Level | module |
| Parent | [storage-kit package](../../DESIGN.md) |
| Children | none; `PostgreSQLStorageEngine`, `PostgreSQLStorageTransaction`, the configuration types, and the result byte owners form one design unit |
| Sources | `Sources/PostgreSQLStorage/` |
| Tests | `Tests/PostgreSQLStorageTests/` |
| Decision record | [Docs/POSTGRES_NIO_DEPENDENCY_DECISION.md](../../Docs/POSTGRES_NIO_DEPENDENCY_DECISION.md) |

## Responsibilities and Boundaries

| Owns | Does not own |
|---|---|
| `PostgreSQLConfiguration`: client configuration, isolation level (default `.serializable`), validated bare `tableName`, schema policy `createIfNeeded` / `assumeExists`; `PostgreSQLConfiguration+Production` and `PostgreSQLConnectionBudget` for serverless connection budgeting | The PostgreSQL wire protocol and pool (`postgres-nio`) |
| `PostgreSQLStorageEngine`: `PostgresClient` run task, schema bootstrap, transaction creation in eager, lazy, and nested modes, native error mapping, shutdown | Directory contract semantics D-1…D-12 and lease semantics L-1…L-8 ([Directory component](../StorageKit/Directory/DESIGN.md)) |
| `PostgreSQLStorageTransaction`: buffered writes, read-your-writes replay for point reads, buffer flush before range reads, advisory-lock atomics, `BEGIN ISOLATION LEVEL …` through `COMMIT`/`ROLLBACK`, exactly-once connection release | The catalog algorithm and root bootstrap (`KeyValueDirectoryCatalog`, StorageKit) |
| `PostgreSQLBindingBytes`: one copy of each bound key or value into independently owned `ByteBuffer` storage | `PartitionLease` (StorageKit) |
| `PostgreSQLResultBytesOwner` / `PostgreSQLResultBytesFactory` / `PostgreSQLResultBytesLifecycleObserver`: result byte ownership and lifecycle evidence | Framework binding of `#Directory` declarations |
| Catalog placement and the isolation-level operation admission rule (PG-3) | Retry policy (owned by the caller's transaction runner) |

Authority: catalog rows in the configured relation are the sole existence
authority for Directories and Partitions of this backend (SPEC §12.3, package
invariant P-4).

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [storage-kit package](../../DESIGN.md) | parent | package invariants P-1…P-7 | Module graph and package-wide invariants. | Public contract changes propagate to database-framework. |
| [StorageKit module](../StorageKit/DESIGN.md) | depends on | `StorageEngine`, `TransactionReadAccess`, `TransactionAccess`, `Transaction`, `StorageEngineLifecycle`, `ActiveTransactionContext`, `StorageError` | Supplies the contracts this adapter realizes and the nested-transaction context. | A nested transaction merges its buffer into the parent on `commit()`; only the parent issues `COMMIT`. |
| [Directory component](../StorageKit/Directory/DESIGN.md) | depends on | `DirectoryAccess`, `KeyValueDirectoryCatalog`, `KeyValueDirectoryCatalog.OperationAdmission`, `StorageTransactionDomain` | The catalog realization, parameterized by the isolation-level admission rule. | The catalog decides existence by read-then-write across rows the other transaction writes and only SERIALIZABLE detects that conflict; a Partition read binding additionally needs a snapshot that outlives its generation walk (PG-3). |
| [StorageKitConformance](../StorageKitConformance/DESIGN.md) | used by | `DirectoryConformanceCase` | Shared fixture executed by `PostgreSQLDirectoryConformanceTests` against a real server. | Each test uses a unique relation name so parallel suites never share catalog state. |
| `postgres-nio` | depends on | `PostgresClient`, `PostgresQuery`, `ByteBuffer` bindings | Connection pool and query execution. | `PostgresClient` exposes only scoped `withConnection`; lazily acquired connections are parked on a continuation (PG-5). Parameters may outlive the source borrow (PG-6). |

## Architecture

```text
PostgreSQLStorageEngine
  ├─ client: PostgresClient + runTask                  pool run loop
  ├─ configuration: PostgreSQLConfiguration            isolation, tableName, schema policy
  ├─ transactionDomain: StorageTransactionDomain       identity + lease issuance gate
  ├─ directoryAccess: KeyValueDirectoryCatalog         operationAdmission = isolation-level rule
  ├─ resultBytesFactory: PostgreSQLResultBytesFactory  result byte owners (+ lifecycle observer in tests)
  └─ creates PostgreSQLStorageTransaction
           ├─ eager   (withTransaction): engine-owned connection and BEGIN/COMMIT/ROLLBACK
           ├─ lazy    (createTransaction): parked pool connection, transaction-owned lifecycle
           ├─ nested  (createTransaction under ActiveTransactionContext): parent connection, buffer merge
           ├─ PostgreSQLBindingBytes -> ByteBuffer (single copy)
           └─ RangeScanPlan (PostgreSQLRangeScanPlan.swift) / PostgreSQLRangeResult -> cursor
```

Dependency direction: `PostgreSQLStorage -> StorageKit`, `PostgreSQLStorage -> DatabaseTypes`,
`PostgreSQLStorage -> postgres-nio`, `PostgreSQLStorage -> swift-log`. Nothing inside this
package depends on `PostgreSQLStorage`.

## Contracts and Invariants

### Configuration

| Field | Contract |
|---|---|
| `clientConfiguration` | `PostgresClient.Configuration` (host, port, unix socket, credentials, TLS, pool limits). |
| `isolationLevel` | `.serializable` (default), `.repeatableRead`, or `.readCommitted`; emitted as `BEGIN ISOLATION LEVEL <name>`. Catalog mutation and a Partition write binding require `.serializable`; a Partition read binding requires `.repeatableRead` or `.serializable` (PG-3). |
| `tableName` | Bare SQL identifier validated by `validateTableName` before any SQL text is built; an invalid name fails `init` instead of corrupting a query. |
| Schema policy | `createIfNeeded` issues `CREATE TABLE IF NOT EXISTS <tableName> (key BYTEA NOT NULL PRIMARY KEY, value BYTEA NOT NULL)` at `init`; `assumeExists` skips DDL for roles without DDL privileges. |
| `PostgreSQLConnectionBudget` | `cloudRunMaxInstances * connectionsPerInstance + reservedConnections <= cloudSQLMaxConnections`, validated when building a production configuration. |

### Directory realization

| Element | Realization |
|---|---|
| Catalog | `KeyValueDirectoryCatalog(transactionDomain:backend: .postgreSQL, operationAdmission:)` |
| Catalog keys | allocator `[0xFE, 0x61]`, node keys `[0xFE, 0x6E] + Tuple(parentPrefix, kind, name)`; rows in the configured relation beside data rows |
| Directory root prefix | catalog-allocated `Tuple(Int64).pack()`; root Directory uses number `0` |
| Operation admission | `nil` for `.serializable`; otherwise a closure that throws `unsupportedOperation` for every catalog write and every Partition write binding, and — under `.readCommitted` only — for a Partition read binding. Catalog reads and lease issuance stay available at every level |

### Invariants

| ID | Invariant |
|---|---|
| PG-1 | Every transaction runs inside one `BEGIN ISOLATION LEVEL …` block on one connection; `COMMIT` succeeds at most once and `ROLLBACK` completes cleanup; concurrent commit or cancel callers observe the same single completion. |
| PG-2 | Catalog rows and data rows of one transaction commit in the same PostgreSQL transaction. |
| PG-3 | Admission is decided by the configured isolation level, and the two callers need different guarantees. Catalog writes (operations 2, 4, 5 and `openOrInitializeRoot`) and Partition write bindings (`PartitionLease.withWriteAccess`) are admitted only under `.serializable`. A Partition read binding (`PartitionLease.withReadAccess`) is admitted under `.serializable` and `.repeatableRead`, and refused under `.readCommitted`. The read operations (1, 3 and `openRoot`), lease issuance, and data-row operations outside a binding are unaffected at every level. A refused catalog write fails before it writes anything, leaving the store untouched; a refused binding fails before any I/O. A write needs a read-write conflict: a parent a catalog walk observed must conflict with its concurrent removal, a removal with a child created concurrently below it, and a write bound to a Partition generation with that Partition's concurrent removal. `.repeatableRead` gives none of the three, because the walk's parent read, the removal's child scan, and the binding's generation read are each disjoint from the rows the other transaction writes, so both commit and leave a child under a removed parent or data under a removed Partition; only `.serializable` turns those read-write dependencies into a serialization failure the caller's runner retries. A read binding needs only that its generation walk stays true for the span of the closure, which the transaction-level snapshot of `.repeatableRead` already gives. `.readCommitted` takes a fresh snapshot per statement, so a Partition removed after the walk reads back as an empty one — a removed Partition reported as success, which Layer 0 forbids. |
| PG-4 | A nested transaction reuses the parent's connection; `commit()` merges its buffer into the parent and `cancel()` discards it; neither touches the native transaction. |
| PG-5 | A lazily acquired connection is leased exactly once per transaction: concurrent first-touch callers share one acquisition task, and `commit()` or `cancel()` releases the parked connection exactly once. |
| PG-6 | Each bound key or value is copied exactly once from the `ByteString` borrow into final independently owned `ByteBuffer` storage, because PostgresNIO may retain the parameter after the borrow returns. |
| PG-7 | Atomic mutations execute as `pg_advisory_xact_lock` plus row-locked (`FOR UPDATE`) read-modify-write, covering missing rows that `FOR UPDATE` alone cannot lock. |
| PG-8 | `requestShutdown()` closes lease issuance and transaction admission, then stops the pool run task through the storage lifecycle; `waitUntilShutdown()` awaits that cleanup. |
| PG-9 | Point reads replay the write buffer (read-your-writes); range reads flush the buffer first so the server observes the transaction's own writes. |

## Runtime Flows

Directory create under `.serializable` (operation 2):

```text
KeyValueDirectoryCatalog.openOrCreateDirectory
  -> operationAdmission == nil                     (PG-3 not triggered)
  -> transaction.getValue(nodeKey)                 SELECT … (buffer replay first, PG-9)
  -> absent: allocate prefix, setValue(nodeKey)    buffered
  -> commit: flush buffer -> INSERT/UPDATE rows -> COMMIT (PG-1, PG-2)
  -> concurrent creator: serialization failure surfaces as a typed StorageError; the caller's runner retries
```

Lazy transaction lifecycle (PG-5):

```text
createTransaction() -> no connection yet
  -> first async operation -> shared acquisition Task -> client.withConnection { park on continuation }
  -> BEGIN ISOLATION LEVEL … -> operations
  -> commit()/cancel() -> COMMIT/ROLLBACK -> resume continuation -> connection returns to pool
```

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| `PostgresClient` and its run task | `PostgreSQLStorageEngine` | from `init` until shutdown cleanup |
| Parked or eager connection | `PostgreSQLStorageTransaction` (lazy) or engine (`withTransaction`) | until terminal commit or cancel |
| Write buffer, lifecycle phase | `PostgreSQLStorageTransaction` | creation to terminal state; nested buffers merge into the parent |
| Bound parameter bytes | `PostgreSQLBindingBytes` → `ByteBuffer` | until the query completes (owned independently of the source borrow) |
| Result bytes | `PostgreSQLResultBytesOwner` | until the consuming cursor or value is released; observed by the lifecycle observer in tests |
| Lease registration | `LeaseRegistration` (StorageKit) | released at lease end of lifetime |

## Failure, Concurrency, and Constraints

- All native failures cross the boundary as `StorageError` with `backend == .postgreSQL`
  through the engine's `mapError`; serialization failures are typed and left to the
  caller's retry policy.
- Isolation is a StorageKit configuration contract, not a driver default; a level
  below SERIALIZABLE narrows the Directory capability (PG-3) instead of weakening
  its semantics, and READ COMMITTED narrows it further than REPEATABLE READ.
- Connection demand is bounded by `PostgreSQLConnectionBudget` at configuration time;
  the pool never grows past the driver configuration.
- Key, value, mutation, and Directory bounds are the StorageKit bounds.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| D-1…D-12, operations 1–5, root bootstrap, L-1…L-3, L-7, L-8 | `Tests/PostgreSQLStorageTests/PostgreSQLDirectoryConformanceTests.swift` (shared `DirectoryConformanceCase` steps with a unique relation per test) |
| PG-3 | `PostgreSQLDirectoryConformanceTests.readCommittedRejectsCatalogMutation`, `repeatableReadRejectsCatalogMutation`, `repeatableReadRejectsPartitionWriteBinding` (which also asserts that lease issuance and `withReadAccess` still work) |
| PG-1, PG-2, PG-4, PG-9, transaction semantics | `PostgreSQLStorageTests`, `DatabaseFrameworkTransactionContractTests` |
| PG-5, PG-8 | `PostgreSQLClientLifecycleTests` (with `PostgreSQLClientLifecycleLogRecorder`) |
| PG-6 | `PostgreSQLBindingBytesTests` |
| Result byte ownership | `PostgreSQLResultBytesLifecycleTests` |
| Configuration, table name validation, budget | `PostgreSQLConfigurationTests`, `PostgreSQLIntegrationEnvironmentTests` |

The suite requires a reachable server configured through `POSTGRES_TEST_HOST`,
`POSTGRES_TEST_PORT`, `POSTGRES_TEST_USER`, `POSTGRES_TEST_PASSWORD`, and
`POSTGRES_TEST_DB` (`PostgreSQLTestEnvironment`).

Changing the relation schema, the isolation admission rule, or the catalog
placement is a persisted-layout or capability change: update this design, the
Directory component's adapter notes, and the package design before implementation.
