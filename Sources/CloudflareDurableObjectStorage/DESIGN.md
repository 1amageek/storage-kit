# CloudflareDurableObjectStorage

## Purpose and Scope

`CloudflareDurableObjectStorage` is the Cloudflare Durable Object adapter of
storage-kit. It realizes the StorageKit `StorageEngine`, transaction, cursor,
and `DirectoryAccess` contracts as an Embedded-compatible client of the
StorageKit Wire v1 protocol, and it realizes Directories and Partitions as a
transactional catalog through Durable Object SQLite as required by SPEC §7.3.

| Field | Value |
|---|---|
| Level | module |
| Parent | [storage-kit package](../../DESIGN.md) |
| Children | none; the engine, transaction, client, and range scanning types form one design unit |
| Sources | `Sources/CloudflareDurableObjectStorage/` |
| Tests | `Tests/CloudflareDurableObjectStorageTests/` |
| Protocol and host contract | [Docs/CLOUDFLARE_DURABLE_OBJECT_STORAGE_DESIGN.md](../../Docs/CLOUDFLARE_DURABLE_OBJECT_STORAGE_DESIGN.md) (owns StorageKit Wire v1, partition identity, Durable Object SQLite tables, transaction semantics, transport adapters, and the host verification contract) |

This design owns the StorageKit-facing module contract. It links to the
protocol document for wire, host, and SQLite details and does not duplicate
them.

## Responsibilities and Boundaries

| Owns | Does not own |
|---|---|
| `CloudflareDurableObjectStorageConfiguration`: `partitionIdentity`, `client`, `limits`, `monotonicClock` | StorageKit Wire v1 values and codec (`CloudflareDurableObjectStorageWire`) |
| `CloudflareDurableObjectStorageEngine`: readiness gate (`schemaVersion == 1`, `metadataInitialized`), transaction admission, shutdown | HTTP and WASI host transports (`…HTTP`, `…HostTransport`) and the TypeScript host |
| `CloudflareDurableObjectStorageTransaction` and `CloudflareDurableObjectTransactionState`: buffered wire mutations, read and write conflict ranges, observed read version, deadline, phase state machine, single-commit request, unknown-outcome preservation | Durable Object routing and lifecycle (application-owned) |
| `CloudflareDurableObjectStorageClient` protocol, `…WireClient`, `…StorageTransport`, transport failure types, timed calls | Directory contract semantics D-1…D-8 and lease semantics L-1…L-8 ([Directory component](../StorageKit/Directory/DESIGN.md)) |
| `CloudflareDurableObjectLimits` and `…LimitsError`: bounded keys, boundaries, values, mutations, conflict ranges, page size, split points, selector steps | The catalog algorithm and layout marker (`KeyValueDirectoryCatalog`, StorageKit) |
| Range scanning (`…RangeScan`, `…RangeScanning`, `…RangeResult`, `…ByteOrdering`) | `PartitionLeaseRegistry` and `PartitionLease` (StorageKit) |
| Catalog placement: the engine instantiates `KeyValueDirectoryCatalog(transactionDomain:backend: .cloudflareDurableObject)` | Framework binding of `#Directory` declarations |

Authority: the catalog rows stored beneath the configured partition identity in
Durable Object SQLite are the sole existence authority for Directories and
Partitions of this backend (SPEC §12.3, package invariant P-4).

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [storage-kit package](../../DESIGN.md) | parent | package invariants P-1…P-7 | Module graph and package-wide invariants. | Public contract changes propagate to database-framework and database-framework-cloudflare. |
| [StorageKit module](../StorageKit/DESIGN.md) | depends on | `StorageEngine`, `TransactionReadAccess`, `TransactionAccess`, `Transaction`, `StorageEngineLifecycle`, `StorageMonotonicClock`, `StorageError` | Supplies the contracts this adapter realizes. | No `StorageCompactionTransaction` conformance: Durable Object SQLite exposes no vacuum PRAGMAs, so compaction is a typed unsupported capability, never a no-op. |
| [Directory component](../StorageKit/Directory/DESIGN.md) | depends on | `DirectoryAccess`, `KeyValueDirectoryCatalog`, `StorageTransactionDomain` | The catalog realization used unchanged by this module. | Catalog reads and writes travel through the same wire `read`, `range`, and `commit` operations as data. |
| [StorageKitConformance](../StorageKitConformance/DESIGN.md) | used by | `DirectoryConformanceCase` | Shared fixture executed by `CloudflareDurableObjectDirectoryConformanceTests` over the in-memory transport fixture. | The in-memory transport proves wire and transaction semantics; host integration is proven by the host verification contract in the protocol document. |
| [Protocol and host design](../../Docs/CLOUDFLARE_DURABLE_OBJECT_STORAGE_DESIGN.md) | coordinates with | StorageKit Wire v1 operations, status codes, limits, transaction state machine, SQLite tables | Owns everything below the `CloudflareDurableObjectStorageClient` boundary. | Wire changes are protocol version changes; Version 1 has no negotiation path. |
| `CloudflareDurableObjectStorageWire` | depends on | request and response values, codec, `StoragePartitionIdentity`, `StorageWireLimits` | Foundation-free bounded representation for Native, WASM, and Embedded Swift. | Byte ownership: requests are borrowed synchronously; responses cross heaps once into final Swift-owned storage. |

## Architecture

```text
CloudflareDurableObjectStorageEngine
  ├─ configuration: partitionIdentity, client, limits, monotonicClock
  ├─ readiness gate at init: client.readiness(...) -> schemaVersion == 1 && metadataInitialized
  ├─ transactionDomain: StorageTransactionDomain      identity + PartitionLeaseRegistry
  ├─ directoryAccess: KeyValueDirectoryCatalog        (StorageKit) over wire read/range/commit
  └─ creates CloudflareDurableObjectStorageTransaction
           ├─ state (Mutex): mutations, read/write conflict ranges, phase, observedReadVersion, deadline
           ├─ reads: client.read / client.range   (pinned to observedReadVersion, limits enforced)
           ├─ commit: one client.commit(mutations, conflict ranges, read version)
           └─ range scanning -> CloudflareDurableObjectRangeResult -> cursor

CloudflareDurableObjectStorageClient (protocol)
  └─ CloudflareDurableObjectStorageWireClient -> CloudflareDurableObjectStorageTransport
        ├─ CloudflareDurableObjectStorageHTTP        (native URLSession)
        └─ CloudflareDurableObjectStorageHostTransport (Embedded WASM host ABI)
```

Dependency direction: `CloudflareDurableObjectStorage -> StorageKit`,
`-> DatabaseTypes`, `-> CloudflareDurableObjectStorageWire`. The transport
modules depend on this module; nothing else inside the package does.

## Contracts and Invariants

### Configuration

| Field | Contract |
|---|---|
| `partitionIdentity: StoragePartitionIdentity` | The Durable Object partition every request of this engine addresses; two engines with distinct identities never observe each other's keys or catalog. |
| `client: any CloudflareDurableObjectStorageClient` | Executes `read`, `range`, `commit`, `readiness`, `rangeSize`, and `rangeSplitPoints`; the engine holds no transport knowledge. |
| `limits: CloudflareDurableObjectLimits` | Bounded key, boundary, value, stored key-value, mutations per commit, conflict ranges per commit, range page limit, split points, selector resolution steps; `.default` matches the host limits. Violations fail with typed `CloudflareDurableObjectLimitsError` before dispatch. |
| `monotonicClock: any StorageMonotonicClock` | Source of transaction deadlines and timed calls. |

### Directory realization

| Element | Realization |
|---|---|
| Catalog | `KeyValueDirectoryCatalog(transactionDomain:backend: .cloudflareDurableObject)` |
| Catalog keys | allocator `[0xFE, 0x61]`, node keys `[0xFE, 0x6E] + Tuple(parentPrefix, kind, name)`; stored in the partition's key-value table beside data |
| Directory root prefix | catalog-allocated `Tuple(Int64).pack()`; root Directory uses number `0` |
| Layout marker | the catalog's marker state machine (Directory component); no host-specific marker |

### Invariants

| ID | Invariant |
|---|---|
| DO-1 | An engine is usable only after the readiness gate succeeded; a host without schema v1 or without initialized metadata fails `init` with `resourceUnavailable` (`operation: .initialize`). |
| DO-2 | A transaction's phase moves `open → committing → committed`, `open → cancelling → cancelled`, or to `failed`; a commit whose outcome is unknown ends in `commitUnknown` and every later operation reports `commitUnknownResult`, never success. |
| DO-3 | All mutations and conflict ranges of one transaction are sent in one `commit` request together with the observed read version, so catalog and data mutations are atomic on the host. |
| DO-4 | Reads pin the first observed read version and re-use it for every later read of the transaction; the host rejects a commit whose conflict ranges were invalidated, and the adapter surfaces that as a typed conflict. |
| DO-5 | Every request is validated against `limits` before dispatch; the adapter never truncates, splits, or silently drops a mutation, range, or selector step. |
| DO-6 | Transport and host failures reach the caller as `StorageError` with `backend == .cloudflareDurableObject`, carrying the transport failure stage; a truncated, mismatched, or suspended response never becomes a value. |
| DO-7 | `requestShutdown()` closes lease issuance and transaction admission; admitted transactions finish their own terminal request; `waitUntilShutdown()` awaits that cleanup. |
| DO-8 | Physical compaction is not a capability of this adapter; the absence is a type-level fact and no runtime operation reports an empty success. |

## Runtime Flows

Transaction commit (DO-2, DO-3):

```text
open: setValue/clear/atomicOp -> buffered StorageWireWriteOperation + write conflict range
      getValue/rangeCursor    -> client.read / client.range (observedReadVersion pinned, DO-4)
commit():
  phase open -> committing
  -> limits check (DO-5)
  -> client.commit(mutations, readConflictRanges, writeConflictRanges, readVersion)
  -> success   -> committed (committedVersion recorded)
  -> conflict  -> failed(typed conflict)
  -> transport failure after dispatch -> commitUnknown(commitUnknownResult)
```

Directory operation (any of operations 1–8):

```text
KeyValueDirectoryCatalog -> transaction.getValue / setValue / clear on catalog keys
  -> same buffer and commit request as the transaction's data operations (DO-3)
```

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Configuration and client | `CloudflareDurableObjectStorageEngine` | engine lifetime |
| `CloudflareDurableObjectTransactionState` (mutations, conflict ranges, phase, versions, deadline) | `CloudflareDurableObjectStorageTransaction` behind a `Mutex` | creation to terminal phase |
| Response bytes | final Swift-owned storage produced by the transport | until the consuming cursor or value is released |
| Lease registrations and intents | `PartitionLeaseRegistry` (StorageKit) | released by lease end of lifetime / transaction completion |

## Failure, Concurrency, and Constraints

- Deadlines come from `monotonicClock`; a timed call that exceeds its budget fails
  typed and marks the transaction according to DO-2.
- Concurrent operations on one transaction serialize on the state `Mutex`; no I/O
  runs while the lock is held.
- Bounds are `CloudflareDurableObjectLimits` in addition to the StorageKit bounds;
  the stricter bound applies.
- The application owns Durable Object routing, authentication, and lifecycle; this
  module never assumes a public Worker route.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| D-1…D-8, operations 1–8, layout marker, L-1…L-3, L-7, L-8 | `Tests/CloudflareDurableObjectStorageTests/CloudflareDurableObjectDirectoryConformanceTests.swift` (shared `DirectoryConformanceCase` steps over `InMemoryCloudflareDurableObjectStorageTransport`) |
| DO-2, DO-3, DO-4 | `CloudflareDurableObjectStorageTransactionTests`, `CloudflareDurableObjectStorageSemanticsTests`, `CloudflareDurableObjectStorageValueSemanticsTests` |
| DO-5, DO-6 | `CloudflareDurableObjectStorageWireClientTests` with the `ConfiguredFailure…`, `MismatchedOperation…`, `TruncatedResponse…`, `Suspending…`, and `BorrowedResponse…` transport fixtures |
| Deadlines | `CloudflareDurableObjectStorageTimeoutTests` |
| Byte ordering | `CloudflareDurableObjectByteOrderingTests` |
| Wire codec, golden vectors, byte ownership | `Tests/CloudflareDurableObjectStorageWireTests/` |
| Transports | `Tests/CloudflareDurableObjectStorageHTTPTests/`, `Tests/CloudflareDurableObjectStorageHostTransportTests/` |
| Host integration | verification contract in the protocol document |

Changing limits, the readiness gate, the commit request composition, or the
catalog placement is a protocol or persisted-layout change: update the protocol
document, this design, the Directory component's adapter notes, and the package
design before implementation.
