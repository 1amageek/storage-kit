# storage-kit

## Purpose and Scope

`storage-kit` is the backend-neutral storage foundation of the Database
workspace. It owns transactional key-value access, the Tuple V1 encoding,
Directory and Partition placement, partition leases, and the adapters that
realize those contracts on FoundationDB, SQLite, PostgreSQL, and Cloudflare
Durable Objects.

| Field | Value |
|---|---|
| Design level | package (also the storage-kit repository root) |
| Parent | [workspace DESIGN.md](../DESIGN.md) |
| Product architecture authority | [SPEC.md](../SPEC.md) §7–§9, §12.3, §24.1, §24.2 |
| Children | [StorageKit module](Sources/StorageKit/DESIGN.md); [FDBStorage module](Sources/FDBStorage/DESIGN.md); [SQLiteStorage module](Sources/SQLiteStorage/DESIGN.md); [PostgreSQLStorage module](Sources/PostgreSQLStorage/DESIGN.md); [CloudflareDurableObjectStorage module](Sources/CloudflareDurableObjectStorage/DESIGN.md) |
| Operation and test rules | [AGENTS.md](AGENTS.md) |

This document owns package-level responsibilities, the module graph, the
package-wide invariants, and the index of module designs. Module and component
contracts are owned by their own `DESIGN.md` and are not duplicated here.

## Responsibilities and Boundaries

| Owned by storage-kit | Not owned by storage-kit |
|---|---|
| Transaction admission, commit-at-most-once, authoritative cancel, shutdown | Query meaning, schema, authorization, result publication (database-framework) |
| Bounded point reads, bounded range reads, cursors, mutation byte metering | Declaration syntax and validation (database-kit) |
| Tuple V1 encoding and `Subspace` derivation | Wire encodings (DatabaseWire in database-kit) |
| Directory and Partition placement, catalog existence authority, root bootstrap | Reserved Directory names `system`, `database-framework`, `data` (Framework concern) |
| `PartitionLease`, `BoundReadAccess`, `BoundWriteAccess` issuance | Which Partition a request may use (Framework authority) |
| One backend adapter per module with typed failure conversion | Process lifecycle, listeners, hosting (database-server, Cloudflare host) |

Authority boundary: StorageKit decides whether a Directory or Partition exists
and whether an address is well formed. It never decides whether a caller is
allowed to use it.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [workspace DESIGN.md](../DESIGN.md) | parent | system context, global invariants | Indexes this package as the foundational storage dependency. | Package boundary changes must be reflected in the workspace index and `Ecosystem.json`. |
| [SPEC.md](../SPEC.md) | product architecture authority | §7 capability list, §8 values, operations, and root bootstrap, §9 leases, §24 verification | Defines what this package must guarantee. | Any deviation is recorded in the owning module design, not silently absorbed. |
| [StorageKit module](Sources/StorageKit/DESIGN.md) | child | `StorageEngine`, `Transaction`, `TransactionReadAccess`, `TransactionAccess`, `DirectoryAccess`, lease types, Tuple, Subspace | Platform-neutral contracts and the InMemory reference engine. | Adapters depend on these contracts only; they never reach into InMemory internals. |
| `database-types` | depends on | `ByteString`, `UUID`, bounded byte owners | Foundation-free primitives used by keys, values, and Tuple elements. | `ByteString` ownership rules apply to every byte path; no `Data` or `[UInt8]` re-materialization on repeated paths. |
| `fdb-swift-bindings` | depends on (FDBStorage only) | `Database`, `Transaction`, `DirectoryLayer`, `DirectoryType` | Safe FoundationDB C client ownership. | A StorageKit Partition on FDB is a native `partition` node, so partitions nest and the adapter defines no custom layer type of its own (owned by the FDBStorage module design). |
| `postgres-nio` | depends on (PostgreSQLStorage only) | connection and query execution | PostgreSQL wire client. | Isolation level selection is a StorageKit configuration contract, not a driver default. |
| database-framework | used by | every public contract of the StorageKit module | Composes containers, kernels, and features on top of Directories and leases. | Framework never holds a raw Partition address as authority; it holds leases. |

## Architecture

```text
                       +-----------------------------+
                       |          StorageKit          |
                       |  Storage/  Tuple/  Directory/ |
                       |  InMemory/ (reference engine) |
                       +-------+----------+-----------+
                               ^          ^
              +----------------+          +-----------------+
              |                |          |                 |
      +-------+------+ +-------+------+ +--+-----------+ +---+--------------------------+
      |  FDBStorage  | | SQLiteStorage| |PostgreSQLStor| |CloudflareDurableObjectStorage|
      | DirectoryLayer| | KV catalog   | | KV catalog   | | KV catalog                   |
      +-------+------+ +--------------+ +------+-------+ +---+--------------------------+
              |                                 |             |
     fdb-swift-bindings                    postgres-nio   CloudflareDurableObjectStorageWire
```

Support modules: `StorageKitSystemClock` (system clock adapter),
`StorageKitFoundation` (Foundation bridging), `StorageKitConformance` (shared
semantic fixture library consumed by every adapter test target),
`CloudflareDurableObjectStorageTesting`, `…HTTP`, `…HostTransport`.

Dependency direction is strictly adapter → StorageKit → database-types. No
adapter depends on another adapter, and StorageKit depends on no adapter.

## Contracts and Invariants

Package-wide invariants (each is owned in detail by the module design):

| ID | Invariant | Owner |
|---|---|---|
| P-1 | One `StorageTransactionDomain` per engine instance; a transaction, Directory, Partition, or lease is valid only inside its domain. | StorageKit module |
| P-2 | `commit` succeeds at most once per transaction; authoritative `cancel` completes backend cleanup; operation failure and cleanup failure are both preserved. | StorageKit module, each adapter |
| P-3 | Unsupported or malformed operations produce typed `StorageError`; empty success is never a substitute. | every module |
| P-4 | Every adapter exposes exactly one `DirectoryAccess` realization that is the sole existence authority for Directories and Partitions in that backend. | Directory component, each adapter |
| P-5 | Tuple V1 byte layout is frozen by `Tests/StorageKitTests/TupleV1GoldenVectorTests.swift`. | Tuple component |
| P-6 | Shutdown rejects new transactions and new lease issuance and completes admitted backend cleanup. | StorageKit module, each adapter |
| P-7 | Every production adapter passes the same `StorageKitConformance` fixture that the InMemory reference engine passes. | AGENTS.md test procedure |

## Failure, Concurrency, and Constraints

- All failures cross module boundaries as `StorageError` with a `code`,
  `operation`, and `backend`; adapters convert backend errors at the boundary.
- Engines are `Sendable` classes; shared mutable state lives behind `Mutex` or
  an actor. Bound access and leases are noncopyable.
- Bounds (key size, value size, mutation bytes, point-read maximum, Directory
  component and depth limits, list page size) are correctness contracts and are
  owned by the module or component that enforces them.

## Verification and Change Impact

| Change | Re-verify |
|---|---|
| Any public StorageKit contract | this design, the module design, all adapter modules, database-framework binding |
| Adapter-internal change | that adapter's module design and its test target plus the shared conformance fixture |
| Tuple encoding | golden vectors; any byte-layout change is a layout change |
| Directory catalog layout | Directory component design, `incompatibleStorageLayout` state machine, every KV-catalog adapter |

Test procedure, expected counts, and service lifecycle are owned by
[AGENTS.md](AGENTS.md). Tests run through `xcodebuild test` with an external
timeout and a unique `.xcresult`.
