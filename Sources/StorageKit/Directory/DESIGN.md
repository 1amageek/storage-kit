# Directory

## Purpose and Scope

The Directory component owns storage placement: hierarchical Directories,
isolated Partitions, the transactional catalog that is the sole existence
authority in key-value backends, the layout-version marker and its bootstrap
state machine, and the noncopyable lease and bound-access types that confine
active work to one Partition.

| Field | Value |
|---|---|
| Design level | component |
| Parent | [StorageKit module](../DESIGN.md) |
| Children | none |
| Product authority | SPEC §8, §9, §10.3, §12.3 |

## Responsibilities and Boundaries

| Owns | Does not own |
|---|---|
| `DirectoryPath`, `PartitionID`, `StorageAddress`, `Directory`, `Partition` values and their bounds | Reserved names (`system`, `database-framework`, `data`) and Framework Subspace layout |
| `DirectoryAccess` contract and the eight semantic operations | Authorization of any operation |
| `KeyValueDirectoryCatalog`: the catalog realization for InMemory, SQLite, PostgreSQL, Cloudflare DO | The FDB realization (owned by `FDBStorage`, which must satisfy the same contract) |
| `StorageLayoutMarker` and the InspectRoot → OpenV1 / InitializeV1 / Reject state machine | Deleting or rewriting roots (never in production) |
| `PartitionLeaseRegistry`, `PartitionLease`, `BoundReadAccess`, `BoundWriteAccess` | Deciding which Partition a request may lease |

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [StorageKit module](../DESIGN.md) | parent | `TransactionReadAccess`, `TransactionAccess`, `StorageEngine.transactionDomain`, `StorageError`, `Subspace`, `Tuple` | Supplies the transaction and encoding contracts the catalog is built on. | Catalog keys use Tuple V1; a Tuple change is a layout change. |
| [FDBStorage module](../../FDBStorage/DESIGN.md) | coordinates with | `DirectoryAccess`, `PartitionLeaseRegistry.registerIntent` | Realizes the same contract over the native FoundationDB Directory Layer (layout FD-1…FD-9). | Partitions are custom-typed directories because native partitions cannot nest; layer type is verified on open; listing below a missing parent fails with `keyNotFound` there. |
| `SQLiteStorage`, `PostgreSQLStorage`, `CloudflareDurableObjectStorage` | used by | `KeyValueDirectoryCatalog` | Each engine instantiates one catalog bound to its domain. | PostgreSQL rejects `readCommitted` isolation for catalog mutation (owned by PostgreSQLStorage). |
| database-framework | used by | every public type here | Binds `#Directory` declarations and the kernel to leases. | Framework proves its own Subspaces empty before requesting Partition removal. |

## Architecture

```text
 DirectoryAccess (protocol)
   |
   +-- KeyValueDirectoryCatalog (StorageKit)      +-- FDBDirectoryAccess (FDBStorage)
   |     reserved prefix 0xFE                      |     native DirectoryLayer
   |     marker / allocator / node keys            |     custom-typed partition dirs
   v                                               v
 TransactionReadAccess / TransactionAccess (caller's transaction)

 StorageEngine.leasePartition
   -> PartitionLeaseRegistry.reserve            (domain, shutdown, intent check)
   -> DirectoryAccess.resolve(address)          (generation check in caller txn)
   -> PartitionLease (~Copyable)
        -> withReadAccess  -> BoundReadAccess  (~Copyable, borrowed)
        -> withWriteAccess -> BoundWriteAccess (~Copyable, borrowed)
```

## Contracts and Invariants

### Values

| Type | Definition | Bounds (`DirectoryLimits`) |
|---|---|---|
| `DirectoryPath` | ordered nonempty `[String]` of exact UTF-8 components; no normalization, no separator parsing | component 1…255 UTF-8 bytes; depth ≤ 64 |
| `PartitionID` | nonempty opaque `ByteString`; `Comparable` by bytes | 1…1024 bytes |
| `StorageAddressStep` | `.directory(String)` or `.partition(PartitionID)` | component bounds above |
| `StorageAddress` | ordered steps from the root; empty = root | depth ≤ 64 |
| `Directory` | `domain` (identity), `address`, `root: Subspace`; generation = `root.prefix` | — |
| `Partition` | `id`, `root: Directory` whose last address step is `.partition(id)` | — |

Value validation fails with `DirectoryAddressError`; `DirectoryAccess`
operations convert it to `StorageError.invalidDirectoryAddress`.

### `DirectoryAccess` — exactly these semantic operations

| # | Operation | Class | Input access | Absence | Notes |
|---|---|---|---|---|---|
| 1 | `openDirectory(_:in:transaction:)` | Read | `TransactionReadAccess` | returns `nil` | never creates |
| 2 | `openOrCreateDirectory(_:in:transaction:)` | Write | `TransactionAccess` | creates | atomic with the caller's transaction |
| 3 | `openPartition(_:in:transaction:)` | Read | `TransactionReadAccess` | returns `nil` | never creates |
| 4 | `openOrCreatePartition(_:in:transaction:)` | Write | `TransactionAccess` | creates | atomic with the caller's transaction |
| 5 | `listDirectories(in:after:limit:transaction:)` | Read | `TransactionReadAccess` | empty page | `limit` 1…1000, ordered by encoded key, `after` exclusive |
| 6 | `listPartitions(in:after:limit:transaction:)` | Read | `TransactionReadAccess` | empty page | same bounds |
| 7 | `moveChild(_:in:to:in:transaction:)` | Write | `TransactionAccess` | `keyNotFound` | Directory: atomic rename; Partition: `unsupportedOperation`; into own subtree: `invalidDirectoryAddress`; target exists: `invalidOperation`; leased subtree: `directoryLeased` |
| 8 | `removeChild(_:in:transaction:)` | Write | `TransactionAccess` | `keyNotFound` | child Directories/Partitions present or data keys present: `directoryNotEmpty`; leased subtree: `directoryLeased` |

The root is a Directory. `openRoot(transaction:)` is operation 1 applied to
the root path (absence = uninitialized empty store, returned as `nil`), and
`openOrInitializeRoot(transaction:)` is operation 2 applied to the root path.
Both run the layout state machine below. `openDirectory(at:in:transaction:)`
walks a `DirectoryPath` with operation 1 and is a convenience, not a ninth
operation.

Every operation checks `transaction.transactionDomain === parent.domain ===
catalog.domain`; a mismatch fails `storageDomainMismatch` before any I/O.

### Directory guarantees (D-1…D-8)

| ID | Guarantee | Enforcement |
|---|---|---|
| D-1 | Hierarchical naming with exact UTF-8 components | `DirectoryPath` / step validation |
| D-2 | Stable resolution: the same address resolves to the same `root.prefix` until moved or removed | catalog node stores the root number |
| D-3 | Opaque root: callers cannot derive a prefix from a name | prefixes are allocator numbers, never name-derived |
| D-4 | Disjoint siblings: distinct children never share a prefix | Tuple-encoded `Int64` root numbers are prefix-free |
| D-5 | Create, move, remove are atomic with the caller's transaction | all catalog writes go through the caller's `TransactionAccess` |
| D-6 | Read-only open never creates | read operations take `TransactionReadAccess` only |
| D-7 | Domain identity: values from one engine are rejected by another | `storageDomainMismatch` |
| D-8 | Bounded enumeration | `limit` 1…1000, else `invalidOperation` |

A resolver that reports every path as present is non-conforming; the fixture
proves absence for unknown children.

### `KeyValueDirectoryCatalog` layout (V1)

Reserved prefix `0xFE`. All catalog keys are below it; all Directory roots are
Tuple-encoded `Int64` root numbers, which never start with `0xFE` or `0xFF`.

| Key | Value | Meaning |
|---|---|---|
| `FE 6C` | `53 4B 4C 01` | layout marker: `"SKL"` + version 1 |
| `FE 61` | `Tuple(Int64(next)).pack()` | next root number (read-modify-write in the caller's transaction) |
| `FE 6E ‖ Tuple(parentRootPrefix: bytes, kind: Int64, name).pack()` | `Tuple(Int64(rootNumber)).pack()` | child node; `kind` 0 = Directory (`name: String`), 1 = Partition (`name: bytes`) |

- Root Directory = root number 0, prefix `Tuple(Int64(0)).pack()` = `14`.
  The root does not use the empty prefix, so root-level data can never collide
  with child roots or the catalog.
- Child listing is a bounded range read under
  `FE 6E ‖ Tuple(parentRootPrefix, kind).pack()`; names are decoded from keys.
- Move renames the node (delete old node key, write new node key with the same
  root number); child nodes are keyed by the moved Directory's own prefix and
  are unaffected.
- Remove requires: node exists; no node under
  `FE 6E ‖ Tuple(childRootPrefix).pack()`; no key in
  `childRoot.prefixRange()`; no lease on the subtree.
- "Root is empty" (InspectRoot) is a limit-1 range read over `[] ..< [0xFF]`.
  Keys starting with `0xFF` are the FoundationDB system keyspace convention and
  never the first byte of a Tuple-encoded key; StorageKit treats them as
  outside the user keyspace on every backend.

Concurrent creation of the same child races on the allocator key; the
backend's conflict detection (FDB, InMemory, PostgreSQL repeatable-read or
serializable, SQLite serialization) makes one transaction fail typed. This is
why the allocator is a read-modify-write and not an atomic add.

### Layout marker state machine (§10.3)

```text
InspectRoot
  marker == V1 bytes                 -> OpenV1        (root number 0)
  marker absent, keyspace empty      -> read:  nil
                                        write: InitializeV1 (marker + allocator = 1, same transaction)
  marker absent, keyspace nonempty   -> Reject  incompatibleStorageLayout
  marker present, other bytes        -> Reject  incompatibleStorageLayout
```

No dual read/write of two layouts; production never deletes or rewrites a
root. A V0 deterministic-prefix store is "marker absent, nonempty" and is
rejected.

### Leases (L-1…L-8)

| ID | Invariant | Enforcement |
|---|---|---|
| L-1 | Lease, transaction, and Partition domains match | `storageDomainMismatch` at issuance and at every bind |
| L-2 | A lease blocks removal or move of its subtree | registry check in operations 7 and 8 → `directoryLeased` |
| L-3 | A stale generation fails; work is never redirected | issuance re-resolves the address in the caller's transaction and compares `root.prefix` → `staleLease` |
| L-4 | Read binding cannot mutate | `BoundReadAccess` has no mutation members |
| L-5 | Write binding cannot commit or cancel | `BoundWriteAccess` has no lifecycle members |
| L-6 | Bound access and cursors cannot escape the closure | noncopyable, borrowed; cursors validate the binding scope before every advance → `staleLease` |
| L-7 | Releasing the last lease is not success | release returns nothing; the caller's transaction outcome is the result |
| L-8 | Keys are confined to the Partition | every key must carry `partition.root.root.prefix`; range end may equal `strinc(prefix)` only through `firstGreaterOrEqual`; `getKey` returns `nil` when the resolved key lies outside |

Registry (per `StorageTransactionDomain`, in-process):

- `reserve(address)` runs before validation so a concurrent removal sees the
  lease; it is rejected after `requestShutdown()` (`resourceUnavailable`) and
  while a removal or move intent covers the address (`staleLease`).
- Operations 7 and 8 register a subtree intent keyed by the caller's
  transaction object. The intent lives until `TransactionLifecycleOwner`
  completes the transaction or, for transactions committed directly, until the
  transaction object is deallocated. The registry holds the transaction only
  weakly inside its `Mutex`; the direction of error is always conservative
  (a lease is refused, never issued over a pending removal).
- Cross-process: the registry cannot see other processes. There the invariant
  is L-3 (staleness at issuance), not prevention; every backend already
  behaves this way and the fixture proves it by removing and recreating a
  Partition between two resolutions.

## Runtime Flows

Lease issuance:

```text
leasePartition(partition, transaction)
  1. domain check (engine, partition.root.domain, transaction)   -> storageDomainMismatch
  2. registry.reserve(partition.root.address)                    -> resourceUnavailable | staleLease
  3. walk address from openRoot through open* in `transaction`   -> nil or prefix mismatch: release, staleLease
  4. return PartitionLease(partition, registration)
```

Bound read:

```text
lease.withReadAccess(transaction) { access in ... }
  - domain check, registration active check
  - BindingScope opened; BoundReadAccess borrowed to the closure
  - every read validates key containment (L-8) and scope/lease (L-6)
  - scope closed on return; escaped cursors fail on next advance
```

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Catalog nodes, allocator, marker | backend keyspace, mutated only through the caller's transaction | durable |
| Lease registrations and intents | `PartitionLeaseRegistry` (`Mutex`) | registration: until `release()` or `PartitionLease` deinit; intent: until owner completion or transaction deallocation |
| Binding scope | `PartitionLease.withReadAccess` / `withWriteAccess` | one closure |

## Failure, Concurrency, and Constraints

New `StorageError.Code` cases: `incompatibleStorageLayout`,
`directoryNotEmpty`, `directoryLeased`, `storageDomainMismatch`, `staleLease`,
`invalidDirectoryAddress`. All have retry disposition `never`.

`DirectoryLimits` (owned operational contract):

| Limit | Value | Rationale |
|---|---|---|
| `maximumComponentByteCount` | 255 | matches common filesystem name bounds; keeps node keys far below backend key limits |
| `maximumDepth` | 64 | bounds address walks and lease validation cost |
| `maximumPartitionIDByteCount` | 1024 | canonical field-number + FieldValue frames fit; node key stays below FDB's 10 KB key bound |
| `maximumListLimit` | 1000 | one bounded page per range read |

Changing a limit requires re-running the shared fixture on every adapter and
confirming node keys stay within each backend's key bound.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| Values and bounds | `Tests/StorageKitTests/DirectoryValueTests.swift` |
| D-1…D-8, state machine, operations 1–8, L-1…L-3, L-7, L-8 | `StorageKitConformance` `DirectoryConformanceCase` run by `InMemoryDirectoryConformanceTests`, `SQLiteDirectoryConformanceTests`, `PostgreSQLDirectoryConformanceTests`, `CloudflareDurableObjectDirectoryConformanceTests`, and `FDBDirectoryConformanceTests` (the layout-marker step is KV-only; FDB proves layer-type rejection in its own suite) |
| L-4, L-5 (compile-level), L-6 escape | `Tests/StorageKitTests/PartitionLeaseTests.swift` |

Changing the catalog layout, the marker bytes, or any operation semantics
requires updating this design first, then the StorageKit module design, then
every adapter module, then database-framework binding (F-15 in `PROGRESS.md`).
