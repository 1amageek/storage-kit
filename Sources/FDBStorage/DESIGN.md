# FDBStorage

## Purpose and Scope

`FDBStorage` is the FoundationDB adapter of storage-kit. It realizes the
StorageKit `StorageEngine`, transaction, cursor, and `DirectoryAccess`
contracts over `fdb-swift-bindings`, and it realizes Directories and
Partitions on the native FoundationDB Directory Layer as required by
SPEC §7.3.

| Field | Value |
|---|---|
| Level | module |
| Parent | [storage-kit package](../../DESIGN.md) |
| Children | none; the module is one design unit. `FDBDirectoryAccess` and `FDBDirectoryLayout` are the Directory realization, `FDBStorageEngine`, `FDBStorageTransaction`, and the cursor types are the transaction realization |
| Sources | `Sources/FDBStorage/` |
| Tests | `Tests/FDBStorageTests/` |

## Responsibilities and Boundaries

| Owns | Does not own |
|---|---|
| FDB client startup and the retained database handle of one engine | The FoundationDB C client and its safe Swift ownership (`fdb-swift-bindings`) |
| `FDBStorageTransaction`: read, write, range, atomic, commit-at-most-once, cancel, error conversion, and the exclusive Directory operation window | Directory contract semantics D-1…D-8 and lease semantics L-1…L-8 ([Directory component](../StorageKit/Directory/DESIGN.md)) |
| Borrowed result byte lifetime bound to the retained owner | Native Directory Layer algorithms: HCA prefix allocation, node metadata, version keys |
| `FDBDirectoryLayout`: root path, layer types, kind-prefixed native names | `PartitionLeaseRegistry` and `PartitionLease` (StorageKit) |
| `FDBDirectoryAccess`: the eight Directory operations, layer type verification, stale parent rejection, emptiness checks, intent registration order, native error mapping | Framework binding of `#Directory` declarations |

Authority: the native Directory Layer metadata below `Configuration.rootPath`
is the sole existence authority for this backend (SPEC §12.3). No StorageKit
marker key exists; the layer type of a node is the layout marker.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [storage-kit package](../../DESIGN.md) | parent | package invariants P-1…P-7 | Module graph and package-wide invariants. | Public contract changes propagate to database-framework. |
| [StorageKit module](../StorageKit/DESIGN.md) | depends on | `StorageEngine`, `TransactionReadAccess`, `TransactionAccess`, `StorageError`, `Subspace`, `StorageTransactionDomain` | Supplies the contracts this adapter realizes. | `Subspace` here is `StorageKit.Subspace`; the bindings own a different `Subspace` type. |
| [Directory component](../StorageKit/Directory/DESIGN.md) | depends on | `DirectoryAccess`, `Directory`, `Partition`, `StorageAddress`, `DirectoryLimits`, `PartitionLeaseRegistry.registerIntent` | Defines the operations, failure codes, bounds, and intent rules this module must satisfy. | Intent registration happens after validation, before the native mutation (FD-7). |
| [StorageKitConformance](../StorageKitConformance/DESIGN.md) | used by | `DirectoryConformanceCase` | Shared fixture executed by `FDBDirectoryConformanceTests`. | `verifyLayoutRejection` plants KV marker keys and does not apply; FD-2 is proven by `foreignLayerTypeIsRejected` instead. |
| `fdb-swift-bindings` | depends on | `FDBClient`, `DatabaseProtocol`, `TransactionProtocol`, `DirectoryLayer`, `DirectorySubspace`, `DirectoryType`, `DirectoryError` | Native client, transactions, and Directory Layer. | `createOrOpen` creates missing ancestors as untyped nodes and writes the layer version key when absent; `list` is unordered; `move` keeps the prefix. |

## Architecture

```text
FDBStorageEngine
  ├─ database: Mutex<(any DatabaseProtocol)?>      client startup, shutdown release
  ├─ transactionDomain: StorageTransactionDomain   identity + PartitionLeaseRegistry
  ├─ directoryAccess: FDBDirectoryAccess ──uses──> FDBDirectoryLayout
  │        │                                          (layer types, "d"/"p" names, hex ids)
  │        └─ per operation: DirectoryLayer(database:) from fdb-swift-bindings
  │                          node subspace 0xFE, content subspace empty
  └─ creates FDBStorageTransaction
           ├─ fdbTransaction: any TransactionProtocol
           ├─ withDirectoryOperation(...)   exclusive window used by FDBDirectoryAccess
           └─ convertFDBError / convertBackendError
```

Dependency direction: `FDBStorage -> StorageKit`, `FDBStorage -> DatabaseTypes`,
`FDBStorage -> FoundationDB` (bindings). Nothing depends on `FDBStorage` inside
this package.

## Contracts and Invariants

### Configuration

| Field | Contract |
|---|---|
| `rootPath: [String]` | Native path that owns the engine's root Directory. Default `Configuration.defaultRootPath = ["storage-kit"]`. Must be non-empty with non-empty components; otherwise `StorageError(.invalidOperation, operation: .open)` before any client work. Engines with distinct root paths on one cluster never observe each other's Directories. |

### Layout V1

| StorageKit node | Native path | Native layer type |
|---|---|---|
| root | `rootPath` | `custom("storage-kit.directory.v1")` |
| `.directory(name)` child | parent path + `"d" + name` | `custom("storage-kit.directory.v1")` |
| `.partition(id)` child | parent path + `"p" + lowercaseHex(id.bytes)` | `custom("storage-kit.partition.v1")` |

`Directory.root.prefix` is the native HCA-allocated prefix of the node.
Partitions are custom-typed directories because native partitions cannot
nest and StorageKit Partitions may contain Directories and Partitions.

### Invariants

| ID | Invariant |
|---|---|
| FD-1 | The native layer below `rootPath` is the sole existence authority; no StorageKit-side existence or marker keys exist. |
| FD-2 | Every open of a node (root, Directory, Partition, move source, removal target) verifies the layer type against the requested kind; absence or a foreign type fails with `incompatibleStorageLayout(.unknownMarker(actualTypeBytes))` and the node is never adopted. |
| FD-3 | Read operations never write: `openRoot` checks `exists` before `open`, so an uninitialized cluster is observed without touching the layer version key. |
| FD-4 | Create and move never resurrect a stale parent or destination: a missing parent or destination fails with `keyNotFound` instead of being created as an untyped native node. |
| FD-5 | Listings filter by the kind prefix of the native name, order by UTF-8 or identifier bytes, apply `after` exclusively, and honor `limit` in `1...DirectoryLimits.maximumListLimit`; foreign-named children are skipped; an undecodable Partition name fails with `dataCorruption`. |
| FD-6 | Removal requires no native children of any kind, including foreign nodes, and no key in `[prefix, strinc(prefix))`; either violation fails with `directoryNotEmpty` before any mutation. |
| FD-7 | A move or removal registers its lease intent after every validation succeeded and immediately before the native mutation, so a rejected operation leaves no pending intent in the transaction. |
| FD-8 | Only Directories move; a Partition step on either side fails with `unsupportedOperation`; a target inside the moved subtree fails with `invalidDirectoryAddress(.targetInsideMovedSubtree)`. |
| FD-9 | Every Directory operation of a transaction runs inside `withDirectoryOperation`, which rejects a transaction of another engine (`storageDomainMismatch`), enforces exclusivity with the transaction's own access, and marks operations 5–8 as mutations. |

### Native error mapping

| `DirectoryError` | `StorageError.Code` |
|---|---|
| `directoryNotFound` | `keyNotFound` |
| `directoryAlreadyExists`, `prefixInUse`, `prefixInMetadataSpace` | `invalidOperation` |
| `invalidPath` | `invalidDirectoryAddress` |
| `layerMismatch(expected:actual:)` | `incompatibleStorageLayout` with `.unknownMarker(actual bytes)` |
| `incompatibleVersion`, `invalidVersion`, `invalidMetadata`, `directoryLayerNotInitialized` | `incompatibleStorageLayout` |
| `cannotCreatePartitionInPartition`, `cannotMoveAcrossPartitions` | `unsupportedOperation` |
| `FDBError` | `FDBStorageTransaction.convertFDBError` (existing transaction mapping) |

### Documented differences from `KeyValueDirectoryCatalog`

| Behavior | KV catalog | FDBStorage |
|---|---|---|
| Listing below a missing parent | empty result | `keyNotFound` (native `list` fails) |
| Layout rejection source | marker key state machine | layer type of the opened node |
| Root prefix | `Tuple(0)` under the catalog subspace | HCA-allocated native prefix |

Both stay within the Directory contract; the shared fixture passes on both.

## Runtime Flows

Operation 6 (`openOrCreateDirectory` / `openOrCreatePartition`):

```text
resolve tx (domain) -> require parent domain -> parent.address.appending(step)
  -> withDirectoryOperation(writes: true)
       -> layer.exists(parentPath)        false -> keyNotFound (FD-4)
       -> layer.createOrOpen(path, type)  type mismatch -> incompatibleStorageLayout (FD-2)
       -> Directory(address, HCA prefix)
```

Operation 8 (`removeChild`):

```text
resolve tx -> require parent domain -> child address
  -> withDirectoryOperation(writes: true)
       -> open node          missing -> keyNotFound
       -> verify type        mismatch -> incompatibleStorageLayout
       -> list children      non-empty -> directoryNotEmpty (FD-6)
       -> getRange(prefix, strinc(prefix), limit: 1)   row -> directoryNotEmpty (FD-6)
       -> registerIntent     leased subtree -> directoryLeased (FD-7)
       -> layer.remove(path)
```

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Database handle | `FDBStorageEngine.database` (`Mutex`) | from `init` until `requestShutdown` releases it; Directory operations retain it per call through `retainedDatabaseForDirectoryOperation` |
| `DirectoryLayer` instance | `FDBDirectoryAccess.withLayer` | one per operation; holds no state beyond its subspaces |
| Directory operation token | `FDBStorageTransaction` | the duration of one `withDirectoryOperation` call |
| Lease intents | `PartitionLeaseRegistry` (StorageKit) | released when the transaction completes (`executeTransaction` defer) |

## Failure, Concurrency, and Constraints

- Native conflicts and retryable client errors surface through the existing
  `FDBStorageTransaction` mapping; Directory operations add no retry of their own.
- Node keys stay below the FoundationDB 10 KB key bound: a Directory name is at
  most 255 bytes, a Partition identifier at most 1024 bytes (2048 hex characters).
- `withDirectoryOperation` serializes Directory operations with the transaction's
  own reads and writes; two engines on one cluster are isolated only by distinct
  root paths, not by the client.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| D-1…D-8, operations 1–8, L-1…L-3, L-7, L-8, FD-3…FD-9 | `Tests/FDBStorageTests/FDBDirectoryConformanceTests.swift` (shared `DirectoryConformanceCase` steps) |
| FD-2 (root, Directory, and Partition kind mismatch) | `FDBDirectoryConformanceTests.foreignLayerTypeIsRejected` |
| Layout V1 native names and identifier hex | `FDBDirectoryConformanceTests.nativeNamesCarryKindAndHexIdentifier` |
| Root path isolation and configuration validation | `FDBDirectoryConformanceTests.distinctRootPathsIsolateCatalogs`, `rootPathConfigurationIsValidated` |
| Transaction semantics, error mapping, byte ownership, footprint | `FDBStorageEngineTests`, `FDBStorageErrorMappingTests`, `FoundationDBByteOwnershipTests`, `FDBStorageTransactionFootprintTests`, `TransactionActivityDrainTests` |

The suite requires a reachable cluster through `FDB_CLUSTER_FILE` or the
default cluster file; it creates and removes its own paths below
`storage-kit-conformance`.

Changing the layer type strings, the name prefixes, the hex encoding, or the
root path default is a layout change: update this design, then the Directory
component design's adapter notes, then database-framework binding.
