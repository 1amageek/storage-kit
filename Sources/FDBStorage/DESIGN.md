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
| `FDBStorageTransaction`: read, write, range, atomic, commit-at-most-once, cancel, error conversion, and the exclusive Directory operation window | Directory contract semantics D-1…D-12 and lease semantics L-1…L-8 ([Directory component](../StorageKit/Directory/DESIGN.md)) |
| Borrowed result byte lifetime bound to the retained owner | Native Directory Layer algorithms: HCA prefix allocation, node metadata, nested partition layers, version keys |
| `FDBDirectoryLayout`: native path composition below the root path and the one-to-one `LayerTag` ↔ `DirectoryType` mapping | `PartitionLease` (StorageKit) |
| `FDBDirectoryAccess`: the five semantic Directory operations plus root open and initialize, layer-tag verification on every open, Partition boundary and own-subtree rejection, listing order and pagination, native error mapping | Framework binding of `#Directory` declarations |

Authority: the native Directory Layer metadata below `Configuration.rootPath`
is the sole existence authority for this backend (SPEC §12.3). A node's
existence, its HCA-allocated prefix, and its layer tag are read only from that
metadata; StorageKit stores no shadow catalog, and this adapter owns no key
of its own below `rootPath`.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [storage-kit package](../../DESIGN.md) | parent | package invariants P-1…P-7 | Module graph and package-wide invariants. | Public contract changes propagate to database-framework. |
| [StorageKit module](../StorageKit/DESIGN.md) | depends on | `StorageEngine`, `TransactionReadAccess`, `TransactionAccess`, `StorageError`, `Subspace`, `StorageTransactionDomain` | Supplies the contracts this adapter realizes. | `Subspace` here is `StorageKit.Subspace`; the bindings own a different `Subspace` type. |
| [Directory component](../StorageKit/Directory/DESIGN.md) | depends on | `DirectoryAccess`, `Directory`, `DirectoryEntry`, `LayerTag`, `Partition`, `StorageAddress`, `DirectoryLimits` | Defines the operations, failure codes, bounds, and root bootstrap state machine this module must satisfy. | Removal and move have no lease precondition; a lease detects a superseded Partition when it is bound (L-2). |
| [StorageKitConformance](../StorageKitConformance/DESIGN.md) | used by | `DirectoryConformanceCase` | Shared fixture executed by `FDBDirectoryConformanceTests`. | Every step runs here except `verifyForeignRootRejection`, which has no FoundationDB state to produce: the native layer never allocates a prefix already in use, so a StorageKit write cannot land on foreign bytes. |
| `fdb-swift-bindings` | depends on | `FDBClient`, `DatabaseProtocol`, `TransactionProtocol`, `DirectoryLayer`, `DirectorySubspace`, `DirectoryType`, `DirectoryError` | Native client, transactions, and Directory Layer. | `createOrOpen` creates missing ancestors as untyped nodes, so this adapter checks the parent itself (FD-10), and writes the layer version key when absent; `list` returns names in Swift `String` order and paginates nothing; `move` keeps the prefix; `remove` is recursive; a `partition` node roots a nested layer and partitions may nest. |

## Architecture

```text
FDBStorageEngine
  ├─ database: Mutex<(any DatabaseProtocol)?>      client startup, shutdown release
  ├─ transactionDomain: StorageTransactionDomain   identity + lease issuance gate
  ├─ directoryAccess: FDBDirectoryAccess ──uses──> FDBDirectoryLayout
  │        │                                          (rootPath + address, LayerTag <-> DirectoryType)
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

Root state is scoped by `rootPath`: the node at that path is the whole of
this root's bootstrap state (FD-1). Two engines with distinct root paths
therefore never observe each other's root, and neither is initialized, opened,
or rejected because of the other's data.

### Layout

The mapping is one to one and adds no encoding of its own.

| StorageKit node | Native path | Native layer type |
|---|---|---|
| root | `rootPath` | none (plain node) |
| child named `name`, `LayerTag.default` | parent path + `name` | none (plain node) |
| child named `name`, `LayerTag.partition` | parent path + `name` | `.partition` |
| child named `name`, other tag `t` | parent path + `name` | `.custom(t)` |

- `Directory.keyspacePrefix` is the native HCA-allocated prefix of the node.
  `Directory.root.prefix` is that prefix for a plain Directory and
  `keyspacePrefix ‖ FD` for a Partition, as the Directory component defines.
- A Partition is a native `partition` node, so it roots a nested Directory
  Layer whose node subspace and content subspace lie inside the Partition
  prefix. Every descendant node, Subspace, and key of a Partition is therefore
  allocated inside `[P, strinc(P))` (D-10), and partitions nest (D-11). No
  custom layer type of the adapter's own exists.
- A tag other than the empty tag and `partition` is application-opaque
  (SPEC §4), so it is carried as native layer bytes with no interpretation and
  read back exactly, valid UTF-8 or not.

### Invariants

| ID | Invariant |
|---|---|
| FD-1 | The native layer below `rootPath` is the sole existence authority, and this adapter owns no key of its own. The root is initialized exactly when the node at `rootPath` exists: `openRoot` reports its absence as an uninitialized root and never writes, and `openOrInitializeRoot` creates it in the caller's transaction. Existence is asked of that node and never of the cluster, so another root's nodes and the native allocator counters are not this root's data. A node found at `rootPath` is opened rather than adjudicated: the native layer never allocates a prefix already in use, so no StorageKit write can land on bytes written outside it, and which node `rootPath` names is an operator decision. `requireRoot` still refuses a node whose layer is not the default, so a native partition at the root path fails with `directoryLayerMismatch`. |
| FD-2 | Every open of a node (root, child, listing row, move source) reads the node's stored layer tag and returns it on the resolved `Directory`; a stated expectation that differs fails with `directoryLayerMismatch` and the node is never adopted. `expecting: nil` verifies nothing, matching the native empty-layer open. |
| FD-3 | Read operations never write: `openRoot` checks `exists` before `open`, so an uninitialized root is observed without touching the layer version key. |
| FD-4 | A move never resurrects a stale destination: a missing destination Directory fails with `keyNotFound` instead of being created as an untyped native node, and an occupied target name fails with `invalidOperation`. Both are checked before the native mutation. |
| FD-5 | Listings sort native names by UTF-8 bytes, apply `after` exclusively, and honor `limit` in `1...DirectoryLimits.maximumListLimit`; each row resolves the child to read its stored tag, so a `DirectoryEntry` carries the node's real layer. A child that disappears between `list` and its resolution is skipped, and a listing below a stale parent is an empty page. |
| FD-6 | Removal is recursive and has no emptiness precondition: the native layer clears every descendant node and the whole content range of the removed node, which for a Partition is one range. A missing node fails with `keyNotFound` before any mutation. |
| FD-8 | A node moves only within the Directory Layer that contains it, Partitions included: source and destination must share the same containing content base, otherwise `partitionBoundaryViolation`; a target inside the moved subtree fails with `invalidDirectoryAddress(.targetInsideMovedSubtree)`. |
| FD-9 | Every Directory operation of a transaction runs inside `withDirectoryOperation`, which rejects a transaction of another engine (`storageDomainMismatch`), enforces exclusivity with the transaction's own access, and marks `openOrInitializeRoot`, `openOrCreate`, `move`, and `remove` as mutations. |
| FD-10 | A create never fabricates the parent chain. `openOrCreate` resolves the child first and, when it is absent, requires the parent node to exist before creating it, so a create below a removed parent fails with `keyNotFound` instead of having its ancestors recreated as untyped nodes. Nothing other than the named child is ever created. |

### Native error mapping

| `DirectoryError` | `StorageError.Code` |
|---|---|
| `directoryNotFound` | `keyNotFound` |
| `directoryAlreadyExists`, `prefixInUse`, `prefixInMetadataSpace` | `invalidOperation` |
| `invalidPath` | `invalidDirectoryAddress` |
| `layerMismatch(expected:actual:)` | `directoryLayerMismatch` |
| `incompatibleVersion`, `invalidVersion`, `invalidMetadata`, `directoryLayerNotInitialized` | `incompatibleStorageLayout` |
| `cannotMoveAcrossPartitions` | `partitionBoundaryViolation` |
| `cannotCreatePartitionInPartition` | `invalidOperation`; nested Partition creation is permitted, so the native layer no longer raises this case and the mapping is defensive |
| `FDBError` | `FDBStorageTransaction.convertFDBError` (existing transaction mapping) |

### Documented differences from `KeyValueDirectoryCatalog`

| Behavior | KV catalog | FDBStorage |
|---|---|---|
| Bootstrap witness | the root layer's allocator key, plus an unbounded emptiness probe that rejects foreign data | the native node at `rootPath`; foreign data cannot collide, because the native layer never allocates a prefix in use, so there is nothing to reject |
| Root prefix | `Tuple(0)` under the domain root layer | HCA-allocated native prefix below `rootPath` |
| Foreign nodes | cannot exist; the catalog owns every edge | a native node created outside StorageKit is listed and resolvable, and carries its own layer tag |

Writing below a removed parent is no longer a difference. FD-10 and FD-4 state
for the native layer what D-13 states for the contract, and the key-value
catalog reaches the same outcome by re-resolving the address in the caller's
transaction, so both backends fail `keyNotFound` before any write. The shared
fixture asserts it on both through `verifyStaleParentRejection`.

## Runtime Flows

Operation 2 (`openOrCreate`, and `openOrCreateDirectory` / `openOrCreatePartition`):

```text
resolve tx (domain) -> require parent domain -> parent.address.appending(name)
  -> LayerTag -> DirectoryType?          non-UTF-8 tag -> invalidDirectoryAddress
  -> withDirectoryOperation(writes: true)
       -> layer.open(path)               present -> requireLayer -> Directory
                                         absent  -> continue below
       -> layer.exists(parent path)      absent  -> keyNotFound (FD-10)
       -> layer.createOrOpen(path, type) stored tag differs -> layerMismatch
       -> Directory(address, HCA prefix, stored tag, parent.childLayerRoot)
       -> requireLayer(stored, expected: tag)  -> directoryLayerMismatch (FD-2)
```

One native call opens or creates the node. Resolving the path first to inspect
an existing node would descend it twice on every create, and the tag is
verified either way: the native layer rejects a stored tag that differs from a
stated type, and states no type for the default tag, which the explicit check
then covers.

Operation 4 (`move`):

```text
resolve tx -> require source and destination domains -> both child addresses
  -> compare containing layer bases      differ -> partitionBoundaryViolation (FD-8)
  -> withDirectoryOperation(writes: true)
       -> open source node               missing -> keyNotFound
       -> target inside moved subtree    -> invalidDirectoryAddress (FD-8)
       -> destination exists             absent  -> keyNotFound (FD-4)
       -> new path free                  taken   -> invalidOperation (FD-4)
       -> layer.move(oldPath, newPath)   prefix and subtree preserved
```

Operation 5 (`remove`):

```text
resolve tx -> require parent domain -> child address
  -> withDirectoryOperation(writes: true)
       -> layer.exists(path)     false -> keyNotFound
       -> layer.remove(path)     recursive; a Partition clears as one range (FD-6)
```

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Database handle | `FDBStorageEngine.database` (`Mutex`) | from `init` until `requestShutdown` releases it; Directory operations retain it per call through `retainedDatabaseForDirectoryOperation` |
| `DirectoryLayer` instance | `FDBDirectoryAccess.withLayer` | one per operation; holds no state beyond its subspaces |
| Directory operation token | `FDBStorageTransaction` | the duration of one `withDirectoryOperation` call |

## Failure, Concurrency, and Constraints

- Native conflicts and retryable client errors surface through the existing
  `FDBStorageTransaction` mapping; Directory operations add no retry of their own.
- Node keys stay below the FoundationDB 10 KB key bound: a name is at most 255
  UTF-8 bytes, a layer tag at most 255 bytes, and the address depth at most 64.
- A listing resolves one node per returned row, so a page costs `limit`
  descents in addition to the parent's own. `DirectoryLimits.maximumListLimit`
  bounds that cost within the transaction's five-second budget.
- `withDirectoryOperation` serializes Directory operations with the transaction's
  own reads and writes; two engines on one cluster are isolated only by distinct
  root paths, not by the client, and each root path carries its own root node
  (FD-1).

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| D-1…D-12, operations 1–5, the root bootstrap state machine, L-1…L-3, L-7, L-8, FD-1, FD-3…FD-9 | `Tests/FDBStorageTests/FDBDirectoryConformanceTests.swift` (shared `DirectoryConformanceCase` steps) |
| Layout names and layer values, nested Partition creation and containment | `FDBDirectoryConformanceTests.nativeNodesCarryStorageKitNamesAndLayers` |
| FD-2 on foreign native nodes: typed root, child, and Partition mismatch, and the tag returned by a listing | `FDBDirectoryConformanceTests.foreignLayerValueIsRejected` |
| A layer tag that is not valid UTF-8 stays application-opaque | `FDBDirectoryConformanceTests.layerTagThatIsNotUTF8RoundTrips` |
| Root path isolation and configuration validation | `FDBDirectoryConformanceTests.distinctRootPathsIsolateCatalogs`, `rootPathConfigurationIsValidated` |
| FD-1 root scoping: a fresh root initializes beside occupied roots, and one root's state never decides another's | `FDBDirectoryConformanceTests.rootInitializesOnANonemptyCluster`, `siblingRootsAreIndependent` |
| Transaction semantics, error mapping, byte ownership, footprint | `FDBStorageEngineTests`, `FDBStorageErrorMappingTests`, `FoundationDBByteOwnershipTests`, `FDBStorageTransactionFootprintTests`, `TransactionActivityDrainTests` |

The suite requires a reachable cluster through `FDB_CLUSTER_FILE` or the
default cluster file; it creates and removes its own paths below
`storage-kit-conformance` after each step.

Changing the path mapping, the layer-tag mapping, the bootstrap witness, or
the root path default is a layout change: update this design, then the Directory
component design's adapter notes, then database-framework binding.
