# Directory

## Purpose and Scope

The Directory component owns storage placement: hierarchical Directories,
Partitions that root a nested Directory Layer over one contiguous keyspace, the
transactional catalog that is the sole existence authority in key-value
backends, the bootstrap state machine that decides whether a storage root is
initialized, and the noncopyable lease and bound-access types that confine
active work to one Partition.

| Field | Value |
|---|---|
| Design level | component |
| Parent | [StorageKit module](../DESIGN.md) |
| Children | none |
| Product authority | SPEC §8, §9, §10.3, §12.3, §24.1, §24.2 |

## Responsibilities and Boundaries

| Owns | Does not own |
|---|---|
| `DirectoryPath`, `LayerTag`, `StorageAddress`, `Directory`, `DirectoryEntry`, `Partition` values and their bounds | Reserved names (`system`, `database-framework`, `data`) and Framework Subspace layout |
| `DirectoryAccess` contract and the five semantic operations | Authorization of any operation |
| `KeyValueDirectoryCatalog`: the catalog realization for InMemory, SQLite, PostgreSQL, Cloudflare DO | The FDB realization (owned by `FDBStorage`, which must satisfy the same contract) |
| The InspectRoot → Open / Initialize / Reject bootstrap decision, read from the root's own allocation authority | Deleting or rewriting roots (never in production) |
| `PartitionLeaseRegistry`, `PartitionLease`, `BoundReadAccess`, `BoundWriteAccess` | Deciding which Partition a request may lease |

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [StorageKit module](../DESIGN.md) | parent | `TransactionReadAccess`, `TransactionAccess`, `StorageEngine.transactionDomain`, `StorageError`, `Subspace`, `Tuple` | Supplies the transaction and encoding contracts the catalog is built on. | Catalog keys use Tuple V1; a Tuple change is a layout change. |
| [FDBStorage module](../../FDBStorage/DESIGN.md) | coordinates with | `DirectoryAccess`, `PartitionLeaseRegistry.registerIntent` | Realizes the same node model over the native FoundationDB Directory Layer. | A Partition is a native node whose layer bytes are `partition`; native partitions nest, so no custom layer type is used. |
| [SQLiteStorage module](../../SQLiteStorage/DESIGN.md), [PostgreSQLStorage module](../../PostgreSQLStorage/DESIGN.md), [CloudflareDurableObjectStorage module](../../CloudflareDurableObjectStorage/DESIGN.md) | used by | `KeyValueDirectoryCatalog` | Each engine instantiates one catalog bound to its domain. | PostgreSQL rejects `readCommitted` isolation for catalog mutation through `MutationAdmission` (owned by PostgreSQLStorage). |
| database-framework | used by | every public type here | Binds `#Directory` declarations and the kernel to leases. | A Partition removal is recursive; the Framework no longer proves its Subspaces empty first. |

## Architecture

```text
 DirectoryAccess (protocol)
   |
   +-- KeyValueDirectoryCatalog (StorageKit)      +-- FDBDirectoryAccess (FDBStorage)
   |     per-layer node subspace 0xFE              |     native DirectoryLayer
   |     allocator / child edges                   |     native `partition` layer bytes
   v                                               v
 TransactionReadAccess / TransactionAccess (caller's transaction)

 StorageEngine.leasePartition
   -> PartitionLeaseRegistry.reserve            (domain, shutdown, intent check)
   -> DirectoryAccess.openDirectory(at:)        (generation check in caller txn)
   -> PartitionLease (~Copyable)
        -> withReadAccess  -> BoundReadAccess  (~Copyable, borrowed)
        -> withWriteAccess -> BoundWriteAccess (~Copyable, borrowed)
```

## Contracts and Invariants

### The node model

A node is a content prefix plus layer-tag bytes. A node whose tag is
`LayerTag.partition` roots a nested Directory Layer whose content base is the
node's own prefix, so every descendant node, Subspace, and key of that
Partition lies inside the Partition prefix range.

```text
layer with content base L
  node subspace   N = L ‖ FE
  allocator         = N ‖ 61                    Tuple(Int64(next))
  child edge        = N ‖ Tuple(parentPrefix, 0, name)
                        -> Tuple(childPrefix: bytes, layerTag: bytes)
  child content     = L ‖ Tuple(Int64(n)), n >= 1

domain root layer   L = empty, root Directory prefix = Tuple(Int64(0)) = 14
Partition P         nested layer with L = P
```

Prefixes are allocated per layer and are independent of the path, so a move is
a metadata-only edge rewrite. A plain Directory's children are allocated from
the layer that contains it, so a plain Directory subtree is not contiguous; a
Partition subtree is.

### Values

| Type | Definition | Bounds (`DirectoryLimits`) |
|---|---|---|
| `DirectoryPath` | ordered nonempty `[String]` of exact UTF-8 components; no normalization, no separator parsing | component 1…255 UTF-8 bytes; depth ≤ 64 |
| `LayerTag` | opaque tag bytes of a node; `.default` is empty, `.partition` is UTF-8 `partition` | 0…255 bytes |
| `StorageAddress` | ordered exact name components from the root; empty = root | depth ≤ 64 |
| `Directory` | `domain` (identity), `address`, `layer`, `keyspacePrefix`, package `layerRoot`, `root: Subspace`; generation = `keyspacePrefix` | — |
| `DirectoryEntry` | one listing row: `name`, `layer` | — |
| `Partition` | a `Directory` whose `layer.isPartition`; constructed only from such a node | — |

`Directory.root` is the Subspace a caller derives keys from:

| Node | `keyspacePrefix` | `root.prefix` |
|---|---|---|
| plain Directory | `C` | `C` |
| Partition | `P` | `P ‖ FD` |

A Partition's nested layer allocates children at `P ‖ Tuple(n)` and holds
metadata at `P ‖ FE`, so data written at the Partition root itself must not
share those bytes. `FD` is below the reserved `FE` and above every Tuple type
code (the largest is `0x33`), so the Partition data root is disjoint from every
allocated child prefix and from the nested node subspace, while staying inside
the Partition keyspace. FoundationDB avoids the same collision by forbidding
`pack()` on a partition subspace; SPEC §12.1 requires the final node to carry
the entity's own Subspaces, so StorageKit reserves the byte instead.

Value validation fails with `DirectoryAddressError`; `DirectoryAccess`
operations convert it to `StorageError.invalidDirectoryAddress`.

### `DirectoryAccess` — exactly these semantic operations

| # | Operation | Class | Input access | Absence | Notes |
|---|---|---|---|---|---|
| 1 | `open(_:expecting:in:transaction:)` | Read | `TransactionReadAccess` | returns `nil` | never creates; a non-`nil` expectation is verified against the stored tag → `directoryLayerMismatch` |
| 2 | `openOrCreate(_:layer:in:transaction:)` | Write | `TransactionAccess` | creates with `layer` | an existing node with a different tag fails `directoryLayerMismatch`; creation is atomic with the caller's transaction |
| 3 | `listChildren(in:after:limit:transaction:)` | Read | `TransactionReadAccess` | empty page | returns `DirectoryEntry` (name + tag); `limit` 1…1000, ordered by encoded key, `after` exclusive |
| 4 | `move(_:in:to:in:transaction:)` | Write | `TransactionAccess` | `keyNotFound` | whole-node rename, Partitions included; different containing layer: `partitionBoundaryViolation`; into own subtree: `invalidDirectoryAddress`; target exists: `invalidOperation`; leased subtree: `directoryLeased` |
| 5 | `remove(_:in:transaction:)` | Write | `TransactionAccess` | `keyNotFound` | atomic recursive removal of the node, its descendants, and their data; leased subtree: `directoryLeased`. There is no empty-only precondition |

`expecting: nil` performs no tag verification and matches the FoundationDB
empty-layer open. The stored tag is always read and returned on the resolved
`Directory`, so a caller that states an expectation gets it enforced and a
caller that does not still learns the tag. This is the documented reading of
SPEC §8.2.

One name namespace exists per parent: a name identifies exactly one node, and
its tag is a property of that node, not part of its identity.

The root is a Directory with the empty address and tag `.default`.
`openRoot(transaction:)` is operation 1 applied to the root (absence =
uninitialized empty store, returned as `nil`), and
`openOrInitializeRoot(transaction:)` is operation 2 applied to the root. Both
run the layout state machine below. `openPartition`, `openOrCreatePartition`,
`openDirectory(at path:)`, and `openDirectory(at address:)` are protocol
extensions over operations 1–2 and add no semantics.

Every operation checks `transaction.transactionDomain === parent.domain ===
catalog.domain`; a mismatch fails `storageDomainMismatch` before any I/O.

### Directory and Partition guarantees (D-1…D-12)

| ID | Guarantee | Enforcement |
|---|---|---|
| D-1 | Hierarchical naming with exact UTF-8 components | `DirectoryPath` / component validation |
| D-2 | Stable resolution: the same address resolves to the same `keyspacePrefix` until moved or removed | the child edge stores the content prefix |
| D-3 | Opaque root: callers cannot derive a prefix from a name | prefixes are per-layer allocator numbers, never name-derived |
| D-4 | Disjoint siblings: distinct children never share a prefix | Tuple-encoded `Int64` allocations are prefix-free |
| D-5 | Create, move, remove are atomic with the caller's transaction | all catalog writes go through the caller's `TransactionAccess` |
| D-6 | Read-only open never creates | read operations take `TransactionReadAccess` only |
| D-7 | Domain identity: values from one engine are rejected by another | `storageDomainMismatch` |
| D-8 | Bounded enumeration | `limit` 1…1000, else `invalidOperation` |
| D-9 | A stated layer expectation is verified on open | `directoryLayerMismatch` |
| D-10 | A Partition keyspace is contiguous: every descendant node, Subspace, and key lies in `[P, strinc(P))` | the nested layer allocates only inside `P` |
| D-11 | Partitions nest, and no node moves into or out of a Partition | the containing layer base of source and destination must be equal |
| D-12 | Removal is recursive and atomic; a Partition subtree clears as one range | `remove` clears descendants and data in the caller's transaction |

A resolver that reports every path as present is non-conforming; the fixture
proves absence for unknown children.

### `KeyValueDirectoryCatalog` layout (V1)

Reserved byte `0xFE` starts every layer's node subspace; `0xFD` starts a
Partition's data root. All content prefixes are Tuple-encoded `Int64` values,
whose type codes never reach `0xFD`.

| Key | Value | Meaning |
|---|---|---|
| `L ‖ FE 61` | `Tuple(Int64(next)).pack()` | next content number of the layer with base `L` (read-modify-write in the caller's transaction) |
| `L ‖ FE ‖ Tuple(parentPrefix: bytes, 0, name: String).pack()` | `Tuple(childPrefix: bytes, layerTag: bytes).pack()` | child edge of a node in the layer with base `L` |

- The domain root layer has `L` empty, so its node subspace is `FE` and its
  allocator key is `FE 61`. A child edge begins with a Tuple type code, which
  is never `0x61`, so edges never collide with the allocator.
- Root Directory = content number 0, prefix `Tuple(Int64(0)).pack()` = `14`.
  The root does not use the empty prefix, so root-level data can never collide
  with child roots or the catalog.
- Creating a Partition also initializes its nested allocator
  (`P ‖ FE 61` = `Tuple(Int64(1))`) in the same transaction, so a missing
  allocator always means corruption.
- Child listing is a bounded range read under
  `L ‖ FE ‖ Tuple(parentPrefix, 0).pack()`; names come from the key and tags
  from the value, so one range read answers operation 3.
- Move rewrites one edge: the old edge is cleared and the same content prefix
  and tag are written under the new parent and name. Descendant edges are keyed
  by the moved node's own prefix and are unaffected.
- Remove walks the node's edges within its layer. A Partition child clears as
  one range `[P, strinc(P))`; a plain child is walked, since its own children
  live in the containing layer. Every visited node also clears its data range
  and its edge.
- "Root is empty" (InspectRoot) is the first key at or after `[]` with no
  upper bound, so a key at or above `[0xFF]` counts as data exactly like any
  other key. A bounded probe would report a root that holds only such keys as
  empty and adopt a foreign layout, so the probe is deliberately unbounded.
  This probe, not any recorded version, is what keeps a Directory off data the
  catalog did not write.

Concurrent creation of the same child races on the layer allocator key; the
backend's conflict detection (FDB, InMemory, PostgreSQL repeatable-read or
serializable, SQLite serialization) makes one transaction fail typed. This is
why the allocator is a read-modify-write and not an atomic add, and why every
catalog write first passes the backend's `MutationAdmission`.

### Root bootstrap state machine (§10.3)

A storage root is initialized exactly when the authority that allocates
prefixes inside it holds state for that root. Nothing else records that fact:
a second witness can disagree with the first, and that disagreement is a state
no operation resolves without either fabricating a root or destroying data.

```text
KeyValueDirectoryCatalog                    -- witness: the root layer allocator `FE 61`
  allocator present                  -> Open      (root = content number 0)
  allocator absent, root empty       -> read:  nil
                                        write: Initialize (allocator = Tuple(1))
  allocator absent, root nonempty    -> Reject    incompatibleStorageLayout

FDBDirectoryAccess                          -- witness: the native node at the root path
  node exists                        -> Open      (requireRoot rejects a `partition` node)
  node absent                        -> read:  nil
                                        write: Initialize (createOrOpen at the root path)
```

No dual read/write of two layouts; production never deletes or rewrites a
root. A V0 deterministic-prefix key-value store is "allocator absent,
nonempty" and is rejected.

The two backends reject different things because they own different
allocation authorities, and the contract states each one rather than averaging
them:

- A key-value root shares one flat keyspace with whatever wrote to it first,
  and content prefixes start at `Tuple(1)`, so foreign data can occupy a
  prefix the catalog would later hand out. Emptiness is therefore a
  precondition of initialization, and the probe covers the whole root.
- FoundationDB allocates every prefix through the native Directory Layer,
  which never returns a prefix already in use, so no StorageKit write can land
  on foreign bytes. What the configured root path names is an operator
  decision; StorageKit verifies only that the node there is a plain Directory
  and not a native partition.

"Root" is one storage root, never a whole physical store. A backend whose
store is its own storage root answers emptiness for the store. A backend that
hosts several storage roots in one store, as FoundationDB does below a
configured root path, answers existence for that root's node alone, so a root
is never initialized, opened, or rejected because of another root's data.

### Leases (L-1…L-8)

| ID | Invariant | Enforcement |
|---|---|---|
| L-1 | Lease, transaction, and Partition domains match | `storageDomainMismatch` at issuance and at every bind |
| L-2 | A lease blocks a removal or move whose subtree intersects the leased subtree | registry check in operations 4 and 5 → `directoryLeased` |
| L-3 | A stale generation fails; work is never redirected | issuance re-resolves the address in the caller's transaction and compares `keyspacePrefix` → `staleLease` |
| L-4 | Read binding cannot mutate | `BoundReadAccess` has no mutation members |
| L-5 | Write binding cannot commit or cancel | `BoundWriteAccess` has no lifecycle members |
| L-6 | Bound access and cursors cannot escape the closure | noncopyable, borrowed; cursors validate the binding scope before every advance → `staleLease` |
| L-7 | Releasing the last lease is not success | release returns nothing; the caller's transaction outcome is the result |
| L-8 | Keys are confined to the Partition's content region | every key must lie in `[P, P ‖ FE)`; a range end may equal `P ‖ FE` only through `firstGreaterOrEqual`; `getKey` returns `nil` when the resolved key lies outside |

L-8 admits the Partition's content region `[P, P ‖ FE)`, not only the
Partition data root `P ‖ FD`, because Directories and Partitions inside a
Partition are not separately leasable and their Subspaces must be reachable
through the containing Partition's lease. Every content prefix below `P` starts
with a Tuple type code or `FD`, so one contiguous region covers all of them:
this is the operational meaning of contiguity in D-10.

The region stops below `P ‖ FE`, the Partition's own nested node subspace. That
metadata is owned by the catalog, not by the leaseholder; admitting it would
let a leaseholder that cleared its whole region delete the allocator of the
node its own lease is bound to and report `dataCorruption` on the next create.
Removing a Partition is still one range clear `[P, strinc(P))`, because the
catalog clears through the transaction, not through bound access.

Registry (per `StorageTransactionDomain`, in-process):

- `reserve(address)` runs before validation so a concurrent removal sees the
  lease; it is rejected after `requestShutdown()` (`resourceUnavailable`) and
  while a removal or move intent intersects the address (`staleLease`).
- Operations 4 and 5 register a subtree intent keyed by the caller's
  transaction object. The transaction releases its own intents when it commits
  or cancels, so an intent never outlives the mutation it protects; the
  registry additionally holds the transaction only weakly inside its `Mutex`,
  which reclaims the intents of a transaction that was abandoned without
  either outcome. The direction of error is always conservative (a lease is
  refused, never issued over a pending removal).
- Both guards compare subtrees, not one direction of ancestry. A lease covers
  the whole subtree under its Partition and a move or removal covers the whole
  subtree under its node, and two ancestor-closed sets intersect exactly when
  one root is an ancestor-or-self of the other, so `StorageAddress`
  `subtreeIntersects` is the single relation both use. Testing only one
  direction would admit removing a node beneath an active lease on one side
  and leasing a Partition that a pending removal is about to delete from
  beneath on the other.
- Cross-process: the registry cannot see other processes. There the invariant
  is L-3 (staleness at issuance), not prevention; every backend already
  behaves this way and the fixture proves it by removing and recreating a
  Partition between two resolutions.

## Runtime Flows

Lease issuance:

```text
leasePartition(partition, transaction)
  1. domain check (engine, partition.domain, transaction)        -> storageDomainMismatch
  2. registry.reserve(partition.root.address)                    -> resourceUnavailable | staleLease
  3. walk address from openRoot through open in `transaction`    -> nil, non-Partition, or prefix mismatch: release, staleLease
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

Recursive removal:

```text
remove(name, in: parent)
  edge = read(parent layer, parent.prefix, name)          absent -> keyNotFound
  admitMutation; registerIntent(address)                  leased -> directoryLeased
  pending = [edge.childPrefix, tag]
  while node = pending.pop
      for each child edge page under (node layer, node.prefix)
          partition child -> clear [childPrefix, strinc(childPrefix)); clear edge
          plain child     -> push; clear edge
      clear [node.prefix, strinc(node.prefix))
  clear edge
```

A Partition child needs no walk: one range covers its whole subtree.

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Catalog edges and per-layer allocators | backend keyspace, mutated only through the caller's transaction | durable |
| Lease registrations and intents | `PartitionLeaseRegistry` (`Mutex`) | registration: until `release()` or `PartitionLease` deinit; intent: until owner completion or transaction deallocation |
| Binding scope | `PartitionLease.withReadAccess` / `withWriteAccess` | one closure |

## Failure, Concurrency, and Constraints

`StorageError.Code` cases owned here: `incompatibleStorageLayout`,
`directoryLayerMismatch`, `partitionBoundaryViolation`, `directoryLeased`,
`storageDomainMismatch`, `staleLease`, `invalidDirectoryAddress`. All have
retry disposition `never`.

`DirectoryLimits` (owned operational contract):

| Limit | Value | Rationale |
|---|---|---|
| `maximumComponentByteCount` | 255 | matches common filesystem name bounds; keeps edge keys far below backend key limits |
| `maximumDepth` | 64 | bounds address walks, lease validation, and removal recursion |
| `maximumLayerTagByteCount` | 255 | tag stays in the edge value and below every backend value bound |
| `maximumListLimit` | 1000 | one bounded page per range read |

Changing a limit requires re-running the shared fixture on every adapter and
confirming edge keys stay within each backend's key bound.

## Verification and Change Impact

| Contract | Evidence |
|---|---|
| Values and bounds | `Tests/StorageKitTests/DirectoryValueTests.swift` |
| D-1…D-12, state machine, operations 1–5, L-1…L-3, L-7, L-8 | `StorageKitConformance` `DirectoryConformanceCase` run by `InMemoryDirectoryConformanceTests`, `SQLiteDirectoryConformanceTests`, `PostgreSQLDirectoryConformanceTests`, `CloudflareDurableObjectDirectoryConformanceTests`, and `FDBDirectoryConformanceTests`. `verifyForeignRootRejection` is key-value only: FoundationDB has no such state, and `FDBDirectoryConformanceTests.siblingRootsAreIndependent` carries the per-root isolation it used to prove |
| L-2 subtree intersection in both directions, intent release at commit and cancel | `DirectoryConformanceCase.verifyLeaseSubtreeExclusion` run by every adapter suite |
| L-4, L-5 (compile-level), L-6 escape, registry intersection relation | `Tests/StorageKitTests/PartitionLeaseTests.swift` |

Changing the catalog layout, the bootstrap witness of any backend, or any
operation semantics requires updating this design first, then the StorageKit
module design, then every adapter module, then database-framework binding
(F-15 in `PROGRESS.md`).
