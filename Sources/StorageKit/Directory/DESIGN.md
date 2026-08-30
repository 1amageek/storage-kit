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
| Product authority | SPEC §8, §9, §12.3, §24.1, §24.2 |

## Responsibilities and Boundaries

| Owns | Does not own |
|---|---|
| `LayerTag`, `StorageAddress`, `Directory`, `DirectoryEntry`, `Partition` values and their bounds | Reserved names (`system`, `database-framework`, `data`) and Framework Subspace layout |
| `DirectoryAccess` contract and the five semantic operations | Authorization of any operation |
| `KeyValueDirectoryCatalog`: the catalog realization for InMemory, SQLite, PostgreSQL, Cloudflare DO | The FDB realization (owned by `FDBStorage`, which must satisfy the same contract) |
| The InspectRoot → Open / Initialize / Reject bootstrap decision, read from the root's own allocation authority | Deleting or rewriting roots (never in production) |
| `PartitionLease`, `BoundReadAccess`, `BoundWriteAccess` | Deciding which Partition a request may lease, and whether a removal that would invalidate one is admissible |

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [StorageKit module](../DESIGN.md) | parent | `TransactionReadAccess`, `TransactionAccess`, `StorageEngine.transactionDomain`, `StorageError`, `Subspace`, `Tuple` | Supplies the transaction and encoding contracts the catalog is built on. | Catalog keys use the Tuple encoding; a Tuple change is a layout change. |
| [FDBStorage module](../../FDBStorage/DESIGN.md) | coordinates with | `DirectoryAccess` | Realizes the same node model over the native FoundationDB Directory Layer. | A Partition is a native node whose layer bytes are `partition`; native partitions nest, so no custom layer type is used. |
| [SQLiteStorage module](../../SQLiteStorage/DESIGN.md), [PostgreSQLStorage module](../../PostgreSQLStorage/DESIGN.md), [CloudflareDurableObjectStorage module](../../CloudflareDurableObjectStorage/DESIGN.md) | used by | `KeyValueDirectoryCatalog` | Each engine instantiates one catalog bound to its domain. | PostgreSQL admits catalog mutation and a Partition write binding only under `serializable`, and a Partition read binding only under an isolation level that holds a stable snapshot, through `admit` backed by `OperationAdmission` (the rule is owned by PostgreSQLStorage). |
| database-framework | used by | every public type here | Binds `#Directory` declarations and the kernel to leases. | A Partition removal is recursive and unconditional here; the Framework decides admissibility, including whether one of its own leases is active (SPEC §12.3), before it calls. |

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
   -> domain and shutdown check
   -> DirectoryAccess.openDirectory(at:)        (prefix check in caller txn)
   -> PartitionLease (~Copyable)
        -> withReadAccess  -> openDirectory(at:) in the binding txn
                           -> BoundReadAccess  (~Copyable, borrowed)
        -> withWriteAccess -> openDirectory(at:) in the binding txn
                           -> BoundWriteAccess (~Copyable, borrowed)
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
| `LayerTag` | opaque tag bytes of a node; `.default` is empty, `.partition` is UTF-8 `partition` | 0…255 bytes |
| `StorageAddress` | the logical path value: ordered exact UTF-8 name components from the root, empty = root; no normalization, no separator parsing; equality and hashing are decided on the component UTF-8 bytes, not by `String` comparison | component 1…255 UTF-8 bytes; depth ≤ 64 |
| `Directory` | `domain` (identity), `address`, `layer`, `keyspacePrefix`, package `layerRoot`, `root: Subspace` | — |
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
| 2 | `openOrCreate(_:layer:in:transaction:)` | Write | `TransactionAccess` | creates with `layer` | an existing node with a different tag fails `directoryLayerMismatch`; creation is atomic with the caller's transaction; a parent that is absent when the write runs fails `keyNotFound` (D-13) |
| 3 | `listChildren(in:after:limit:transaction:)` | Read | `TransactionReadAccess` | empty page | returns `DirectoryEntry` (name + tag); `limit` 1…1000, ordered by encoded key, `after` exclusive; the bound is applied by the backend read, never by truncating a materialized listing |
| 4 | `move(_:in:to:in:transaction:)` | Write | `TransactionAccess` | `keyNotFound` | whole-node rename, Partitions included; a destination parent that is absent when the write runs fails `keyNotFound` (D-13); different containing layer: `partitionBoundaryViolation`; into own subtree: `invalidDirectoryAddress`; target exists: `invalidOperation` |
| 5 | `remove(_:in:transaction:)` | Write | `TransactionAccess` | `keyNotFound` | atomic recursive removal of the node, its descendants, and their data. There is no empty-only precondition, and no lease precondition |

`admit(_:)` is not one of these operations. It is the synchronous gate a
backend uses to refuse an operation its configured transaction semantics
cannot carry. A catalog write reaches it once its resolution reads are done
and before it writes anything, so a refused mutation leaves the store
untouched; an access binding reaches it before any I/O at all, so a refused
binding spends no round trip. It has two callers — every catalog write
(operations 2, 4, 5 and `openOrInitializeRoot`) and every Partition access
binding, read or write — and one default: admit. It is a protocol requirement
rather than an extension so a backend's rule is reached through the witness
table from both callers, including `PartitionLease`, which holds only
`any DirectoryAccess`.

The gate takes the operation because the two callers need different
guarantees, and a backend may be able to give one and not the other. The
catalog operations reaching it are all mutations; a binding reaches it with
`.read` or `.write`. Catalog reads (operations 1, 3 and `openRoot`) and lease
issuance never reach it: each is a single resolution that promises nothing
beyond itself, so a backend that cannot admit a binding can still open, list,
and lease.

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
| D-1 | Hierarchical naming with exact UTF-8 components | `StorageAddress.validateComponent` |
| D-1a | One component is one storage identity: addresses that differ in component bytes are never equal, and equal addresses always encode to equal keys | `StorageAddress` equality and hashing run over the UTF-8 bytes |
| D-2 | Stable resolution: the same address resolves to the same `keyspacePrefix` until moved or removed | the child edge stores the content prefix |
| D-3 | Opaque root: callers cannot derive a prefix from a name | prefixes are per-layer allocator numbers, never name-derived |
| D-4 | Disjoint siblings: distinct children never share a prefix | Tuple-encoded `Int64` allocations are prefix-free |
| D-5 | Create, move, remove are atomic with the caller's transaction | all catalog writes go through the caller's `TransactionAccess` |
| D-6 | Read-only open never creates | read operations take `TransactionReadAccess` only |
| D-7 | Domain identity: values from one engine are rejected by another | `storageDomainMismatch` |
| D-8 | Bounded enumeration | `limit` 1…1000, else `invalidOperation`; the page is produced by a bounded backend read in key order, so cost and order both come from the store |
| D-9 | A stated layer expectation is verified on open | `directoryLayerMismatch` |
| D-10 | A Partition keyspace is contiguous: every descendant node, Subspace, and key lies in `[P, strinc(P))` | the nested layer allocates only inside `P` |
| D-11 | Partitions nest, and no node moves into or out of a Partition | the containing layer base of source and destination must be equal |
| D-12 | Removal is recursive and atomic; a Partition subtree clears as one range | `remove` clears descendants and data in the caller's transaction |
| D-13 | A write positions a node by what the named address resolves to now, never by a prefix the caller already holds, and reports it in the physical context that resolution produced | operation 2 re-resolves the parent and operation 4 both of its endpoints, in the caller's transaction, before any write → `keyNotFound`; the returned `Directory` carries the live node's `keyspacePrefix` and containing layer base |

A resolver that reports every path as present is non-conforming; the fixture
proves absence for unknown children.

D-13 is what makes D-2 safe to hold a value across statements. A `Directory`
carries the `keyspacePrefix` of the resolution that produced it, so a handle
outlives the node it names; using that prefix as a write position would place
a node under a parent that no longer exists, reachable by no path and covered
by no removal.

Only operations 2 and 4 re-resolve, because only they place a key under a
parent. When the parent was removed and not recreated, the others report the
same absence from what remains: operation 1 finds no child, operation 3
returns an empty page, and operation 5 finds nothing to clear and fails
`keyNotFound`. Operation 4 re-resolves its source as well as its destination
so that one operation reads both endpoints through the same authority; its
source would otherwise be positioned by a prefix while its destination is
positioned by an address.

Re-resolution decides the returned value as well as the write position. A
`Directory` returned by operation 2 or 4 carries the `keyspacePrefix` and the
containing layer base of the node the address resolved to in that transaction,
never the ones copied from the caller's handle. A value that paired a live
prefix with a superseded layer base would name a node in one Partition while
reporting the boundary of another, and the next operation 4 wholly inside the
live Partition would read that base and reject itself as a boundary
violation.

A parent that was removed and recreated at the same address resolves to the
live node, so a write through a handle from before the recreation lands in the
new node rather than failing. That is the consequence of positioning by
address: D-13 makes absence observable, not identity. Making a superseded
resolution fail is the stale-generation rule of SPEC §9.3, which the lease
layer owns: a `PartitionLease` re-resolves its own Partition in every
transaction it is bound to (L-2), so a superseded Partition fails there
instead of being written through here.

Operations 1, 3, and 5 do not re-resolve, so after a recreation at the same
address they report the backend's own positioning: FoundationDB reads the live
successor, and the key-value catalogs read from the prefix the caller holds
and find nothing. Both satisfy D-13, which constrains where a write lands and
what a write reports, not what a superseded handle observes on a read. A
caller that needs one answer resolves the address again, or holds a
`PartitionLease` and receives the SPEC §9.3 rejection.

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

Operations 2 and 4 resolve the named parent by walking child edges from the
root inside the caller's transaction, at most `DirectoryLimits.maximumDepth`
point reads, and take the write position from that resolution. The walk is not
only a precondition: its edge reads enter the caller's read set, so a removal
that commits concurrently conflicts through the same backend detection that
protects the allocator, and the check cannot be observed as passing against a
parent that is already gone. FoundationDB obtains the identical property by
resolving the native path, which is why the guarantee is backend-neutral.

Concurrent creation of the same child races on the layer allocator key; the
backend's conflict detection (FDB, InMemory, PostgreSQL repeatable-read or
serializable, SQLite serialization) makes one transaction fail typed. This is
why the allocator is a read-modify-write and not an atomic add, and why every
catalog write first passes the backend's `admit`.

A Partition access binding passes the same gate, because a binding promises
the leased generation for the whole span of its closure rather than for the
instant of its generation walk.

A write binding needs the same conflict a catalog write needs. The generation
walk is the same read-then-write shape: the binding reads the Partition's
node, the closure writes keys inside its prefix, and a concurrent removal of
that Partition writes neither of those rows. A backend that does not turn that
read-write dependency into a conflict admits a write into a Partition that no
longer exists.

A read binding needs the weaker property that the walk's observation stays
true for the closure. On a backend whose reads each take a fresh snapshot, the
Partition can be removed after the walk and the closure's reads then return
nothing — a removed Partition reported as an empty one, which is the synthetic
success Layer 0 forbids. Such a backend rejects the read binding too, so the
gate is reached on both paths and the operation tells the backend which
guarantee is being asked for.

### Root bootstrap state machine (§8.7)

A storage root is initialized exactly when the authority that allocates
prefixes inside it holds state for that root. Nothing else records that fact:
a second witness can disagree with the first, and that disagreement is a state
no operation resolves without either fabricating a root or destroying data.

```text
KeyValueDirectoryCatalog                    -- witness: the root layer allocator `FE 61`
  allocator holds a next number      -> Open      (root = content number 0)
  allocator holds anything else      -> Reject    incompatibleStorageLayout
  allocator absent, root empty       -> read:  nil
                                        write: Initialize (allocator = Tuple(1))
  allocator absent, root nonempty    -> Reject    incompatibleStorageLayout

FDBDirectoryAccess                          -- witness: the root record inside the root node's content prefix
  node exists, record present        -> Open      (requireRoot rejects a typed node)
  node exists, record absent         -> Reject    incompatibleStorageLayout
  node exists, record foreign        -> Reject    incompatibleStorageLayout
  node absent, an ancestor is a root -> Reject    incompatibleStorageLayout
  node absent                        -> read:  nil
                                        write: Initialize (createOrOpen, then the record)
```

No dual read/write of two layouts; production never deletes or rewrites a
root. A key-value store holding deterministic path-derived prefixes is
"allocator absent, nonempty" and is rejected.

The two backends reject different things because they own different
allocation authorities, and the contract states each one rather than averaging
them:

- A key-value root shares one flat keyspace with whatever wrote to it first,
  and content prefixes start at `Tuple(1)`, so foreign data can occupy a
  prefix the catalog would later hand out. Emptiness is therefore a
  precondition of initialization, and the probe covers the whole root.
  A foreign key can also occupy the allocator key itself, and presence of that
  key would then adopt the root and skip the emptiness probe. The witness is
  therefore the value this catalog writes: the allocator is read as a packed
  `Tuple` integer and rejected unless it is a next number in
  `[1, Int64.max)`. Bytes that decode to a plausible next number stay
  indistinguishable from this catalog's own allocator; separating them would
  need a second witness, which is the disagreement no operation resolves, so
  that residue is stated rather than closed.
- FoundationDB allocates every prefix through the native Directory Layer,
  which never returns a prefix already in use, so the root node's content
  prefix belongs to this catalog alone and no StorageKit write can land on
  foreign bytes. Existence of the node is still not the witness: the native
  layer creates the ancestors of a path as ordinary untyped Directories, so
  opening a root at `["a", "b"]` brings `["a"]` into existence as a side
  effect. The witness is therefore the record this catalog writes inside that
  content prefix, which only initialization writes and which implicit ancestor
  creation never sets. A raw Layer 0 transaction can still write anywhere, so
  the record is adjudicated by the value it holds rather than by its presence.

The root record lives inside the root node's own content prefix, above every
Tuple type code, so no `StorageAddress` resolves onto it. It is not a
`LayerTag` a caller observes, and no layer value is reserved: a caller tag
round-trips on FoundationDB exactly as it does on a key-value backend
(SPEC §4). `requireRoot` verifies the record and returns the root `Directory`
with `.default`, so both backend classes expose the same root value and no
caller learns the record.

Storage roots do not nest, and the record closes both orders of creation:

| Created first | Created second | Outcome for the second |
|---|---|---|
| `["a", "b"]` | `["a"]` | the node exists carrying no root record → `incompatibleStorageLayout` |
| `["a"]` | `["a", "b"]` | a proper ancestor carries a root record → `incompatibleStorageLayout` |

The ancestor check reads one node per proper ancestor of the configured root
path, in the caller's transaction. The default root path has no proper
ancestor, so the check costs nothing there. Without it, the outer root would
list the inner root among its own children and `remove` would destroy it
recursively — a whole storage root deleted through an operation that names one
child.

"Root" is one storage root, never a whole physical store. A backend whose
store is its own storage root answers emptiness for the store. A backend that
hosts several storage roots in one store, as FoundationDB does below a
configured root path, answers existence for that root's node alone, so a root
is never initialized, opened, or rejected because of another root's data.

### Leases (L-1…L-8)

| ID | Invariant | Enforcement |
|---|---|---|
| L-1 | Lease, transaction, and Partition domains match | `storageDomainMismatch` at issuance and at every bind |
| L-2 | A lease is validated against the store at issuance and again at every access binding | the address walk runs in the issuing or binding transaction, so the resolution enters that transaction's read set |
| L-3 | A stale generation fails; work is never redirected | an absent node, a non-Partition layer, or a different `keyspacePrefix` → `staleLease` |
| L-4 | Read binding cannot mutate | `BoundReadAccess` has no mutation members |
| L-5 | Write binding cannot commit or cancel | `BoundWriteAccess` has no lifecycle members |
| L-6 | Bound access and cursors cannot escape the closure | noncopyable, borrowed; cursors validate the binding scope on both sides of every advance → `staleLease`, so an advance already in flight when the binding closes does not deliver its row either, and the binding scope owns every cursor it issued and completes that cursor's backend cleanup as part of closing, so no capability of an escaped cursor — cleanup included — is still outstanding when the binding closes |
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

Staleness is the only lease mechanism, and the store is its only authority:

- A lease does not block operation 4 or 5. Removal and move are unconditional
  here; whether removing a Partition that something is using is admissible is
  a database-operation decision the Framework makes before it calls
  (SPEC §12.3).
- The prefix allocator never reuses a number, so a Partition removed and
  recreated at the same address always carries a different `keyspacePrefix`.
  Comparing the prefix is therefore a complete staleness test: it needs no
  record of who else is running, and it gives the same answer in every process
  that reads the same store.
- Issuance and every bind perform the same walk in the transaction that will
  use the lease. Concurrency is then the backend's: the resolution is in that
  transaction's read set, so a removal committing concurrently makes the
  transaction conflict rather than silently succeed.
- The mechanism therefore holds across engine instances and processes, which a
  process-local registry could not do: a second engine over the same store
  invalidates a lease the first engine issued, without the first engine
  observing anything in memory.
- A bind costs one address walk — the root read plus one read per address
  component — so a caller holds a binding for the span of the work it covers
  rather than opening one per key. The walk is not deduplicated against earlier
  reads in the same transaction: what it shares with them is the read set, not
  the round trip.

## Runtime Flows

Lease issuance:

```text
leasePartition(partition, transaction)
  1. domain check (engine, partition.domain, transaction)        -> storageDomainMismatch
  2. shutdown check                                              -> resourceUnavailable
  3. walk address from openRoot through open in `transaction`    -> nil, non-Partition, or prefix mismatch: staleLease
  4. return PartitionLease(partition, directoryAccess)
```

Bound read:

```text
lease.withReadAccess(transaction) { access in ... }
  - domain check, registration active check               -> local, no I/O
  - admit(.read)                                          -> unsupportedOperation, before any I/O
  - step 3 of issuance repeated in `transaction`          -> staleLease
  - BindingScope opened; BoundReadAccess borrowed to the closure
  - every read validates key containment (L-8) and scope/lease (L-6)
  - every issued cursor is adopted by the scope at issuance
  - on return, and on a body failure, the scope closes:
      each adopted cursor reaches terminal backend cleanup, newest first,
      awaiting an advance that is still in flight
  - an escaped cursor is then already finished and fails on next advance
```

Bound write:

```text
lease.withWriteAccess(transaction) { access in ... }
  - the read binding steps above, with admit(.write)
  - BoundWriteAccess borrowed to the closure
```

Both bindings validate in the same order: what this process already knows
(domain, lease still held), then what the backend will admit, then the I/O
that resolves the generation. A caller error is therefore reported as itself
rather than as the backend's refusal, and no binding a backend would refuse
spends a round trip.

Recursive removal:

```text
remove(name, in: parent)
  edge = read(parent layer, parent.prefix, name)          absent -> keyNotFound
  admit(.delete)
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
| Lease registration | `LeaseRegistration` (`Mutex`) | until `release()` or `PartitionLease` deinit |
| Binding scope | `PartitionLease.withReadAccess` / `withWriteAccess` | one closure |
| Cursors issued by a binding | the binding scope, from `rangeCursor` until the scope closes | one closure; the scope drops its references once cleanup completed |

## Failure, Concurrency, and Constraints

`StorageError.Code` cases owned here: `incompatibleStorageLayout`,
`directoryLayerMismatch`, `partitionBoundaryViolation`,
`storageDomainMismatch`, `staleLease`, `invalidDirectoryAddress`. All have
retry disposition `never`.

Closing a binding is authoritative, so it can fail on its own. The dispositions
are fixed:

| Closing outcome | Result of the binding |
|---|---|
| body succeeded, cleanup succeeded | the body's value |
| body succeeded, cleanup failed | `PartitionBindingCleanupError` with no operation error; the value is not returned, because a value produced over storage whose cleanup failed is not a result |
| body failed, cleanup succeeded | the body's error unchanged |
| body failed, cleanup failed | `PartitionBindingCleanupError` carrying both |

The close reports only the cleanup failures it caused. A cursor the body itself
drove to a terminal state has already completed, and its stored failure was
delivered to the caller that drove it there; restating that failure at close
would convert a failure the body deliberately handled into a binding failure.
Closing therefore awaits such a cursor without reporting it again.

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
| D-1…D-12, state machine, operations 1–5, L-1, L-3, L-7, L-8 | `StorageKitConformance` `DirectoryConformanceCase` run by `InMemoryDirectoryConformanceTests`, `SQLiteDirectoryConformanceTests`, `PostgreSQLDirectoryConformanceTests`, `CloudflareDurableObjectDirectoryConformanceTests`, and `FDBDirectoryConformanceTests`. `verifyForeignRootRejection` is key-value only: FoundationDB has no such state, and `FDBDirectoryConformanceTests.siblingRootsAreIndependent` carries the per-root isolation it used to prove |
| L-2 revalidation at bind, including a lease bound to a transaction later than the one that issued it | `DirectoryConformanceCase.verifyLeaseStalenessDetection` run by every adapter suite |
| L-2 across engine instances over one store | `Tests/SQLiteStorageTests/SQLiteCrossEngineLeaseTests.swift`; SQLite is the cheapest backend that admits two engines over one physical root, and the guarantee is a property of the walk rather than of the adapter |
| D-13 on both write operations, under a plain and a Partition parent | `DirectoryConformanceCase.verifyStaleParentRejection` run by every adapter suite; FoundationDB passes it before the key-value catalog does, which is the evidence that the native layer already held the guarantee |
| L-4, L-5 | the type declarations: `BoundReadAccess` declares no mutation member and `BoundWriteAccess` declares no lifecycle member, so a violation does not compile and no run can observe one |
| L-6 escape, L-8 at the binding, and release and staleness as a holder observes them | `Tests/StorageKitTests/PartitionLeaseTests.swift` |
| L-6 cleanup authority: an escaped cursor is already finished, a close awaits an advance still in flight, a cleanup failure is reported through `PartitionBindingCleanupError`, and a cursor the body finished is not restated | `Tests/StorageKitTests/PartitionBindingScopeTests.swift` |

Changing the catalog layout, the bootstrap witness of any backend, or any
operation semantics requires updating this design first, then the StorageKit
module design, then every adapter module, then database-framework binding
(F-15 in `PROGRESS.md`).
