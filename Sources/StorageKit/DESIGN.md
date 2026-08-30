# StorageKit

## Purpose and Scope

`StorageKit` is the platform-neutral module of `storage-kit`. It defines the
replaceable storage capabilities (`StorageEngine`, `Transaction`,
`TransactionReadAccess`, `TransactionAccess`, `DirectoryAccess`), the concrete
placement values (Directory, Partition, Subspace, Tuple), the lease types, the
transaction lifecycle owner, and the InMemory reference engine.

| Field | Value |
|---|---|
| Design level | module |
| Parent | [storage-kit package](../../DESIGN.md) |
| Children | [Directory component](Directory/DESIGN.md); `Storage/` (transaction, cursor, bounds, lifecycle), `Tuple/` (Tuple V1, Subspace, Versionstamp), `InMemory/` (reference engine) — the latter three predate this design and are owned by their source files and tests until a component design is added |

## Responsibilities and Boundaries

| Owns | Does not own |
|---|---|
| Public capability protocols and their default behavior | Backend connection handling and backend error taxonomies (adapter modules) |
| Transaction admission, commit-at-most-once, cancel, shutdown ordering | Which transactions a caller may start (Framework) |
| Bounded point read, bounded range read, cursor scope validation, mutation byte meter | Query semantics, ordering across Partitions |
| `Subspace` and Tuple V1 | Wire encodings |
| Directory placement, catalog authority for KV backends, leases | Reserved Directory names and Framework-owned Subspace layout |
| InMemory engine as executable reference for every contract | Production durability |

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [storage-kit package](../../DESIGN.md) | parent | package invariants P-1…P-7 | Module graph and package-wide invariants. | Public contract changes propagate to every adapter and to database-framework. |
| [Directory component](Directory/DESIGN.md) | child | `DirectoryAccess`, `Directory`, `Partition`, `PartitionLease`, `BoundReadAccess`, `BoundWriteAccess`, `KeyValueDirectoryCatalog` | Placement, catalog authority, root bootstrap, leases. | Read operations accept only `TransactionReadAccess`; catalog reads never create. |
| [Storage component](Storage/DESIGN.md) | child | `StorageEngine`, `Transaction`, cursors, `StorageError`, `StorageEngineLifecycle`, `StorageTransactionDomain` | Transaction and bound contracts; the cursor state owns advance ordering, post-advance validation, and terminal cleanup. | `StorageTransactionDomain` now also owns the lease registry. Cursor scope validation belongs to the cursor state, not to a caller wrapper. |
| [Tuple component](Tuple/DESIGN.md) | child | `Tuple`, `Subspace`, `TupleElement`, `strinc`, admission-aware `pack(admitting:)` | Tuple encoding frozen by golden vectors; admission measures the exact packed byte count before the single result allocation. | Any byte-layout change is a layout change. The admission callback is a caller resource boundary, never a backend limit. |
| `InMemory/` sources | child (no component design yet) | `InMemoryEngine`, `InMemoryTransaction` | Reference engine with conflict detection and versionstamps. | Reference only; must pass every shared fixture. |
| database-framework | used by | all public contracts | Composes containers and kernels. | Holds leases, never raw Partition addresses, as authority. |

## Architecture

```text
 caller (Framework)
   |  executeTransaction / withTransaction        leasePartition
   v                                                     |
 StorageEngine ----------------------------------------- +
   |  createTransaction                                  |
   v                                                     v
 TransactionLifecycleOwner (package, ~Copyable)   PartitionLeaseRegistry (per domain)
   |  owns exactly one Transaction                        |
   |  borrows attenuated TransactionAccess to callback    |
   |  commit at most once / authoritative cancel          |
   v                                                     v
 Transaction  <--- DirectoryAccess (catalog) ---> PartitionLease -> BoundReadAccess / BoundWriteAccess
```

Layering inside the module:

| Layer | Types | Depends on |
|---|---|---|
| Values | `ByteString` (database-types), `Tuple`, `Subspace`, `LayerTag`, `DirectoryEntry`, `StorageAddress`, `Directory`, `Partition` | database-types |
| Access contracts | `TransactionReadAccess`, `TransactionAccess`, `Transaction`, `KeySelector`, `KeyValueCursor`, `StreamingMode` | Values |
| Placement | `DirectoryAccess`, `KeyValueDirectoryCatalog`, `DirectoryLimits` | Access contracts |
| Leases | `PartitionLeaseRegistry`, `PartitionLease`, `BoundReadAccess`, `BoundWriteAccess` | Placement |
| Lifecycle | `StorageEngine`, `StorageEngineLifecycle`, `StorageTransactionDomain`, `TransactionLifecycleOwner`, `StorageTransactionExecutor` | Leases |
| Reference | `InMemoryEngine`, `InMemoryTransaction` | all above |

## Contracts and Invariants

### `StorageEngine`

| Member | Contract |
|---|---|
| `transactionDomain` | One domain per engine instance; every transaction, Directory, Partition, and lease produced by the engine carries this domain. |
| `directoryAccess` | Exactly one `DirectoryAccess` realization; no protocol default. |
| `createTransaction()` | Admission through `StorageEngineLifecycle`; rejected with `invalidOperation` after `requestShutdown()`. |
| `executeTransaction(_:)` | Runs the operation under `TransactionLifecycleOwner`: commit on success, cancel on failure, operation and cleanup failures preserved as `StorageTransactionCleanupError`. |
| `leasePartition(_:transaction:)` | Extension; issues `PartitionLease` after domain check, registry reservation, and generation validation in the caller's transaction (details in the Directory component design). |
| `requestShutdown()` / `waitUntilShutdown()` | Shutdown rejects new transactions and new lease issuance, then completes admitted backend cleanup. It does not wait for outstanding leases: a lease after shutdown cannot perform I/O because transaction admission is closed, and waiting would let a leaked lease hang shutdown. |

Removed in this revision: `namespaceResolver`, `namespaceCatalog`,
`resolveOrCreateNamespace`, `resolveExistingNamespace`, `listNamespaces`,
`removeNamespace`, `namespaceExists`, `NamespaceResolver`, `NamespaceCatalog`,
`DeterministicNamespaceResolver`, and the FDB `NamespaceRegistry`. A root
holding deterministic path-derived prefixes is foreign data and is rejected
(see the Directory component design, root bootstrap state machine).

### Transaction invariants (M-1…M-7)

| ID | Invariant |
|---|---|
| M-1 | A transaction belongs to exactly one domain; access from another domain fails `storageDomainMismatch`. |
| M-2 | `commit()` succeeds at most once; a second commit or a commit after cancel fails `invalidOperation`. |
| M-3 | `cancel()` is authoritative: after it returns, backend cleanup is complete or a typed cleanup failure is reported. |
| M-4 | Every failure is a `StorageError`; unsupported operations fail `unsupportedOperation`, never empty success. |
| M-5 | Read operations accept `TransactionReadAccess` and cannot mutate. |
| M-6 | Cursors are scoped: advancing a cursor after its owning scope closed fails typed. |
| M-7 | Bounds (`keyTooLarge`, `valueTooLarge`, `transactionTooLarge`, `invalidPointReadMaximum`, `pointReadValueTooLarge`) are enforced before backend I/O where the bound is known. |

### `TransactionLifecycleOwner` (package, `~Copyable`)

- Owns exactly one `Transaction`; constructed only by engine code.
- `execute(operation:)` borrows the transaction as `any TransactionAccess` to
  the operation inside `ActiveTransactionContext`, commits on success, cancels
  on failure, and returns or throws exactly one outcome.
- Adapters that must not cancel after a particular commit failure (FDB
  `commitUnknownResult`) express that through the owner's
  `retainsTransactionAfterCommitFailure` predicate rather than by bypassing the
  owner.
- On completion the owner releases the transaction's subtree intents in the
  domain's lease registry.

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Engine lifecycle phase | `StorageEngineLifecycle` (`Mutex`) | engine instance |
| Active transaction set | engine (`Mutex`) | until commit/cancel completes |
| Lease registry | `StorageTransactionDomain.leases` | engine instance |
| Transaction ownership | `TransactionLifecycleOwner` | one `execute` call |
| Bound access scope | `PartitionLease.withReadAccess` / `withWriteAccess` | one closure |

## Failure, Concurrency, and Constraints

- Shared mutable state uses `Mutex` (`StorageEngineLifecycle`, registry,
  InMemory store) or actor isolation; no `@unchecked Sendable` outside verified
  immutable backings.
- `withReadAccess` / `withWriteAccess` are `nonisolated(nonsending)` so the
  noncopyable bound access is borrowed on the caller's isolation without
  requiring a `Sendable` closure.
- Operational bounds are owned by `DirectoryLimits` (Directory component) and
  the existing point-read and mutation byte limits in `Storage/`.

## Verification and Change Impact

| Evidence | Location |
|---|---|
| Transaction, cursor, lifecycle, byte meter, versionstamp contracts | `Tests/StorageKitTests/*` |
| Tuple V1 frozen bytes | `Tests/StorageKitTests/TupleV1GoldenVectorTests.swift` |
| Admission-aware packing traversal and typed rejection | `Tests/StorageKitTests/TupleTests.swift` |
| Cursor advance ordering, post-advance validation, finish coordination | `Tests/StorageKitTests/TransactionRangeCleanupTests.swift` |
| Directory, Partition, root bootstrap, lease contracts on the reference engine | `Tests/StorageKitTests/InMemoryDirectoryConformanceTests.swift` driving `StorageKitConformance` |
| Reference-only escape and compile-level attenuation | `Tests/StorageKitTests/PartitionLeaseTests.swift` |

Changing any public contract here requires re-verifying every adapter module
through the shared `StorageKitConformance` fixture and the database-framework
binding.
