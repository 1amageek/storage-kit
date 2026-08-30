# StorageKitConformance

## Purpose and Scope

Shared behavioral proof for the `DirectoryAccess` and Partition lease
contract of every StorageKit engine (SPEC §24.2). Design level: module.
Parent: [storage-kit/DESIGN.md](../../DESIGN.md). Children: none.

## Responsibilities and Boundaries

- Owns `DirectoryConformanceCase`, the single fixture that each adapter test
  target runs against its own isolated backend, and its typed failure
  `DirectoryConformanceFailure`.
- Does not own adapter setup, service lifecycle, or test-framework
  reporting; adapter test targets wrap one step per test.
- Never imports a test framework so the fixture links into every test
  target and can also be executed by tools.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [StorageKit/Directory](../StorageKit/Directory/DESIGN.md) | depends on | `DirectoryAccess` operations 1–5, `StorageEngine.leasePartition`, `PartitionLease` | Verifies the operation table, state machine, lease invariants L-1…L-8, and domain checks | A change to any typed failure in the operation table must update the matching step here |
| [StorageKit](../StorageKit/DESIGN.md) | depends on | `StorageEngine`, `withTransaction`, `requestShutdown` | Every step creates fresh engines through the adapter factory | Steps assume `makeEngine` returns an empty keyspace |

## Architecture

```text
adapter test target (one @Test per step)
    -> DirectoryConformanceCase<Engine>(makeEngine:layoutProbe:)
        -> verifyRootInitialization / verifyLayoutRejection
        -> verifyCreateAndOpen / verifyListing / verifyPartitionContiguity
        -> verifyMove / verifyRemove
        -> verifyDomainMismatch / verifyLeaseLifecycle / verifyTransactionalAtomicity
            -> engine.directoryAccess, engine.leasePartition, engine.withTransaction
```

## Contracts and Invariants

- Input: a `@Sendable` factory that returns a fresh engine over an empty
  storage root, and a `LayoutProbe` that puts that root into a state the
  layout state machine must reject. Every step calls the factory at least once
  and shuts the engine down. `LayoutProbe.wholeStore` is the default and suits
  a backend whose whole store is one storage root; a backend that hosts its
  root below a path in a shared store supplies its own probe.
- Output: normal return on success; `DirectoryConformanceFailure(step:message:)`
  on a contract violation; adapter errors propagate unchanged.
- The case asserts only observable `DirectoryAccess` behavior. Key-layout
  facts (marker bytes, allocator encoding, node keys) are owned by
  `KeyValueDirectoryCatalog` tests in `StorageKitTests`.
- `verifyCreateAndOpen` asserts that a layer tag other than the empty tag and
  `partition` stays application-opaque: bytes that are not UTF-8 round-trip
  byte for byte through creation, a verified open, a mismatched open, and
  enumeration. SPEC F-03 forbids a backend from weakening that, so the
  assertion belongs to every adapter rather than to one backend's suite.
- `verifyLeaseSubtreeExclusion` proves the direction of SPEC §8.3 that
  `verifyLeaseLifecycle` does not reach: a lease covers the whole subtree under
  its Partition, so a node below it cannot be moved or removed, and a removal
  pending below a Partition blocks leasing that Partition. It also drives a
  caller-owned transaction from `createOwnedTransaction()` through `commit()`
  and `cancel()` and keeps the transaction object alive across the check, so a
  passing run proves the intents were released by the outcome rather than by
  deallocation.
- `verifyLayoutRejection` applies to every engine: the layout marker is the
  StorageKit-owned layout authority on all backends, including FoundationDB,
  where the native Directory Layer owns node existence but not the layout
  version. The step never names a key; it acts through `LayoutProbe`, so its
  scope is exactly the storage root the engine under test owns and one root is
  never rejected because of another root's data.

## Verification and Change Impact

- Proven by `InMemoryDirectoryConformanceTests` (reference engine) and by
  each adapter's `*DirectoryConformanceTests` suite.
- Changing a step changes the completion evidence of every adapter; rerun
  all adapter conformance suites after editing this module.
