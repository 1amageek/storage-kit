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
| [StorageKit/Directory](../StorageKit/Directory/DESIGN.md) | depends on | `DirectoryAccess` operations 1–5, `StorageEngine.leasePartition`, `PartitionLease` | Verifies the operation table, root bootstrap state machine, lease invariants L-1…L-8, and domain checks | A change to any typed failure in the operation table must update the matching step here |
| [StorageKit](../StorageKit/DESIGN.md) | depends on | `StorageEngine`, `withTransaction`, `requestShutdown` | Every step creates fresh engines through the adapter factory | Steps assume `makeEngine` returns an empty keyspace |

## Architecture

```text
adapter test target (one @Test per step)
    -> DirectoryConformanceCase<Engine>(makeEngine:foreignRootProbe:)
        -> verifyRootInitialization / verifyForeignRootRejection
        -> verifyCreateAndOpen / verifyListing / verifyPartitionContiguity
        -> verifyMove / verifyRemove / verifyStaleParentRejection
        -> verifyDomainMismatch / verifyLeaseLifecycle / verifyTransactionalAtomicity
            -> engine.directoryAccess, engine.leasePartition, engine.withTransaction
```

## Contracts and Invariants

- Input: a `@Sendable` factory that returns a fresh engine over an empty
  storage root, and a `ForeignRootProbe` that writes data no Directory catalog
  wrote into that root. Every step calls the factory at least once and shuts
  the engine down. `ForeignRootProbe.wholeStore` is the default and suits a
  backend whose whole store is one storage root.
- Output: normal return on success; `DirectoryConformanceFailure(step:message:)`
  on a contract violation; adapter errors propagate unchanged.
- The case asserts only observable `DirectoryAccess` behavior. Key-layout
  facts (allocator encoding, node keys) are owned by `KeyValueDirectoryCatalog`
  tests in `StorageKitTests`.
- `verifyCreateAndOpen` asserts that a layer tag other than the empty tag and
  `partition` stays application-opaque: bytes that are not UTF-8 round-trip
  byte for byte through creation, a verified open, a mismatched open, and
  enumeration. SPEC F-03 forbids a backend from weakening that, so the
  assertion belongs to every adapter rather than to one backend's suite.
- `verifyStaleParentRejection` owns D-13, the guarantee that a write positions
  a node by the named parent's current existence rather than by a prefix the
  caller already holds. It removes a parent, then drives both write operations
  through the handle that survives the removal: a create below a removed plain
  Directory, a create below a removed Partition, and a move into a removed
  destination. Each must fail `keyNotFound`, and the live tree must be
  unchanged afterward. One outcome is required of every backend, because the
  alternative is a node that no path reaches and no removal covers, and because
  a caller that must ask which backend it holds cannot write correct recovery.
  The step also proves that the failure is absence rather than fabrication: no
  ancestor is rebuilt, and a rebuilt ancestor would carry no layer tag and yield
  a tree that no operation created and no invariant describes.
- `verifyLeaseStalenessDetection` proves the direction of SPEC §9.3 that
  `verifyLeaseLifecycle` does not reach: a lease is answered by the store, not
  by the object that issued it. One lease is carried across four caller-owned
  transactions. It binds successfully in a transaction later than the one that
  issued it, which is what makes the check meaningful rather than a restatement
  of issuance. A third transaction then removes and recreates the Partition at
  the same address, and the fixture asserts the recreated Partition carries a
  different `keyspacePrefix`, because the prefix allocator is strictly
  monotonic and never reuses a number. In the fourth transaction both
  `withWriteAccess` and `withReadAccess` must fail `staleLease`, and the
  recreated Partition must hold none of the data written under the old one.
  Driving `createOwnedTransaction()` through `commit()` and `cancel()` while
  keeping each transaction object alive across the check means a passing run
  proves the outcome came from the store rather than from deallocation.
- `verifyForeignRootRejection` applies to key-value engines only, and the
  restriction is a property of the backends rather than a gap in the fixture.
  A key-value root shares one flat keyspace with whatever wrote to it first,
  so initializing over existing keys would allocate a Directory on top of
  them; both `openRoot` and `openOrInitializeRoot` must fail with
  `incompatibleStorageLayout`. FoundationDB allocates every prefix through the
  native Directory Layer, which never returns a prefix already in use, so the
  state this step produces does not exist there and the FoundationDB suite
  does not run it. The step never names a key; it acts through
  `ForeignRootProbe`, so its scope is exactly the storage root the engine
  under test owns.

## Verification and Change Impact

- Proven by `InMemoryDirectoryConformanceTests` (reference engine) and by
  each adapter's `*DirectoryConformanceTests` suite.
- Changing a step changes the completion evidence of every adapter; rerun
  all adapter conformance suites after editing this module.
