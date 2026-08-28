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
| [StorageKit/Directory](../StorageKit/Directory/DESIGN.md) | depends on | `DirectoryAccess` operations 1–8, `StorageEngine.leasePartition`, `PartitionLease` | Verifies the operation table, state machine, lease invariants L-1…L-8, and domain checks | A change to any typed failure in the operation table must update the matching step here |
| [StorageKit](../StorageKit/DESIGN.md) | depends on | `StorageEngine`, `withTransaction`, `requestShutdown` | Every step creates fresh engines through the adapter factory | Steps assume `makeEngine` returns an empty keyspace |

## Architecture

```text
adapter test target (one @Test per step)
    -> DirectoryConformanceCase<Engine>(makeEngine:)
        -> verifyRootInitialization / verifyLayoutRejection
        -> verifyCreateAndOpen / verifyListing / verifyMove / verifyRemove
        -> verifyDomainMismatch / verifyLeaseLifecycle / verifyTransactionalAtomicity
            -> engine.directoryAccess, engine.leasePartition, engine.withTransaction
```

## Contracts and Invariants

- Input: a `@Sendable` factory that returns a fresh engine over an empty
  keyspace. Every step calls it at least once and shuts the engine down.
- Output: normal return on success; `DirectoryConformanceFailure(step:message:)`
  on a contract violation; adapter errors propagate unchanged.
- The case asserts only observable `DirectoryAccess` behavior. Key-layout
  facts (marker bytes, allocator encoding, node keys) are owned by
  `KeyValueDirectoryCatalog` tests in `StorageKitTests`.
- `verifyLayoutRejection` applies to engines whose catalog uses the
  StorageKit layout marker; adapters with a native Directory Layer decide
  in their own test target whether to run it.

## Verification and Change Impact

- Proven by `InMemoryDirectoryConformanceTests` (reference engine) and by
  each adapter's `*DirectoryConformanceTests` suite.
- Changing a step changes the completion evidence of every adapter; rerun
  all adapter conformance suites after editing this module.
