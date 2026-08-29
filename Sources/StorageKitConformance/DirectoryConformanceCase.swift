import DatabaseTypes
import StorageKit

/// Shared behavioral proof for one `StorageEngine`'s `DirectoryAccess` and
/// Partition lease contract (SPEC §24.2).
///
/// Every step creates fresh engines through `makeEngine`, so each adapter test
/// target wraps one step per test and supplies its own isolated backend. The
/// case never imports a test framework; it reports contract violations as
/// `DirectoryConformanceFailure` and propagates adapter failures unchanged.
public struct DirectoryConformanceCase<Engine: StorageEngine>: Sendable {
    private let makeEngine: @Sendable () async throws -> Engine

    public init(makeEngine: @escaping @Sendable () async throws -> Engine) {
        self.makeEngine = makeEngine
    }

    private struct AbortTransaction: Error {}

    // MARK: - Steps

    /// Root Directory initialization is explicit, idempotent, and stable.
    public func verifyRootInitialization() async throws {
        let step = "root initialization"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let before = try await engine.withTransaction { transaction in
                try await catalog.openRoot(transaction: transaction)
            }
            try require(before == nil, step, "openRoot must return nil before initialization")

            let root = try await engine.withTransaction { transaction in
                try await catalog.openOrInitializeRoot(transaction: transaction)
            }
            try require(root.address == .root, step, "root Directory must carry the root address")
            try require(root.layer.isDefault, step, "root Directory must carry the default layer tag")
            try require(root.domain === engine.transactionDomain, step, "root Directory must belong to the engine domain")

            let reopened = try await engine.withTransaction { transaction -> (Directory?, Directory) in
                let opened = try await catalog.openRoot(transaction: transaction)
                let again = try await catalog.openOrInitializeRoot(transaction: transaction)
                return (opened, again)
            }
            try require(reopened.0 == root, step, "openRoot must return the initialized root")
            try require(reopened.1 == root, step, "openOrInitializeRoot must be idempotent")
        }
    }

    /// A keyspace without a supported layout is rejected, never adopted.
    public func verifyLayoutRejection() async throws {
        let step = "layout rejection"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            try await engine.withTransaction { transaction in
                try transaction.setValue([0x01], for: [0x61])
            }
            try await requireFailure(.incompatibleStorageLayout, step, "openRoot on a nonempty keyspace without marker") {
                _ = try await engine.withTransaction { transaction in
                    try await catalog.openRoot(transaction: transaction)
                }
            }
            try await requireFailure(.incompatibleStorageLayout, step, "openOrInitializeRoot on a nonempty keyspace without marker") {
                _ = try await engine.withTransaction { transaction in
                    try await catalog.openOrInitializeRoot(transaction: transaction)
                }
            }
        }
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            try await engine.withTransaction { transaction in
                try transaction.setValue([0x53, 0x4B, 0x4C, 0x02], for: StorageLayoutMarker.key)
            }
            try await requireFailure(.incompatibleStorageLayout, step, "openRoot with an unknown marker") {
                _ = try await engine.withTransaction { transaction in
                    try await catalog.openRoot(transaction: transaction)
                }
            }
        }
    }

    /// Operations 1–2: creation is idempotent, opening a missing child yields
    /// nil, one name namespace carries one layer tag, and addresses are
    /// validated.
    public func verifyCreateAndOpen() async throws {
        let step = "create and open"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let created = try await engine.withTransaction { transaction -> (Directory, Directory, Directory, Partition) in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                let a = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                let b = try await catalog.openOrCreateDirectory("b", in: a, transaction: transaction)
                let partition = try await catalog.openOrCreatePartition("tenant-1", in: b, transaction: transaction)
                return (root, a, b, partition)
            }
            let (root, a, b, partition) = created
            try require(a.address == (try StorageAddress(["a"])), step, "child address must extend the parent address")
            try require(b.address == (try StorageAddress(["a", "b"])), step, "nested address must extend the parent address")
            try require(a.layer == .default && !a.isPartition, step, "a created Directory must carry the default layer tag")
            try require(partition.root.layer == .partition && partition.root.isPartition, step, "a created Partition must carry the partition layer tag")
            try require(partition.name == "tenant-1", step, "a Partition must be identified by its exact name")
            try require(partition.address == b.address.appending("tenant-1"), step, "Partition must be addressed under its parent")

            // Siblings of one Directory Layer never nest inside one another.
            let prefixes = [root.keyspacePrefix, a.keyspacePrefix, b.keyspacePrefix, partition.keyspacePrefix]
            for (index, prefix) in prefixes.enumerated() {
                for (otherIndex, other) in prefixes.enumerated() where otherIndex != index {
                    try require(!other.starts(with: prefix), step, "node prefixes of one layer must be prefix-free")
                }
            }

            let reopened = try await engine.withTransaction { transaction -> (Directory?, Directory?, Partition?, Directory?, Directory?, Partition?, Directory, Partition) in
                let root = try await requireRoot(catalog, transaction, step)
                let openedA = try await catalog.openDirectory("a", in: root, transaction: transaction)
                let openedB = try await catalog.openDirectory(at: DirectoryPath("a", "b"), in: root, transaction: transaction)
                let openedPartition = try await catalog.openPartition("tenant-1", in: b, transaction: transaction)
                let walked = try await catalog.openDirectory(at: partition.address, transaction: transaction)
                let missingDirectory = try await catalog.openDirectory("zzz", in: root, transaction: transaction)
                let missingPartition = try await catalog.openPartition("missing", in: b, transaction: transaction)
                let againA = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                let againPartition = try await catalog.openOrCreatePartition("tenant-1", in: b, transaction: transaction)
                return (openedA, openedB, openedPartition, walked, missingDirectory, missingPartition, againA, againPartition)
            }
            try require(reopened.0 == a, step, "openDirectory must return the created Directory")
            try require(reopened.1 == b, step, "openDirectory(at:) must walk the path to the same Directory")
            try require(reopened.2 == partition, step, "openPartition must return the created Partition")
            try require(reopened.3 == partition.root, step, "openDirectory(at:) must resolve a Partition address to its node")
            try require(reopened.4 == nil, step, "opening a missing Directory must return nil")
            try require(reopened.5 == nil, step, "opening a missing Partition must return nil")
            try require(reopened.6 == a, step, "openOrCreateDirectory must be idempotent")
            try require(reopened.7 == partition, step, "openOrCreatePartition must be idempotent")

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await requireFailure(.directoryLayerMismatch, step, "opening a Partition as a plain Directory") {
                    _ = try await catalog.openDirectory("tenant-1", in: b, transaction: transaction)
                }
                try await requireFailure(.directoryLayerMismatch, step, "creating a plain Directory over a Partition name") {
                    _ = try await catalog.openOrCreateDirectory("tenant-1", in: b, transaction: transaction)
                }
                try await requireFailure(.directoryLayerMismatch, step, "opening a plain Directory as a Partition") {
                    _ = try await catalog.openPartition("a", in: root, transaction: transaction)
                }
                try await requireFailure(.directoryLayerMismatch, step, "creating a Partition over a plain Directory name") {
                    _ = try await catalog.openOrCreatePartition("a", in: root, transaction: transaction)
                }
                // An unverified open reports the stored tag instead of failing.
                let untypedPartition = try await catalog.open("tenant-1", expecting: nil, in: b, transaction: transaction)
                try require(untypedPartition?.layer == .partition, step, "an unverified open must report the stored partition tag")
                let untypedDirectory = try await catalog.open("a", expecting: nil, in: root, transaction: transaction)
                try require(untypedDirectory?.layer == .default, step, "an unverified open must report the stored default tag")
            }

            try await recovering({ $0 is AbortTransaction }) {
                try await engine.withTransaction { transaction in
                    let root = try await requireRoot(catalog, transaction, step)
                    try await verifyNameValidation(catalog, root, transaction, step)
                    throw AbortTransaction()
                }
            }
            try await verifyDepthValidation(engine, catalog, step)
        }
    }

    /// Name validation rejects before any I/O, so one aborted transaction
    /// proves it without leaving state behind.
    private func verifyNameValidation(
        _ catalog: any DirectoryAccess,
        _ root: Directory,
        _ transaction: any TransactionAccess,
        _ step: String
    ) async throws {
        try await requireFailure(.invalidDirectoryAddress, step, "empty Directory name") {
            _ = try await catalog.openOrCreateDirectory("", in: root, transaction: transaction)
        }
        let longName = String(repeating: "n", count: DirectoryLimits.maximumComponentByteCount + 1)
        try await requireFailure(.invalidDirectoryAddress, step, "Directory name over the byte limit") {
            _ = try await catalog.openDirectory(longName, in: root, transaction: transaction)
        }
    }

    /// The deepest legal address is reachable and the next component is
    /// rejected.
    ///
    /// The chain is built in committed batches rather than in one transaction.
    /// The contract states which depth is legal, not how many levels one
    /// transaction can create, and a backend with a bounded transaction
    /// lifetime would otherwise fail this step for a reason the contract does
    /// not state. A Directory value stays valid across transactions of its own
    /// engine, so each batch continues from the node the previous batch
    /// committed.
    private func verifyDepthValidation(
        _ engine: Engine,
        _ catalog: any DirectoryAccess,
        _ step: String
    ) async throws {
        let batchSize = 8
        var deep = try await engine.withTransaction { transaction in
            try await requireRoot(catalog, transaction, step)
        }
        var level = 0
        while level < DirectoryLimits.maximumDepth {
            let levels = level..<min(level + batchSize, DirectoryLimits.maximumDepth)
            let parent = deep
            deep = try await engine.withTransaction { transaction -> Directory in
                var node = parent
                for index in levels {
                    node = try await catalog.openOrCreateDirectory(
                        "d\(index)",
                        in: node,
                        transaction: transaction
                    )
                }
                return node
            }
            level = levels.upperBound
        }
        try require(
            deep.address.depth == DirectoryLimits.maximumDepth,
            step,
            "the deepest legal Directory must carry the maximum depth"
        )
        let deepest = deep
        try await engine.withTransaction { transaction in
            try await requireFailure(.invalidDirectoryAddress, step, "Directory depth over the limit") {
                _ = try await catalog.openOrCreateDirectory(
                    "overflow",
                    in: deepest,
                    transaction: transaction
                )
            }
        }
    }

    /// Operation 3: one namespace lists every child with its layer tag, in
    /// byte order, paginated, and bounded by `maximumListLimit`.
    public func verifyListing() async throws {
        let step = "listing"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            try await engine.withTransaction { transaction in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                for name in ["c", "a", "b"] {
                    _ = try await catalog.openOrCreateDirectory(name, in: root, transaction: transaction)
                }
                for name in ["p2", "p1"] {
                    _ = try await catalog.openOrCreatePartition(name, in: root, transaction: transaction)
                }
            }
            let listed = try await engine.withTransaction { transaction -> ([DirectoryEntry], [DirectoryEntry], [DirectoryEntry], [DirectoryEntry]) in
                let root = try await requireRoot(catalog, transaction, step)
                let all = try await catalog.listChildren(in: root, after: nil, limit: 10, transaction: transaction)
                let page1 = try await catalog.listChildren(in: root, after: nil, limit: 2, transaction: transaction)
                let page2 = try await catalog.listChildren(in: root, after: page1.last?.name, limit: 2, transaction: transaction)
                guard let child = try await catalog.openDirectory("a", in: root, transaction: transaction) else {
                    throw DirectoryConformanceFailure(step: step, message: "Directory 'a' must exist")
                }
                let childEntries = try await catalog.listChildren(in: child, after: nil, limit: 10, transaction: transaction)
                return (all, page1, page2, childEntries)
            }
            try require(listed.0.map(\.name) == ["a", "b", "c", "p1", "p2"], step, "children must list in byte order, got \(listed.0.map(\.name))")
            try require(listed.0.map(\.isPartition) == [false, false, false, true, true], step, "each entry must carry its own layer tag")
            try require(listed.1.map(\.name) == ["a", "b"], step, "first page must honor the limit, got \(listed.1.map(\.name))")
            try require(listed.2.map(\.name) == ["c", "p1"], step, "second page must continue after the cursor, got \(listed.2.map(\.name))")
            try require(listed.3.isEmpty, step, "a child listing must not include the parent's children")

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await requireFailure(.invalidOperation, step, "listing limit 0") {
                    _ = try await catalog.listChildren(in: root, after: nil, limit: 0, transaction: transaction)
                }
                try await requireFailure(.invalidOperation, step, "listing limit above the maximum") {
                    _ = try await catalog.listChildren(in: root, after: nil, limit: DirectoryLimits.maximumListLimit + 1, transaction: transaction)
                }
            }
        }
    }

    /// A Partition owns one contiguous keyspace, Partitions nest, and the
    /// Partition data root never collides with an allocated descendant.
    public func verifyPartitionContiguity() async throws {
        let step = "partition contiguity"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let built = try await engine.withTransaction { transaction -> (Directory, Partition, Directory, Partition, Directory) in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                let outside = try await catalog.openOrCreateDirectory("outside", in: root, transaction: transaction)
                let tenant = try await catalog.openOrCreatePartition("tenant", in: root, transaction: transaction)
                let inner = try await catalog.openOrCreateDirectory("inner", in: tenant.root, transaction: transaction)
                let nested = try await catalog.openOrCreatePartition("nested", in: inner, transaction: transaction)
                let deep = try await catalog.openOrCreateDirectory("deep", in: nested.root, transaction: transaction)
                return (outside, tenant, inner, nested, deep)
            }
            let (outside, tenant, inner, nested, deep) = built

            for descendant in [inner, nested.root, deep] {
                try require(
                    descendant.keyspacePrefix.starts(with: tenant.keyspacePrefix),
                    step,
                    "every descendant of a Partition must be allocated inside its keyspace"
                )
            }
            try require(deep.keyspacePrefix.starts(with: nested.keyspacePrefix), step, "a nested Partition must contain its own descendants")
            try require(!outside.keyspacePrefix.starts(with: tenant.keyspacePrefix), step, "a sibling of a Partition must lie outside it")
            try require(!tenant.keyspacePrefix.starts(with: outside.keyspacePrefix), step, "a Partition must lie outside its siblings")

            let dataRoot = tenant.root.root.prefix
            try require(dataRoot.starts(with: tenant.keyspacePrefix) && dataRoot != tenant.keyspacePrefix, step, "the Partition data root must lie inside the Partition keyspace")
            for descendant in [inner, nested.root, deep] {
                try require(!descendant.keyspacePrefix.starts(with: dataRoot), step, "an allocated descendant must not fall inside the Partition data root")
                try require(!dataRoot.starts(with: descendant.keyspacePrefix), step, "the Partition data root must not fall inside an allocated descendant")
            }

            // Data at the Partition data root survives later allocation, and
            // one lease reaches every Subspace inside the Partition.
            let tenantKey = key(in: tenant.root, suffix: [0x6B])
            let deepKey = key(in: deep, suffix: [0x6B])
            try await engine.withTransaction { transaction in
                let lease = try await engine.leasePartition(tenant, transaction: transaction)
                try await lease.withWriteAccess(transaction) { access in
                    try access.setValue([0x01], for: tenantKey)
                    try access.setValue([0x02], for: deepKey)
                }
                lease.release()
            }
            let stored = try await engine.withTransaction { transaction -> (ByteString?, ByteString?, Directory?) in
                let reopened = try await catalog.openDirectory(at: deep.address, transaction: transaction)
                return (
                    try await transaction.getValue(for: tenantKey),
                    try await transaction.getValue(for: deepKey),
                    reopened
                )
            }
            try require(stored.0 == [0x01], step, "Partition data must survive descendant allocation")
            try require(stored.1 == [0x02], step, "a descendant Subspace inside a Partition must be writable through its lease")
            try require(stored.2 == deep, step, "a Directory nested through a Partition must reopen by address")
        }
    }

    /// Operation 4: move preserves the node keyspace and its subtree, moves a
    /// whole Partition, and refuses to cross a Partition boundary.
    public func verifyMove() async throws {
        let step = "move"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let setup = try await engine.withTransaction { transaction -> (Directory, Directory, Directory, Directory, Partition, Directory, ByteString, ByteString) in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                let a = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                let b = try await catalog.openOrCreateDirectory("b", in: root, transaction: transaction)
                let child = try await catalog.openOrCreateDirectory("child", in: a, transaction: transaction)
                let sub = try await catalog.openOrCreateDirectory("sub", in: child, transaction: transaction)
                let tenant = try await catalog.openOrCreatePartition("tenant", in: root, transaction: transaction)
                let insideDirectory = try await catalog.openOrCreateDirectory("inside", in: tenant.root, transaction: transaction)
                _ = try await catalog.openOrCreateDirectory("other", in: b, transaction: transaction)
                let childKey = key(in: child, suffix: [0x6B])
                let tenantKey = key(in: tenant.root, suffix: [0x6B])
                try transaction.setValue([0x01], for: childKey)
                try transaction.setValue([0x02], for: tenantKey)
                return (a, b, child, sub, tenant, insideDirectory, childKey, tenantKey)
            }
            let (a, b, child, sub, tenant, insideDirectory, childKey, tenantKey) = setup

            let moved = try await engine.withTransaction { transaction in
                try await catalog.move("child", in: a, to: "renamed", in: b, transaction: transaction)
            }
            try require(moved.keyspacePrefix == child.keyspacePrefix, step, "move must preserve the node keyspace")
            try require(moved.address == b.address.appending("renamed"), step, "moved Directory must carry the destination address")

            let movedTenantNode = try await engine.withTransaction { transaction in
                try await catalog.move("tenant", in: try await requireRoot(catalog, transaction, step), to: "tenant", in: b, transaction: transaction)
            }
            guard let movedTenant = Partition(movedTenantNode) else {
                throw DirectoryConformanceFailure(step: step, message: "a moved Partition must keep its partition layer tag")
            }
            try require(movedTenant.keyspacePrefix == tenant.keyspacePrefix, step, "moving a Partition must preserve its keyspace")

            let after = try await engine.withTransaction { transaction -> (Directory?, Directory?, Directory?, Directory?, ByteString?, ByteString?) in
                let old = try await catalog.openDirectory("child", in: a, transaction: transaction)
                let new = try await catalog.openDirectory("renamed", in: b, transaction: transaction)
                let movedSub = try await catalog.openDirectory("sub", in: moved, transaction: transaction)
                let movedInside = try await catalog.openDirectory("inside", in: movedTenant.root, transaction: transaction)
                let childData = try await transaction.getValue(for: childKey)
                let tenantData = try await transaction.getValue(for: tenantKey)
                return (old, new, movedSub, movedInside, childData, tenantData)
            }
            try require(after.0 == nil, step, "source child must no longer resolve after move")
            try require(after.1 == moved, step, "destination child must resolve to the moved Directory")
            try require(after.2?.keyspacePrefix == sub.keyspacePrefix, step, "descendants must keep their keyspaces after move")
            try require(after.2?.address == moved.address.appending("sub"), step, "descendants must be re-addressed under the destination")
            try require(after.3?.keyspacePrefix == insideDirectory.keyspacePrefix, step, "a moved Partition must keep its own descendants")
            try require(after.4 == [0x01], step, "data under the moved Directory must remain readable")
            try require(after.5 == [0x02], step, "data inside the moved Partition must remain readable")

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await requireFailure(.partitionBoundaryViolation, step, "moving a Directory into a Partition") {
                    _ = try await catalog.move("renamed", in: b, to: "renamed", in: movedTenant.root, transaction: transaction)
                }
                try await requireFailure(.partitionBoundaryViolation, step, "moving a Directory out of a Partition") {
                    _ = try await catalog.move("inside", in: movedTenant.root, to: "escaped", in: root, transaction: transaction)
                }
                try await requireFailure(.keyNotFound, step, "moving a missing child") {
                    _ = try await catalog.move("missing", in: a, to: "x", in: b, transaction: transaction)
                }
                try await requireFailure(.invalidDirectoryAddress, step, "moving a Directory into its own subtree") {
                    _ = try await catalog.move("b", in: root, to: "x", in: b, transaction: transaction)
                }
                try await requireFailure(.invalidOperation, step, "moving onto an existing target") {
                    _ = try await catalog.move("a", in: root, to: "other", in: b, transaction: transaction)
                }
            }
        }
    }

    /// Operation 5: removal is recursive and atomic, clears every descendant
    /// key, and never reuses a released keyspace.
    public func verifyRemove() async throws {
        let step = "remove"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let setup = try await engine.withTransaction { transaction -> (Directory, Directory, Directory, Partition, Partition, [ByteString]) in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                let a = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                let child = try await catalog.openOrCreateDirectory("child", in: a, transaction: transaction)
                let tenant = try await catalog.openOrCreatePartition("tenant", in: child, transaction: transaction)
                let inner = try await catalog.openOrCreateDirectory("inner", in: tenant.root, transaction: transaction)
                let nested = try await catalog.openOrCreatePartition("nested", in: inner, transaction: transaction)
                let keys = [
                    key(in: a, suffix: [0x6B]),
                    key(in: child, suffix: [0x6B]),
                    key(in: tenant.root, suffix: [0x6B]),
                    key(in: inner, suffix: [0x6B]),
                    key(in: nested.root, suffix: [0x6B])
                ]
                for dataKey in keys {
                    try transaction.setValue([0x01], for: dataKey)
                }
                return (a, child, inner, tenant, nested, keys)
            }
            let (a, child, inner, tenant, nested, keys) = setup

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await requireFailure(.keyNotFound, step, "removing a missing child") {
                    try await catalog.remove("missing", in: root, transaction: transaction)
                }
            }

            // Removing a nonempty subtree succeeds in one transaction.
            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await catalog.remove("a", in: root, transaction: transaction)
            }

            let after = try await engine.withTransaction { transaction -> (Directory?, [DirectoryEntry], [ByteString?], Directory) in
                let root = try await requireRoot(catalog, transaction, step)
                let gone = try await catalog.openDirectory("a", in: root, transaction: transaction)
                let entries = try await catalog.listChildren(in: root, after: nil, limit: 10, transaction: transaction)
                var values: [ByteString?] = []
                for dataKey in keys {
                    values.append(try await transaction.getValue(for: dataKey))
                }
                let fresh = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                return (gone, entries, values, fresh)
            }
            try require(after.0 == nil, step, "removed Directory must not resolve")
            try require(after.1.isEmpty, step, "a removed subtree must leave no listable child")
            try require(after.2.allSatisfy { $0 == nil }, step, "removal must clear every descendant key")

            let released = [a.keyspacePrefix, child.keyspacePrefix, inner.keyspacePrefix, tenant.keyspacePrefix, nested.keyspacePrefix]
            try require(!released.contains(after.3.keyspacePrefix), step, "a released keyspace must not be reused")

            // A Partition's whole subtree is reachable through one range and
            // leaves through one range clear.
            let partitioned = try await engine.withTransaction { transaction -> (Directory, Partition, [ByteString], ByteString) in
                let root = try await requireRoot(catalog, transaction, step)
                let zone = try await catalog.openOrCreateDirectory("zone", in: root, transaction: transaction)
                let leased = try await catalog.openOrCreatePartition("tenant", in: zone, transaction: transaction)
                let insideDirectory = try await catalog.openOrCreateDirectory("inner", in: leased.root, transaction: transaction)
                let insidePartition = try await catalog.openOrCreatePartition("nested", in: insideDirectory, transaction: transaction)
                let deep = try await catalog.openOrCreateDirectory("deep", in: insidePartition.root, transaction: transaction)
                let inside = [
                    key(in: leased.root, suffix: [0x6B]),
                    key(in: insideDirectory, suffix: [0x6B]),
                    key(in: insidePartition.root, suffix: [0x6B]),
                    key(in: deep, suffix: [0x6B])
                ]
                let outside = key(in: zone, suffix: [0x6B])
                for dataKey in inside {
                    try transaction.setValue([0x01], for: dataKey)
                }
                try transaction.setValue([0x02], for: outside)
                return (zone, leased, inside, outside)
            }
            let (zone, partition, insideKeys, outsideKey) = partitioned
            let partitionEnd = try strinc(partition.keyspacePrefix)

            let covered = try await engine.withTransaction { transaction in
                try await transaction.collectRange(begin: partition.keyspacePrefix, end: partitionEnd)
            }
            let coveredKeys = Set(covered.map(\.0))
            for dataKey in insideKeys {
                try require(coveredKeys.contains(dataKey), step, "one range read must cover every key inside a Partition")
            }
            try require(!coveredKeys.contains(outsideKey), step, "a Partition range must not reach outside the Partition")
            try require(
                covered.allSatisfy { $0.0.starts(with: partition.keyspacePrefix) },
                step,
                "every key a Partition range returns must lie inside the Partition"
            )

            try await engine.withTransaction { transaction in
                try await catalog.remove("tenant", in: zone, transaction: transaction)
            }
            let emptied = try await engine.withTransaction { transaction -> ([(ByteString, ByteString)], Directory?, ByteString?) in
                let remaining = try await transaction.collectRange(begin: partition.keyspacePrefix, end: partitionEnd)
                let reopened = try await catalog.openDirectory("tenant", in: zone, transaction: transaction)
                let survivor = try await transaction.getValue(for: outsideKey)
                return (remaining, reopened, survivor)
            }
            try require(emptied.0.isEmpty, step, "removing a Partition must leave its whole range empty")
            try require(emptied.1 == nil, step, "a removed Partition must not resolve")
            try require(emptied.2 == [0x02], step, "removing a Partition must not touch keys outside it")
        }
    }

    /// Every operation rejects participants from another engine before I/O.
    public func verifyDomainMismatch() async throws {
        let step = "domain mismatch"
        try await withEngine { first in
            try await withEngine { second in
                let created = try await first.withTransaction { transaction -> (Directory, Partition) in
                    let root = try await first.directoryAccess.openOrInitializeRoot(transaction: transaction)
                    let partition = try await first.directoryAccess.openOrCreatePartition("p", in: root, transaction: transaction)
                    return (root, partition)
                }
                let (foreignRoot, foreignPartition) = created
                try await second.withTransaction { transaction in
                    _ = try await second.directoryAccess.openOrInitializeRoot(transaction: transaction)
                    try await requireFailure(.storageDomainMismatch, step, "foreign transaction with the catalog") {
                        _ = try await first.directoryAccess.openRoot(transaction: transaction)
                    }
                    try await requireFailure(.storageDomainMismatch, step, "foreign Directory with the catalog") {
                        _ = try await second.directoryAccess.openOrCreateDirectory("x", in: foreignRoot, transaction: transaction)
                    }
                    try await requireFailure(.storageDomainMismatch, step, "foreign Directory in a listing") {
                        _ = try await second.directoryAccess.listChildren(in: foreignRoot, after: nil, limit: 1, transaction: transaction)
                    }
                    try await requireFailure(.storageDomainMismatch, step, "foreign Partition lease") {
                        _ = try await second.leasePartition(foreignPartition, transaction: transaction)
                    }
                    try await requireFailure(.storageDomainMismatch, step, "foreign transaction for a lease") {
                        _ = try await first.leasePartition(foreignPartition, transaction: transaction)
                    }
                }
            }
        }
    }

    /// Leases bound data access to the Partition, block move and removal of any
    /// covering subtree, detect stale Partition values, and stop being issued
    /// after shutdown.
    public func verifyLeaseLifecycle() async throws {
        let step = "lease lifecycle"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let created = try await engine.withTransaction { transaction -> (Partition, Partition) in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                _ = try await catalog.openOrCreatePartition("q", in: root, transaction: transaction)
                let parent = try await catalog.openOrCreateDirectory("parent", in: root, transaction: transaction)
                let nested = try await catalog.openOrCreatePartition("nested", in: parent, transaction: transaction)
                let partition = try await catalog.openOrCreatePartition("p", in: root, transaction: transaction)
                return (partition, nested)
            }
            let (partition, nested) = created
            let dataKey = key(in: partition.root, suffix: [0x6B])

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                let lease = try await engine.leasePartition(partition, transaction: transaction)
                try require(lease.isActive, step, "a fresh lease must be active")
                try await requireFailure(.directoryLeased, step, "removing a leased Partition") {
                    try await catalog.remove("p", in: root, transaction: transaction)
                }
                let nestedLease = try await engine.leasePartition(nested, transaction: transaction)
                try await requireFailure(.directoryLeased, step, "moving the ancestor of a leased Partition") {
                    _ = try await catalog.move("parent", in: root, to: "moved", in: root, transaction: transaction)
                }
                try await requireFailure(.directoryLeased, step, "removing the ancestor of a leased Partition") {
                    try await catalog.remove("parent", in: root, transaction: transaction)
                }
                nestedLease.release()
                let movedParent = try await catalog.move("parent", in: root, to: "moved", in: root, transaction: transaction)
                try require(movedParent.address == (try StorageAddress(["moved"])), step, "ancestor move must succeed once the lease is released")
                let value = try await lease.withWriteAccess(transaction) { access -> ByteString? in
                    try access.setValue([0x01], for: dataKey)
                    try await requireFailure(.invalidOperation, step, "writing outside the Partition") {
                        try access.setValue([0x01], for: [0x7A])
                    }
                    try await requireFailure(.invalidOperation, step, "reading outside the Partition") {
                        _ = try await access.getValue(for: [0x7A])
                    }
                    return try await access.getValue(for: dataKey)
                }
                try require(value == [0x01], step, "bound write must be readable through bound read")
                lease.release()
            }

            // Removal is recursive, so a released Partition holding data is
            // removed without an emptiness precondition.
            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await catalog.remove("p", in: root, transaction: transaction)
                try await requireFailure(.staleLease, step, "leasing a Partition whose removal is pending in the same transaction") {
                    _ = try await engine.leasePartition(partition, transaction: transaction)
                }
            }

            let recreated = try await engine.withTransaction { transaction -> Partition in
                let root = try await requireRoot(catalog, transaction, step)
                let cleared = try await transaction.getValue(for: dataKey)
                try require(cleared == nil, step, "removing a Partition must clear its data")
                return try await catalog.openOrCreatePartition("p", in: root, transaction: transaction)
            }
            try require(recreated.keyspacePrefix != partition.keyspacePrefix, step, "a recreated Partition must receive a new keyspace")

            try await engine.withTransaction { transaction in
                try await requireFailure(.staleLease, step, "leasing a Partition value whose keyspace was replaced") {
                    _ = try await engine.leasePartition(partition, transaction: transaction)
                }
                let lease = try await engine.leasePartition(recreated, transaction: transaction)
                try require(lease.isActive, step, "the recreated Partition must be leasable")
                lease.release()
            }

            let other = try await engine.withTransaction { transaction -> Partition in
                let root = try await requireRoot(catalog, transaction, step)
                guard let other = try await catalog.openPartition("q", in: root, transaction: transaction) else {
                    throw DirectoryConformanceFailure(step: step, message: "Partition 'q' must exist")
                }
                return other
            }
            try await recovering({ $0 is AbortTransaction }) {
                try await engine.withTransaction { transaction in
                    let root = try await requireRoot(catalog, transaction, step)
                    try await catalog.remove("q", in: root, transaction: transaction)
                    try await requireFailure(.staleLease, step, "leasing under a pending removal intent") {
                        _ = try await engine.leasePartition(other, transaction: transaction)
                    }
                    throw AbortTransaction()
                }
            }
            try await engine.withTransaction { transaction in
                let lease = try await engine.leasePartition(other, transaction: transaction)
                try require(lease.isActive, step, "intents of a cancelled transaction must be released")
                lease.release()
            }

            let admitted = try engine.createOwnedTransaction()
            engine.requestShutdown()
            try await requireFailure(.resourceUnavailable, step, "leasing after shutdown was requested") {
                _ = try await engine.leasePartition(other, transaction: admitted)
            }
            try await admitted.cancel()
        }
    }

    /// Directory mutations share the caller's transaction boundary.
    public func verifyTransactionalAtomicity() async throws {
        let step = "transactional atomicity"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            try await engine.withTransaction { transaction in
                _ = try await catalog.openOrInitializeRoot(transaction: transaction)
            }
            try await recovering({ $0 is AbortTransaction }) {
                try await engine.withTransaction { transaction in
                    let root = try await requireRoot(catalog, transaction, step)
                    _ = try await catalog.openOrCreateDirectory("ephemeral", in: root, transaction: transaction)
                    throw AbortTransaction()
                }
            }
            let persisted = try await engine.withTransaction { transaction -> Directory? in
                let root = try await requireRoot(catalog, transaction, step)
                return try await catalog.openDirectory("ephemeral", in: root, transaction: transaction)
            }
            try require(persisted == nil, step, "a Directory created in a failed transaction must not persist")

            // A failed recursive removal leaves the whole subtree intact.
            let subtree = try await engine.withTransaction { transaction -> ByteString in
                let root = try await requireRoot(catalog, transaction, step)
                let keep = try await catalog.openOrCreateDirectory("keep", in: root, transaction: transaction)
                _ = try await catalog.openOrCreateDirectory("leaf", in: keep, transaction: transaction)
                let dataKey = key(in: keep, suffix: [0x6B])
                try transaction.setValue([0x01], for: dataKey)
                return dataKey
            }
            try await recovering({ $0 is AbortTransaction }) {
                try await engine.withTransaction { transaction in
                    let root = try await requireRoot(catalog, transaction, step)
                    try await catalog.remove("keep", in: root, transaction: transaction)
                    throw AbortTransaction()
                }
            }
            let survived = try await engine.withTransaction { transaction -> (Directory?, ByteString?) in
                let root = try await requireRoot(catalog, transaction, step)
                let keep = try await catalog.openDirectory("keep", in: root, transaction: transaction)
                return (keep, try await transaction.getValue(for: subtree))
            }
            try require(survived.0 != nil, step, "a Directory removed in a failed transaction must survive")
            try require(survived.1 == [0x01], step, "data removed in a failed transaction must survive")
        }
    }

    // MARK: - Support

    private func withEngine<R: Sendable>(
        _ body: (Engine) async throws -> R
    ) async throws -> R {
        let engine = try await makeEngine()
        do {
            let result = try await body(engine)
            await engine.shutdown()
            return result
        } catch {
            await engine.shutdown()
            throw error
        }
    }

    private func recovering(
        _ isExpected: (any Error) -> Bool,
        _ body: () async throws -> Void
    ) async throws {
        do {
            try await body()
        } catch {
            guard isExpected(error) else {
                throw error
            }
        }
    }

    private func requireRoot(
        _ catalog: any DirectoryAccess,
        _ transaction: any TransactionReadAccess,
        _ step: String
    ) async throws -> Directory {
        guard let root = try await catalog.openRoot(transaction: transaction) else {
            throw DirectoryConformanceFailure(step: step, message: "root Directory must be initialized")
        }
        return root
    }

    private func require(
        _ condition: Bool,
        _ step: String,
        _ message: @autoclosure () -> String
    ) throws {
        guard condition else {
            throw DirectoryConformanceFailure(step: step, message: message())
        }
    }

    private func requireFailure(
        _ code: StorageError.Code,
        _ step: String,
        _ message: String,
        _ body: () async throws -> Void
    ) async throws {
        do {
            try await body()
        } catch let error as StorageError {
            guard error.code == code else {
                throw DirectoryConformanceFailure(
                    step: step,
                    message: "\(message): expected \(code) but got \(error.code) (\(error.message))"
                )
            }
            return
        } catch {
            throw DirectoryConformanceFailure(
                step: step,
                message: "\(message): expected StorageError \(code) but got \(error)"
            )
        }
        throw DirectoryConformanceFailure(
            step: step,
            message: "\(message): expected \(code) but the operation succeeded"
        )
    }

    private func key(in directory: Directory, suffix: [UInt8]) -> ByteString {
        ByteString(Array(directory.root.prefix) + suffix)
    }
}
