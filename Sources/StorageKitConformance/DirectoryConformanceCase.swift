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

    /// Operations 1–4: creation is idempotent, opening a missing child yields
    /// nil, roots are prefix-free, and addresses are validated.
    public func verifyCreateAndOpen() async throws {
        let step = "create and open"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let partitionID = try PartitionID(utf8: "tenant-1")
            let created = try await engine.withTransaction { transaction -> (Directory, Directory, Directory, Partition) in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                let a = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                let b = try await catalog.openOrCreateDirectory("b", in: a, transaction: transaction)
                let partition = try await catalog.openOrCreatePartition(partitionID, in: b, transaction: transaction)
                return (root, a, b, partition)
            }
            let (root, a, b, partition) = created
            try require(a.address == StorageAddress([.directory("a")]), step, "child address must extend the parent address")
            try require(b.address == StorageAddress([.directory("a"), .directory("b")]), step, "nested address must extend the parent address")
            try require(partition.id == partitionID, step, "Partition must carry its identifier")
            try require(partition.hasConsistentAddress, step, "Partition address must end with its own identifier step")
            try require(partition.root.address.parent == b.address, step, "Partition root must be addressed under its parent")
            let prefixes = [root.root.prefix, a.root.prefix, b.root.prefix, partition.root.root.prefix]
            for (index, prefix) in prefixes.enumerated() {
                for (otherIndex, other) in prefixes.enumerated() where otherIndex != index {
                    try require(!other.starts(with: prefix), step, "root prefixes must be prefix-free")
                }
            }

            let reopened = try await engine.withTransaction { transaction -> (Directory?, Directory?, Partition?, Directory?, Directory?, Partition?, Directory, Partition) in
                let root = try await requireRoot(catalog, transaction, step)
                let openedA = try await catalog.openDirectory("a", in: root, transaction: transaction)
                let openedB = try await catalog.openDirectory(at: DirectoryPath("a", "b"), in: root, transaction: transaction)
                let openedPartition = try await catalog.openPartition(partitionID, in: b, transaction: transaction)
                let walked = try await catalog.openDirectory(at: partition.root.address, transaction: transaction)
                let missingDirectory = try await catalog.openDirectory("zzz", in: root, transaction: transaction)
                let missingPartition = try await catalog.openPartition(PartitionID(utf8: "missing"), in: b, transaction: transaction)
                let againA = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                let againPartition = try await catalog.openOrCreatePartition(partitionID, in: b, transaction: transaction)
                return (openedA, openedB, openedPartition, walked, missingDirectory, missingPartition, againA, againPartition)
            }
            try require(reopened.0 == a, step, "openDirectory must return the created Directory")
            try require(reopened.1 == b, step, "openDirectory(at:) must walk the path to the same Directory")
            try require(reopened.2 == partition, step, "openPartition must return the created Partition")
            try require(reopened.3 == partition.root, step, "openDirectory(at:) must resolve a Partition address to its root")
            try require(reopened.4 == nil, step, "opening a missing Directory must return nil")
            try require(reopened.5 == nil, step, "opening a missing Partition must return nil")
            try require(reopened.6 == a, step, "openOrCreateDirectory must be idempotent")
            try require(reopened.7 == partition, step, "openOrCreatePartition must be idempotent")

            try await recovering({ $0 is AbortTransaction }) {
                try await engine.withTransaction { transaction in
                    let root = try await requireRoot(catalog, transaction, step)
                    try await verifyAddressValidation(catalog, root, transaction, step)
                    throw AbortTransaction()
                }
            }
        }
    }

    private func verifyAddressValidation(
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
        var deep = root
        for level in 0..<DirectoryLimits.maximumDepth {
            deep = try await catalog.openOrCreateDirectory("d\(level)", in: deep, transaction: transaction)
        }
        try await requireFailure(.invalidDirectoryAddress, step, "Directory depth over the limit") {
            _ = try await catalog.openOrCreateDirectory("overflow", in: deep, transaction: transaction)
        }
    }

    /// Operations 5–6: listings are ordered, paginated, kind-separated, and
    /// bounded by `DirectoryLimits.maximumListLimit`.
    public func verifyListing() async throws {
        let step = "listing"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let ids = try ["3", "1", "2"].map { try PartitionID(utf8: $0) }
            let sortedIDs = ids.sorted()
            try await engine.withTransaction { transaction in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                for name in ["c", "a", "b"] {
                    _ = try await catalog.openOrCreateDirectory(name, in: root, transaction: transaction)
                }
                for id in ids {
                    _ = try await catalog.openOrCreatePartition(id, in: root, transaction: transaction)
                }
            }
            let listed = try await engine.withTransaction { transaction -> ([String], [String], [String], [PartitionID], [PartitionID], [String], [PartitionID]) in
                let root = try await requireRoot(catalog, transaction, step)
                let all = try await catalog.listDirectories(in: root, after: nil, limit: 10, transaction: transaction)
                let page1 = try await catalog.listDirectories(in: root, after: nil, limit: 2, transaction: transaction)
                let page2 = try await catalog.listDirectories(in: root, after: page1.last, limit: 2, transaction: transaction)
                let partitions = try await catalog.listPartitions(in: root, after: nil, limit: 10, transaction: transaction)
                let partitionPage = try await catalog.listPartitions(in: root, after: sortedIDs[0], limit: 1, transaction: transaction)
                guard let child = try await catalog.openDirectory("a", in: root, transaction: transaction) else {
                    throw DirectoryConformanceFailure(step: step, message: "Directory 'a' must exist")
                }
                let childDirectories = try await catalog.listDirectories(in: child, after: nil, limit: 10, transaction: transaction)
                let childPartitions = try await catalog.listPartitions(in: child, after: nil, limit: 10, transaction: transaction)
                return (all, page1, page2, partitions, partitionPage, childDirectories, childPartitions)
            }
            try require(listed.0 == ["a", "b", "c"], step, "Directories must list in byte order, got \(listed.0)")
            try require(listed.1 == ["a", "b"], step, "first page must honor the limit, got \(listed.1)")
            try require(listed.2 == ["c"], step, "second page must continue after the cursor, got \(listed.2)")
            try require(listed.3 == sortedIDs, step, "Partitions must list in byte order")
            try require(listed.4 == [sortedIDs[1]], step, "Partition page must continue after the cursor")
            try require(listed.5.isEmpty && listed.6.isEmpty, step, "child Directory listings must not include the parent's children")

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await requireFailure(.invalidOperation, step, "listing limit 0") {
                    _ = try await catalog.listDirectories(in: root, after: nil, limit: 0, transaction: transaction)
                }
                try await requireFailure(.invalidOperation, step, "listing limit above the maximum") {
                    _ = try await catalog.listPartitions(in: root, after: nil, limit: DirectoryLimits.maximumListLimit + 1, transaction: transaction)
                }
            }
        }
    }

    /// Operation 7: move preserves the root prefix and subtree, and rejects
    /// missing, Partition, self-subtree, and occupied targets.
    public func verifyMove() async throws {
        let step = "move"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let partitionID = try PartitionID(utf8: "p")
            let setup = try await engine.withTransaction { transaction -> (Directory, Directory, Directory, Partition, ByteString) in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                let a = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                let b = try await catalog.openOrCreateDirectory("b", in: root, transaction: transaction)
                let child = try await catalog.openOrCreateDirectory("child", in: a, transaction: transaction)
                let partition = try await catalog.openOrCreatePartition(partitionID, in: child, transaction: transaction)
                _ = try await catalog.openOrCreateDirectory("other", in: b, transaction: transaction)
                let dataKey = key(in: child, suffix: [0x6B])
                try transaction.setValue([0x01], for: dataKey)
                return (a, b, child, partition, dataKey)
            }
            let (a, b, child, partition, dataKey) = setup

            let moved = try await engine.withTransaction { transaction in
                try await catalog.moveChild(.directory("child"), in: a, to: .directory("renamed"), in: b, transaction: transaction)
            }
            try require(moved.root.prefix == child.root.prefix, step, "move must preserve the root prefix")
            try require(moved.address == b.address.appending(.directory("renamed")), step, "moved Directory must carry the destination address")

            let after = try await engine.withTransaction { transaction -> (Directory?, Directory?, Partition?, ByteString?) in
                let old = try await catalog.openDirectory("child", in: a, transaction: transaction)
                let new = try await catalog.openDirectory("renamed", in: b, transaction: transaction)
                let movedPartition = try await catalog.openPartition(partitionID, in: moved, transaction: transaction)
                let data = try await transaction.getValue(for: dataKey)
                return (old, new, movedPartition, data)
            }
            try require(after.0 == nil, step, "source child must no longer resolve after move")
            try require(after.1 == moved, step, "destination child must resolve to the moved Directory")
            try require(after.2?.root.root == partition.root.root, step, "descendants must keep their root prefixes after move")
            try require(after.2?.root.address == moved.address.appending(.partition(partitionID)), step, "descendants must be re-addressed under the destination")
            try require(after.3 == [0x01], step, "data under the moved Directory must remain readable")

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await requireFailure(.keyNotFound, step, "moving a missing child") {
                    _ = try await catalog.moveChild(.directory("missing"), in: a, to: .directory("x"), in: b, transaction: transaction)
                }
                try await requireFailure(.unsupportedOperation, step, "moving a Partition") {
                    _ = try await catalog.moveChild(.partition(partitionID), in: moved, to: .partition(partitionID), in: a, transaction: transaction)
                }
                try await requireFailure(.invalidDirectoryAddress, step, "moving a Directory into its own subtree") {
                    _ = try await catalog.moveChild(.directory("a"), in: root, to: .directory("x"), in: a, transaction: transaction)
                }
                try await requireFailure(.invalidOperation, step, "moving onto an existing target") {
                    _ = try await catalog.moveChild(.directory("a"), in: root, to: .directory("other"), in: b, transaction: transaction)
                }
            }
        }
    }

    /// Operation 8: removal requires an existing, empty, unleased child and
    /// never reuses a released root prefix.
    public func verifyRemove() async throws {
        let step = "remove"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let partitionID = try PartitionID(utf8: "p")
            let setup = try await engine.withTransaction { transaction -> (Directory, Directory, Partition, ByteString) in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                let a = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                let child = try await catalog.openOrCreateDirectory("child", in: a, transaction: transaction)
                let partition = try await catalog.openOrCreatePartition(partitionID, in: a, transaction: transaction)
                let dataKey = key(in: partition.root, suffix: [0x6B])
                try transaction.setValue([0x01], for: dataKey)
                return (a, child, partition, dataKey)
            }
            let (a, child, partition, dataKey) = setup

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await requireFailure(.keyNotFound, step, "removing a missing Directory") {
                    try await catalog.removeChild(.directory("missing"), in: root, transaction: transaction)
                }
                try await requireFailure(.keyNotFound, step, "removing a missing Partition") {
                    try await catalog.removeChild(.partition(PartitionID(utf8: "missing")), in: root, transaction: transaction)
                }
                try await requireFailure(.directoryNotEmpty, step, "removing a Directory with children") {
                    try await catalog.removeChild(.directory("a"), in: root, transaction: transaction)
                }
                try await requireFailure(.directoryNotEmpty, step, "removing a Partition with data") {
                    try await catalog.removeChild(.partition(partitionID), in: a, transaction: transaction)
                }
            }

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try transaction.clear(key: dataKey)
                try await catalog.removeChild(.partition(partitionID), in: a, transaction: transaction)
                try await catalog.removeChild(.directory("child"), in: a, transaction: transaction)
                try await catalog.removeChild(.directory("a"), in: root, transaction: transaction)
            }

            let recreated = try await engine.withTransaction { transaction -> (Directory?, Directory) in
                let root = try await requireRoot(catalog, transaction, step)
                let gone = try await catalog.openDirectory("a", in: root, transaction: transaction)
                let fresh = try await catalog.openOrCreateDirectory("a", in: root, transaction: transaction)
                return (gone, fresh)
            }
            try require(recreated.0 == nil, step, "removed Directory must not resolve")
            try require(recreated.1.root.prefix != a.root.prefix, step, "a released root prefix must not be reused")
            try require(recreated.1.root.prefix != child.root.prefix && recreated.1.root.prefix != partition.root.root.prefix, step, "a released descendant prefix must not be reused")
        }
    }

    /// Every operation rejects participants from another engine before I/O.
    public func verifyDomainMismatch() async throws {
        let step = "domain mismatch"
        try await withEngine { first in
            try await withEngine { second in
                let partitionID = try PartitionID(utf8: "p")
                let created = try await first.withTransaction { transaction -> (Directory, Partition) in
                    let root = try await first.directoryAccess.openOrInitializeRoot(transaction: transaction)
                    let partition = try await first.directoryAccess.openOrCreatePartition(partitionID, in: root, transaction: transaction)
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
                        _ = try await second.directoryAccess.listDirectories(in: foreignRoot, after: nil, limit: 1, transaction: transaction)
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

    /// Leases bound data access to the Partition, block removal while active,
    /// detect stale Partition values, and stop being issued after shutdown.
    public func verifyLeaseLifecycle() async throws {
        let step = "lease lifecycle"
        try await withEngine { engine in
            let catalog = engine.directoryAccess
            let partitionID = try PartitionID(utf8: "p")
            let otherID = try PartitionID(utf8: "q")
            let nestedID = try PartitionID(utf8: "nested")
            let created = try await engine.withTransaction { transaction -> (Partition, Partition) in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                _ = try await catalog.openOrCreatePartition(otherID, in: root, transaction: transaction)
                let parent = try await catalog.openOrCreateDirectory("parent", in: root, transaction: transaction)
                let nested = try await catalog.openOrCreatePartition(nestedID, in: parent, transaction: transaction)
                let partition = try await catalog.openOrCreatePartition(partitionID, in: root, transaction: transaction)
                return (partition, nested)
            }
            let (partition, nested) = created
            let dataKey = key(in: partition.root, suffix: [0x6B])

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                let lease = try await engine.leasePartition(partition, transaction: transaction)
                try require(lease.isActive, step, "a fresh lease must be active")
                try await requireFailure(.directoryLeased, step, "removing a leased Partition") {
                    try await catalog.removeChild(.partition(partitionID), in: root, transaction: transaction)
                }
                let nestedLease = try await engine.leasePartition(nested, transaction: transaction)
                try await requireFailure(.directoryLeased, step, "moving the ancestor of a leased Partition") {
                    _ = try await catalog.moveChild(.directory("parent"), in: root, to: .directory("moved"), in: root, transaction: transaction)
                }
                nestedLease.release()
                let movedParent = try await catalog.moveChild(.directory("parent"), in: root, to: .directory("moved"), in: root, transaction: transaction)
                try require(movedParent.address == StorageAddress([.directory("moved")]), step, "ancestor move must succeed once the lease is released")
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

            try await engine.withTransaction { transaction in
                let root = try await requireRoot(catalog, transaction, step)
                try await requireFailure(.directoryNotEmpty, step, "removing a Partition with data after lease release") {
                    try await catalog.removeChild(.partition(partitionID), in: root, transaction: transaction)
                }
                let lease = try await engine.leasePartition(partition, transaction: transaction)
                try await lease.withWriteAccess(transaction) { access in
                    try access.clear(key: dataKey)
                }
                lease.release()
                try await catalog.removeChild(.partition(partitionID), in: root, transaction: transaction)
                try await requireFailure(.staleLease, step, "leasing a Partition whose removal is pending in the same transaction") {
                    _ = try await engine.leasePartition(partition, transaction: transaction)
                }
            }

            try await engine.withTransaction { transaction in
                try await requireFailure(.staleLease, step, "leasing a removed Partition") {
                    _ = try await engine.leasePartition(partition, transaction: transaction)
                }
                let inconsistent = Partition(id: otherID, root: partition.root)
                try await requireFailure(.invalidDirectoryAddress, step, "leasing a Partition whose address does not end with its identifier") {
                    _ = try await engine.leasePartition(inconsistent, transaction: transaction)
                }
            }

            let other = try await engine.withTransaction { transaction -> Partition in
                let root = try await requireRoot(catalog, transaction, step)
                guard let other = try await catalog.openPartition(otherID, in: root, transaction: transaction) else {
                    throw DirectoryConformanceFailure(step: step, message: "Partition 'q' must exist")
                }
                return other
            }
            try await recovering({ $0 is AbortTransaction }) {
                try await engine.withTransaction { transaction in
                    let root = try await requireRoot(catalog, transaction, step)
                    try await catalog.removeChild(.partition(otherID), in: root, transaction: transaction)
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
