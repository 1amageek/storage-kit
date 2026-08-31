import DatabaseTypes
@testable import FDBStorage
import Foundation
import FoundationDB
import StorageKit
import StorageKitConformance
import Synchronization
import Testing

/// Runs the shared Directory conformance case over the native FoundationDB
/// Directory Layer and proves the FoundationDB-specific mapping rules of
/// SPEC §7.3: native path components, the native `partition` layer, and no
/// custom layer types of the adapter's own.
///
/// Every engine receives its own root path below a per-test base path, so each
/// step starts from an absent root on a shared cluster; the base path and every
/// root below it are removed after the step, leaving only the shared
/// `storage-kit-conformance` parent node and the native allocator counters
/// behind.
@Suite("FoundationDB Directory conformance", .serialized)
struct FDBDirectoryConformanceTests {
    private final class RootPathAllocator: Sendable {
        let base: [String]
        private let issued = Mutex(0)

        init() {
            base = ["storage-kit-conformance", UUID().uuidString.lowercased()]
        }

        func next() -> [String] {
            let index = issued.withLock { count -> Int in
                count += 1
                return count
            }
            return base + ["engine-\(index)"]
        }
    }

    /// Root path an engine was configured with.
    ///
    /// The adapter is the sole owner of the mapping from a configured root path
    /// to native paths, so a test reads the path back from the adapter instead
    /// of restating it.
    private static func rootPath(of engine: FDBStorageEngine) throws -> [String] {
        guard let access = engine.directoryAccess as? FDBDirectoryAccess else {
            throw StorageError(
                code: .invalidOperation,
                operation: .open,
                backend: .foundationDB,
                message: "FoundationDB engine must expose the native Directory access"
            )
        }
        return access.rootPath
    }

    // MARK: - Shared conformance

    // `verifyForeignRootRejection` is not run here, and the omission is a
    // property of this backend rather than a gap in the fixture. A key-value
    // root shares one flat keyspace with whatever wrote to it first, so
    // initializing over existing keys would allocate a Directory on top of
    // them. The native Directory Layer allocates every prefix and never
    // returns one already in use, so no StorageKit write can reach foreign
    // bytes and the state that step produces does not exist here.

    @Test("Root initialization", .timeLimit(.minutes(1)))
    func rootInitialization() async throws {
        try await Self.withConformance { try await $0.verifyRootInitialization() }
    }

    @Test("Create and open", .timeLimit(.minutes(1)))
    func createAndOpen() async throws {
        try await Self.withConformance { try await $0.verifyCreateAndOpen() }
    }

    @Test("Listing", .timeLimit(.minutes(1)))
    func listing() async throws {
        try await Self.withConformance { try await $0.verifyListing() }
    }

    @Test("Partition contiguity", .timeLimit(.minutes(1)))
    func partitionContiguity() async throws {
        try await Self.withConformance { try await $0.verifyPartitionContiguity() }
    }

    @Test("Move", .timeLimit(.minutes(1)))
    func move() async throws {
        try await Self.withConformance { try await $0.verifyMove() }
    }

    @Test("Remove", .timeLimit(.minutes(1)))
    func remove() async throws {
        try await Self.withConformance { try await $0.verifyRemove() }
    }

    @Test("Stale parent rejection", .timeLimit(.minutes(1)))
    func staleParentRejection() async throws {
        try await Self.withConformance { try await $0.verifyStaleParentRejection() }
    }

    @Test("Recreated parent positioning", .timeLimit(.minutes(1)))
    func recreatedParentPositioning() async throws {
        try await Self.withConformance { try await $0.verifyRecreatedParentPositioning() }
    }

    @Test("Domain mismatch", .timeLimit(.minutes(1)))
    func domainMismatch() async throws {
        try await Self.withConformance { try await $0.verifyDomainMismatch() }
    }

    @Test("Lease lifecycle", .timeLimit(.minutes(1)))
    func leaseLifecycle() async throws {
        try await Self.withConformance { try await $0.verifyLeaseLifecycle() }
    }

    @Test("Lease staleness detection", .timeLimit(.minutes(1)))
    func leaseStalenessDetection() async throws {
        try await Self.withConformance { try await $0.verifyLeaseStalenessDetection() }
    }

    @Test("Transactional atomicity", .timeLimit(.minutes(1)))
    func transactionalAtomicity() async throws {
        try await Self.withConformance { try await $0.verifyTransactionalAtomicity() }
    }

    // MARK: - Root scoping

    /// The storage root of an engine is the node at its configured root path,
    /// so the bootstrap decision of SPEC §8.7 asks whether that node exists
    /// and never whether the cluster is empty.
    ///
    /// A cluster always holds data belonging to no single root: other roots and
    /// the native allocator counters. Counting that as this root's data would
    /// reject every new root on a cluster already in use.
    @Test("A root initializes on a cluster that already holds other data", .timeLimit(.minutes(1)))
    func rootInitializesOnANonemptyCluster() async throws {
        let roots = RootPathAllocator()
        let occupied = roots.next()
        let fresh = roots.next()
        try await Self.withEngine(rootPath: occupied, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            try await engine.withTransaction { transaction in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                _ = try await catalog.openOrCreateDirectory("occupant", in: root, transaction: transaction)
            }

            let second = try await FDBStorageEngine(configuration: .init(rootPath: fresh))
            do {
                let before = try await second.withTransaction { transaction in
                    try await second.directoryAccess.openRoot(transaction: transaction)
                }
                #expect(before == nil, "a fresh root must read as uninitialized, not as a rejected layout")
                let initialized = try await second.withTransaction { transaction in
                    try await second.directoryAccess.openOrInitializeRoot(transaction: transaction)
                }
                #expect(initialized.address == .root)

                // Initializing one root leaves every other root untouched.
                let names = try await engine.withTransaction { transaction -> [String] in
                    let root = try #require(try await catalog.openRoot(transaction: transaction))
                    return try await catalog.listChildren(
                        in: root, after: nil, limit: 10, transaction: transaction
                    ).map(\.name)
                }
                #expect(names == ["occupant"])
            } catch {
                await second.shutdown()
                throw error
            }
            await second.shutdown()
        }
    }

    /// One root's bootstrap state never decides another root's (FD-1).
    ///
    /// Existence is asked of the engine's own node at `rootPath`, so an
    /// initialized root leaves its siblings uninitialized, and initializing a
    /// sibling leaves the first one open.
    @Test("Sibling roots are independent", .timeLimit(.minutes(1)))
    func siblingRootsAreIndependent() async throws {
        let roots = RootPathAllocator()
        let first = roots.next()
        let second = roots.next()
        try await Self.withEngine(rootPath: first, base: roots.base) { engine in
            let initialized = try await engine.withTransaction { transaction in
                try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            }
            #expect(initialized.address == .root)

            let sibling = try await FDBStorageEngine(configuration: .init(rootPath: second))
            do {
                let before = try await sibling.withTransaction { transaction in
                    try await sibling.directoryAccess.openRoot(transaction: transaction)
                }
                #expect(before == nil, "an initialized root must not initialize its sibling")
                let siblingRoot = try await sibling.withTransaction { transaction in
                    try await sibling.directoryAccess.openOrInitializeRoot(transaction: transaction)
                }
                #expect(siblingRoot.address == .root)
                #expect(siblingRoot.keyspacePrefix != initialized.keyspacePrefix)
                let reopened = try await engine.withTransaction { transaction in
                    try await engine.directoryAccess.openRoot(transaction: transaction)
                }
                #expect(reopened == initialized, "a sibling root must not disturb this root")
            } catch {
                await sibling.shutdown()
                throw error
            }
            await sibling.shutdown()
        }
    }

    /// A node at the configured root path is a storage root only when its
    /// reserved slot holds this catalog's witness (FD-1).
    ///
    /// The native layer creates the ancestors of a path as ordinary
    /// empty-layer Directories, so an engine configured below this path brings
    /// the node into existence on its way down. A bootstrap that asked only
    /// whether the node exists would adopt that node, and the two engines
    /// would then allocate inside one another.
    @Test("An unwitnessed node at the root path is not a storage root", .timeLimit(.minutes(1)))
    func unwitnessedRootNodeIsRejected() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let layer = DirectoryLayer(database: try FDBClient.openDatabase())
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            _ = try await layer.createOrOpen(path: rootPath + ["descendant"], type: nil)
            let exists = try await layer.exists(path: rootPath)
            #expect(exists, "the ancestor walk must have created the node at the root path")

            await Self.expectFailure(.incompatibleStorageLayout, "openRoot on an unwitnessed node") {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openRoot(transaction: transaction)
                }
            }
            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openOrInitializeRoot on an unwitnessed node"
            ) {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openOrInitializeRoot(transaction: transaction)
                }
            }
        }
    }

    /// Storage roots do not nest, in either order of creation (FD-1a).
    ///
    /// Whichever engine arrives second is refused, so the state where an outer
    /// root lists an inner root among its own children, and removes it
    /// recursively, is never reachable.
    @Test("Storage roots do not nest", .timeLimit(.minutes(1)))
    func storageRootsDoNotNest() async throws {
        let innerFirst = RootPathAllocator()
        let outerPath = innerFirst.next()
        let innerPath = outerPath + ["inner"]
        try await Self.withEngine(rootPath: innerPath, base: innerFirst.base) { engine in
            try await engine.withTransaction { transaction in
                _ = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            }
            // The inner root's own creation left an unwitnessed node on the
            // outer path, which FD-1 refuses.
            let outer = try await FDBStorageEngine(configuration: .init(rootPath: outerPath))
            await Self.expectFailure(.incompatibleStorageLayout, "openRoot above an inner root") {
                try await outer.withTransaction { transaction in
                    _ = try await outer.directoryAccess.openRoot(transaction: transaction)
                }
            }
            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openOrInitializeRoot above an inner root"
            ) {
                try await outer.withTransaction { transaction in
                    _ = try await outer.directoryAccess.openOrInitializeRoot(transaction: transaction)
                }
            }
            await outer.shutdown()
        }

        let outerFirst = RootPathAllocator()
        let established = outerFirst.next()
        let below = established + ["inner"]
        try await Self.withEngine(rootPath: established, base: outerFirst.base) { engine in
            try await engine.withTransaction { transaction in
                _ = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            }
            // The node below is absent, so the ancestor walk is what refuses it.
            let inner = try await FDBStorageEngine(configuration: .init(rootPath: below))
            await Self.expectFailure(.incompatibleStorageLayout, "openRoot below an outer root") {
                try await inner.withTransaction { transaction in
                    _ = try await inner.directoryAccess.openRoot(transaction: transaction)
                }
            }
            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openOrInitializeRoot below an outer root"
            ) {
                try await inner.withTransaction { transaction in
                    _ = try await inner.directoryAccess.openOrInitializeRoot(transaction: transaction)
                }
            }
            await inner.shutdown()

            // The refused engine wrote nothing, so the outer root still holds
            // only what it created itself.
            let names = try await engine.withTransaction { transaction -> [String] in
                let root = try #require(try await engine.directoryAccess.openRoot(transaction: transaction))
                return try await engine.directoryAccess.listChildren(
                    in: root, after: nil, limit: 10, transaction: transaction
                ).map(\.name)
            }
            #expect(names.isEmpty)
        }
    }

    /// No layer value is reserved, so a caller tag that happens to spell this
    /// catalog's own name round-trips like any other tag (FD-1).
    ///
    /// The witness of an initialized root lives in the slot the native layer
    /// reserves in that node's own metadata, not in a layer value, so SPEC §4
    /// holds unchanged here: every tag other than `partition` is
    /// application-opaque, is stored and read back exactly, and names a child
    /// rather than a storage root.
    @Test("No layer tag is reserved against caller tags", .timeLimit(.minutes(1)))
    func noLayerTagIsReservedAgainstCallerTags() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let database = try FDBClient.openDatabase()
        let layer = DirectoryLayer(database: database)
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            let tag = try LayerTag(utf8: "storage-kit")
            try await engine.withTransaction { transaction in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                let created = try await catalog.openOrCreate(
                    "child",
                    layer: tag,
                    in: root,
                    transaction: transaction
                )
                #expect(created.layer == tag)
                let reopened = try await catalog.open(
                    "child",
                    expecting: tag,
                    in: root,
                    transaction: transaction
                )
                #expect(reopened?.layer == tag)
                let entries = try await catalog.listChildren(
                    in: root,
                    after: nil,
                    limit: 10,
                    transaction: transaction
                )
                #expect(entries.map(\.name) == ["child"])
                #expect(entries.first?.layer == tag)
            }

            // The tag reaches the native layer verbatim, and the node carrying
            // it is a child: it holds no witness of its own.
            let child = try await layer.open(path: rootPath + ["child"])
            #expect(child.type == .custom("storage-kit"))
            let observation = try await database.withTransaction { transaction in
                try await layer.inspectWitness(
                    try Self.storageRootWitness(),
                    at: rootPath + ["child"],
                    transaction: transaction
                )
            }
            #expect(observation.witness == .absent)
        }
    }

    /// The witness this adapter records on, and looks for on, its own root
    /// node.
    private static func storageRootWitness() throws -> DirectoryNodeWitness {
        try DirectoryNodeWitness(identifier: FDBDirectoryLayout.rootWitnessIdentifier)
    }

    /// The witness of a foreign owner, which is the only way another writer
    /// legitimately reaches a node's slot.
    private static func foreignWitness() throws -> DirectoryNodeWitness {
        try DirectoryNodeWitness(identifier: ByteString(FoundationDB.Tuple("other-owner").pack()))
    }

    /// The witness is adjudicated by what it holds, so a node another owner
    /// witnessed is corruption in this catalog's root rather than an
    /// initialized root (FD-1).
    ///
    /// The native layer never hands out a prefix already in use, so no
    /// StorageKit write reaches a node someone else owns. Adopting whatever a
    /// different owner left would allocate this catalog's nodes inside a
    /// subtree it does not own.
    @Test("A conflicting root witness is rejected", .timeLimit(.minutes(1)))
    func conflictingRootWitnessIsRejected() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let database = try FDBClient.openDatabase()
        let layer = DirectoryLayer(database: database)
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            try await database.withTransaction { transaction in
                _ = try await layer.create(
                    path: rootPath,
                    type: nil,
                    recording: try Self.foreignWitness(),
                    transaction: transaction
                )
            }

            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openRoot over a conflicting witness"
            ) {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openRoot(transaction: transaction)
                }
            }
            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openOrInitializeRoot over a conflicting witness"
            ) {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openOrInitializeRoot(transaction: transaction)
                }
            }
        }
    }

    /// A proper ancestor of the configured root path carrying a witness this
    /// catalog did not write is corruption in a node this adapter would
    /// otherwise write straight through, so the path is refused rather than
    /// adopted around it (FD-1a).
    @Test("A conflicting ancestor witness is rejected", .timeLimit(.minutes(1)))
    func conflictingAncestorWitnessIsRejected() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let ancestor = Array(rootPath.dropLast())
        let database = try FDBClient.openDatabase()
        let layer = DirectoryLayer(database: database)
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            try await database.withTransaction { transaction in
                _ = try await layer.create(
                    path: ancestor,
                    type: nil,
                    recording: try Self.foreignWitness(),
                    transaction: transaction
                )
            }

            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openRoot below a conflicting ancestor witness"
            ) {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openRoot(transaction: transaction)
                }
            }
            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openOrInitializeRoot below a conflicting ancestor witness"
            ) {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openOrInitializeRoot(transaction: transaction)
                }
            }
        }
    }

    /// Clearing the root's whole content range leaves the root openable
    /// (SPEC §8.7).
    ///
    /// The witness lives in the node's own metadata, so it is disjoint from
    /// every key and range a caller can derive from `Directory.root`, a
    /// `Subspace`, a `StorageAddress`, a `Partition`, or a bound access. The
    /// range cleared here is the strongest legitimate erasure available
    /// through this adapter's own keyspace: everything sharing the root's
    /// content prefix.
    @Test("Clearing the root content preserves bootstrap", .timeLimit(.minutes(1)))
    func clearingRootContentPreservesBootstrap() async throws {
        let roots = RootPathAllocator()
        try await Self.withEngine(rootPath: roots.next(), base: roots.base) { engine in
            let catalog = engine.directoryAccess
            let prefix = try await engine.withTransaction { transaction -> ByteString in
                try await catalog.openOrInitializeRoot(transaction: transaction).keyspacePrefix
            }
            let dataKey = prefix.appending(0x6B)
            try await engine.withTransaction { transaction in
                try transaction.setValue(ByteString([0x01]), for: dataKey)
            }

            try await engine.withTransaction { transaction in
                try transaction.clearRange(beginKey: prefix, endKey: try strinc(prefix))
            }

            let survivors = try await engine.withTransaction { transaction -> ByteString? in
                try await transaction.getValue(for: dataKey)
            }
            #expect(survivors == nil, "the caller's own data must be gone")

            let reopened = try await engine.withTransaction { transaction -> Directory? in
                try await catalog.openRoot(transaction: transaction)
            }
            #expect(reopened?.keyspacePrefix == prefix)
        }
    }

    /// The record this adapter used to write inside the root node's own
    /// content prefix has no meaning any more: bootstrap reads the node's
    /// witness slot and nothing else, so a store still holding the old bytes
    /// is refused rather than migrated (FD-1).
    ///
    /// The superseded coordinate is spelled out here rather than derived from
    /// production code, because production code no longer knows it.
    @Test("A superseded root record is not adopted", .timeLimit(.minutes(1)))
    func supersededRootRecordIsNotAdopted() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let layer = DirectoryLayer(database: try FDBClient.openDatabase())
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            let node = try await layer.createOrOpen(path: rootPath, type: nil)
            // The superseded shape: an untyped node at the root path carrying
            // the old record at `rootPrefix + 0xFE + 0x72`.
            let superseded = ByteString(node.prefix).appending(0xFE).appending(0x72)
            try await engine.withTransaction { transaction in
                try transaction.setValue(
                    StorageKit.Tuple("storage-kit").pack(),
                    for: superseded
                )
            }

            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openRoot over a superseded record"
            ) {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openRoot(transaction: transaction)
                }
            }
            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openOrInitializeRoot over a superseded record"
            ) {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openOrInitializeRoot(transaction: transaction)
                }
            }
        }
    }

    /// The node and its witness commit or roll back together, so an aborted
    /// initialization leaves neither behind (FD-1).
    @Test("An aborted initialization leaves no root", .timeLimit(.minutes(1)))
    func abortedInitializationLeavesNoRoot() async throws {
        struct Abort: Error {}

        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let database = try FDBClient.openDatabase()
        let layer = DirectoryLayer(database: database)
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            do {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openOrInitializeRoot(transaction: transaction)
                    throw Abort()
                }
                Issue.record("the aborted transaction must not commit")
            } catch is Abort {
                // Expected.
            }

            let root = try await engine.withTransaction { transaction -> Directory? in
                try await catalog.openRoot(transaction: transaction)
            }
            #expect(root == nil, "the rolled-back root must not be observable")

            let exists = try await layer.exists(path: rootPath)
            #expect(!exists, "the rolled-back node must not exist natively")

            // Initialization succeeds afterwards, so the abort left nothing
            // that would refuse the path.
            let initialized = try await engine.withTransaction { transaction -> Directory in
                try await catalog.openOrInitializeRoot(transaction: transaction)
            }
            #expect(initialized.address == .root)
        }
    }

    // MARK: - Native mapping

    /// A StorageKit node is one native node with the same name, a plain
    /// Directory stores no native layer value, a Partition stores the native
    /// `partition` layer, and a Partition may contain a Partition.
    @Test("Native nodes carry StorageKit names and layers", .timeLimit(.minutes(1)))
    func nativeNodesCarryStorageKitNamesAndLayers() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            let index = try LayerTag(utf8: "index")
            try await engine.withTransaction { transaction in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                _ = try await catalog.openOrCreateDirectory("alpha", in: root, transaction: transaction)
                _ = try await catalog.openOrCreate("labelled", layer: index, in: root, transaction: transaction)
                let tenant = try await catalog.openOrCreatePartition("tenant", in: root, transaction: transaction)
                let inner = try await catalog.openOrCreateDirectory(
                    "inner",
                    in: tenant.root,
                    transaction: transaction
                )
                _ = try await catalog.openOrCreatePartition("nested", in: inner, transaction: transaction)
            }

            let layer = DirectoryLayer(database: try FDBClient.openDatabase())
            let names = try await layer.list(path: rootPath).sorted()
            #expect(names == ["alpha", "labelled", "tenant"])
            let alpha = try await layer.open(path: rootPath + ["alpha"])
            #expect(alpha.type == nil)
            let labelled = try await layer.open(path: rootPath + ["labelled"])
            #expect(labelled.type == .custom("index"))
            let tenant = try await layer.open(path: rootPath + ["tenant"])
            #expect(tenant.type == .partition)
            // The nested Partition resolves through its parent Partition, which
            // proves the native layer accepts Partition-in-Partition creation.
            let nested = try await layer.open(path: rootPath + ["tenant", "inner", "nested"])
            #expect(nested.type == .partition)
            #expect(ByteString(nested.prefix).starts(with: ByteString(tenant.prefix)))

            let reopened = try await engine.withTransaction { transaction -> (LayerTag?, LayerTag?) in
                let root = try #require(try await catalog.openRoot(transaction: transaction))
                let labelled = try await catalog.open(
                    "labelled",
                    expecting: index,
                    in: root,
                    transaction: transaction
                )
                let alpha = try await catalog.open("alpha", expecting: nil, in: root, transaction: transaction)
                return (labelled?.layer, alpha?.layer)
            }
            #expect(reopened.0 == index)
            #expect(reopened.1 == .default)
        }
    }

    /// A layer tag other than the empty tag and `partition` is
    /// application-opaque (SPEC §4), and no backend weakens that (SPEC F-03),
    /// so bytes that are not UTF-8 are stored and read back exactly.
    @Test("Layer tag that is not UTF-8 round-trips", .timeLimit(.minutes(1)))
    func layerTagThatIsNotUTF8RoundTrips() async throws {
        let roots = RootPathAllocator()
        try await Self.withEngine(rootPath: roots.next(), base: roots.base) { engine in
            let catalog = engine.directoryAccess
            let opaque = try LayerTag(ByteString([0xFF, 0xFE, 0x00, 0x80]))
            try await engine.withTransaction { transaction in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                let created = try await catalog.openOrCreate(
                    "x",
                    layer: opaque,
                    in: root,
                    transaction: transaction
                )
                #expect(created.layer == opaque)

                // Every open verifies the tag, so the opaque tag must both
                // match itself and be distinguishable from the default tag.
                let reopened = try await catalog.open(
                    "x",
                    expecting: opaque,
                    in: root,
                    transaction: transaction
                )
                #expect(reopened?.layer == opaque)
                await Self.expectFailure(.directoryLayerMismatch, "open with the default tag") {
                    _ = try await catalog.open("x", expecting: .default, in: root, transaction: transaction)
                }

                let entries = try await catalog.listChildren(
                    in: root,
                    after: nil,
                    limit: 10,
                    transaction: transaction
                )
                #expect(entries.map(\.name) == ["x"])
                #expect(entries.first?.layer == opaque)
            }
        }
    }

    /// A native node created outside StorageKit keeps its own layer value, so a
    /// typed open reports a mismatch instead of adopting the node.
    ///
    /// At the root path the shape check of FD-1 decides first: this catalog
    /// initializes an untyped node, so any layer value there belongs to another
    /// writer and the root open reports an incompatible layout rather than a
    /// tag mismatch. On a child, FD-2 tag verification is the gate.
    @Test("Foreign layer value is rejected", .timeLimit(.minutes(1)))
    func foreignLayerValueIsRejected() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let layer = DirectoryLayer(database: try FDBClient.openDatabase())
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            // Initializing first gives `remove` a node to remove; what the
            // replacement node carries is what this step verifies.
            try await engine.withTransaction { transaction in
                _ = try await catalog.openOrInitializeRoot(transaction: transaction)
            }
            try await layer.remove(path: rootPath)
            _ = try await layer.createOrOpen(path: rootPath, type: .custom("foreign.layer"))
            await Self.expectFailure(.incompatibleStorageLayout, "openRoot on a foreign-typed root") {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openRoot(transaction: transaction)
                }
            }
            await Self.expectFailure(
                .incompatibleStorageLayout,
                "openOrInitializeRoot on a foreign-typed root"
            ) {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openOrInitializeRoot(transaction: transaction)
                }
            }

            try await layer.remove(path: rootPath)
            try await engine.withTransaction { transaction in
                _ = try await catalog.openOrInitializeRoot(transaction: transaction)
            }
            _ = try await layer.createOrOpen(path: rootPath + ["child"], type: .custom("foreign.layer"))
            _ = try await layer.createOrOpen(path: rootPath + ["plain"], type: nil)
            let foreign = try LayerTag(utf8: "foreign.layer")
            let entries = try await engine.withTransaction { transaction -> [DirectoryEntry] in
                let root = try #require(try await catalog.openRoot(transaction: transaction))
                await Self.expectFailure(.directoryLayerMismatch, "openDirectory on a foreign-typed child") {
                    _ = try await catalog.openDirectory("child", in: root, transaction: transaction)
                }
                await Self.expectFailure(
                    .directoryLayerMismatch,
                    "openOrCreateDirectory on a foreign-typed child"
                ) {
                    _ = try await catalog.openOrCreateDirectory("child", in: root, transaction: transaction)
                }
                await Self.expectFailure(.directoryLayerMismatch, "openPartition on a plain native node") {
                    _ = try await catalog.openPartition("plain", in: root, transaction: transaction)
                }
                // The foreign node is still listed with the layer tag it carries.
                return try await catalog.listChildren(
                    in: root,
                    after: nil,
                    limit: 10,
                    transaction: transaction
                )
            }
            #expect(entries.map(\.name) == ["child", "plain"])
            #expect(entries.map(\.layer) == [foreign, .default])
        }
    }

    @Test("Distinct root paths isolate catalogs", .timeLimit(.minutes(1)))
    func distinctRootPathsIsolateCatalogs() async throws {
        let roots = RootPathAllocator()
        let first = try await FDBStorageEngine(configuration: .init(rootPath: roots.next()))
        let second = try await FDBStorageEngine(configuration: .init(rootPath: roots.next()))
        var failure: (any Error)?
        do {
            try await first.withTransaction { transaction in
                let root = try await first.directoryAccess.openOrInitializeRoot(transaction: transaction)
                _ = try await first.directoryAccess.openOrCreateDirectory("shared", in: root, transaction: transaction)
            }
            // Each root is its own node, so the second catalog observes an
            // uninitialized root rather than the first catalog's root.
            let seenBySecond = try await second.withTransaction { transaction in
                try await second.directoryAccess.openRoot(transaction: transaction)
            }
            #expect(seenBySecond == nil)
            let secondRoot = try await second.withTransaction { transaction in
                try await second.directoryAccess.openOrInitializeRoot(transaction: transaction)
            }
            let firstRoot = try await first.withTransaction { transaction in
                try #require(try await first.directoryAccess.openRoot(transaction: transaction))
            }
            #expect(firstRoot.root.prefix != secondRoot.root.prefix)
            let children = try await second.withTransaction { transaction in
                try await second.directoryAccess.listChildren(
                    in: secondRoot, after: nil, limit: 10, transaction: transaction
                )
            }
            #expect(children.isEmpty)
        } catch {
            failure = error
        }
        await first.shutdown()
        await second.shutdown()
        try await Self.cleanUp(roots.base)
        if let failure {
            throw failure
        }
    }

    @Test("Root path configuration is validated", .timeLimit(.minutes(1)))
    func rootPathConfigurationIsValidated() async {
        for rootPath in [[String](), ["storage-kit", ""]] {
            await Self.expectFailure(.invalidOperation, "root path \(rootPath)") {
                _ = try await FDBStorageEngine(configuration: .init(rootPath: rootPath))
            }
        }
    }

    // MARK: - Support

    private static func withConformance(
        _ body: (DirectoryConformanceCase<FDBStorageEngine>) async throws -> Void
    ) async throws {
        let roots = RootPathAllocator()
        let conformance = DirectoryConformanceCase<FDBStorageEngine>(
            makeEngine: {
                try await FDBStorageEngine(configuration: .init(rootPath: roots.next()))
            }
        )
        var failure: (any Error)?
        do {
            try await body(conformance)
        } catch {
            failure = error
        }
        try await cleanUp(roots.base)
        if let failure {
            throw failure
        }
    }

    private static func withEngine(
        rootPath: [String],
        base: [String],
        _ body: (FDBStorageEngine) async throws -> Void
    ) async throws {
        let engine = try await FDBStorageEngine(configuration: .init(rootPath: rootPath))
        var failure: (any Error)?
        do {
            try await body(engine)
        } catch {
            failure = error
        }
        await engine.shutdown()
        try await cleanUp(base)
        if let failure {
            throw failure
        }
    }

    /// Removes the per-test base path through a bootstrapped client so cleanup
    /// never depends on the engine under test.
    ///
    /// Every root path of a step extends `base`, and this adapter owns no key
    /// of its own outside the native layer's metadata, so removing that node
    /// removes the whole step's state.
    private static func cleanUp(_ base: [String]) async throws {
        let layer = DirectoryLayer(database: try FDBClient.openDatabase())
        if try await layer.exists(path: base) {
            try await layer.remove(path: base)
        }
    }

    private static func expectFailure(
        _ code: StorageError.Code,
        _ label: String,
        _ body: () async throws -> Void
    ) async {
        do {
            try await body()
            Issue.record("\(label): expected \(code) but the operation succeeded")
        } catch let error as StorageError {
            #expect(error.code == code, "\(label): \(error)")
        } catch {
            Issue.record("\(label): unexpected error \(error)")
        }
    }
}
