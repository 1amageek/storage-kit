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
/// step starts from an absent root and an absent marker on a shared cluster;
/// the base path and the markers of every root below it are removed after the
/// step, leaving only the shared `storage-kit-conformance` parent node and the
/// native allocator counters behind.
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
    /// to native paths and to the root's layout marker key, so the probe reads
    /// the path back from the adapter instead of restating it.
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

    /// Layout probe for a backend whose storage root is a node in a shared
    /// cluster.
    ///
    /// A foreign layout leaves a native Directory node at the engine's root
    /// path that StorageKit never marked, which is the V0 row of the layout
    /// state machine. The marker of that root is one key of its own, so the
    /// probe never touches another root's marker.
    private static let layoutProbe = DirectoryConformanceCase<FDBStorageEngine>.LayoutProbe(
        makeRootForeign: { engine in
            let rootPath = try Self.rootPath(of: engine)
            let layer = DirectoryLayer(database: try FDBClient.openDatabase())
            _ = try await layer.createOrOpen(path: rootPath)
        },
        writeMarker: { engine, value in
            let rootPath = try Self.rootPath(of: engine)
            try await engine.withTransaction { transaction in
                try transaction.setValue(
                    value,
                    for: StorageLayoutMarker.key(rootPath: rootPath)
                )
            }
        }
    )

    // MARK: - Shared conformance

    @Test("Root initialization", .timeLimit(.minutes(1)))
    func rootInitialization() async throws {
        try await Self.withConformance { try await $0.verifyRootInitialization() }
    }

    @Test("Layout rejection", .timeLimit(.minutes(1)))
    func layoutRejection() async throws {
        try await Self.withConformance { try await $0.verifyLayoutRejection() }
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

    @Test("Domain mismatch", .timeLimit(.minutes(1)))
    func domainMismatch() async throws {
        try await Self.withConformance { try await $0.verifyDomainMismatch() }
    }

    @Test("Lease lifecycle", .timeLimit(.minutes(1)))
    func leaseLifecycle() async throws {
        try await Self.withConformance { try await $0.verifyLeaseLifecycle() }
    }

    @Test("Lease subtree exclusion", .timeLimit(.minutes(1)))
    func leaseSubtreeExclusion() async throws {
        try await Self.withConformance { try await $0.verifyLeaseSubtreeExclusion() }
    }

    @Test("Transactional atomicity", .timeLimit(.minutes(1)))
    func transactionalAtomicity() async throws {
        try await Self.withConformance { try await $0.verifyTransactionalAtomicity() }
    }

    // MARK: - Root scoping

    /// The storage root of an engine is the node at its configured root path,
    /// so the layout state machine of SPEC §10.3 reads that root's own marker
    /// and that root's own emptiness.
    ///
    /// A cluster always holds data belonging to no single root: other roots,
    /// their markers, and the native allocator counters. Counting that as this
    /// root's data would reject every new root on a cluster already in use.
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

    /// One root's rejected layout never rejects another root.
    @Test("A rejected root leaves its sibling roots usable", .timeLimit(.minutes(1)))
    func aRejectedRootLeavesItsSiblingsUsable() async throws {
        let roots = RootPathAllocator()
        let rejected = roots.next()
        let healthy = roots.next()
        try await Self.withEngine(rootPath: rejected, base: roots.base) { engine in
            try await Self.layoutProbe.writeMarker(engine, [0x53, 0x4B, 0x4C, 0x02])
            await Self.expectFailure(.incompatibleStorageLayout, "unknown marker on its own root") {
                _ = try await engine.withTransaction { transaction in
                    try await engine.directoryAccess.openRoot(transaction: transaction)
                }
            }

            let sibling = try await FDBStorageEngine(configuration: .init(rootPath: healthy))
            do {
                let initialized = try await sibling.withTransaction { transaction in
                    try await sibling.directoryAccess.openOrInitializeRoot(transaction: transaction)
                }
                #expect(initialized.address == .root)
            } catch {
                await sibling.shutdown()
                throw error
            }
            await sibling.shutdown()
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
    /// The layout state machine runs before any layer-tag verification (FD-1),
    /// so a root without a V1 marker is a layout rejection whatever its node
    /// carries; `layoutRejection` and the root-scoping tests own that path.
    /// This step therefore marks the root first and then replaces its native
    /// node, which leaves the layer-tag verification of FD-2 as the only gate
    /// the root open can fail.
    @Test("Foreign layer value is rejected", .timeLimit(.minutes(1)))
    func foreignLayerValueIsRejected() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let layer = DirectoryLayer(database: try FDBClient.openDatabase())
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
            // The marker lives outside the root node's content prefix, so it
            // survives the removal below and the root stays layout-V1 while
            // its native node carries a foreign layer value.
            try await engine.withTransaction { transaction in
                _ = try await catalog.openOrInitializeRoot(transaction: transaction)
            }
            try await layer.remove(path: rootPath)
            _ = try await layer.createOrOpen(path: rootPath, type: .custom("foreign.layer"))
            await Self.expectFailure(.directoryLayerMismatch, "openRoot on a foreign-typed root") {
                try await engine.withTransaction { transaction in
                    _ = try await catalog.openRoot(transaction: transaction)
                }
            }
            await Self.expectFailure(.directoryLayerMismatch, "openOrInitializeRoot on a foreign-typed root") {
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
            // Each root owns its own marker, so the second catalog observes an
            // uninitialized layout rather than the first catalog's root.
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
            },
            layoutProbe: layoutProbe
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

    /// Removes the per-test base path and the layout markers of every root
    /// below it through a bootstrapped client so cleanup never depends on the
    /// engine under test.
    ///
    /// Every root path of a step extends `base`, so every marker key of the
    /// step lies under the marker key of `base` itself.
    private static func cleanUp(_ base: [String]) async throws {
        let bootstrap = try await FDBStorageEngine(configuration: .init(rootPath: base))
        do {
            let markers = try Subspace(
                prefix: StorageLayoutMarker.key(rootPath: base)
            ).prefixRange()
            try await bootstrap.withTransaction { transaction in
                try transaction.clearRange(beginKey: markers.begin, endKey: markers.end)
            }
        } catch {
            await bootstrap.shutdown()
            throw error
        }
        await bootstrap.shutdown()
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
