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
/// step starts from an absent root on a shared cluster; the base path and the
/// cluster-global layout keys are removed after the step, leaving only the
/// shared `storage-kit-conformance` parent node and the native allocator
/// counters behind.
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

    /// Cluster-global preparation applied to every engine the conformance case
    /// creates. The layout marker is one key per cluster, so a shared cluster
    /// is brought into the state the step under test expects.
    private enum Layout {
        /// Key the layout rejection step writes to make the keyspace nonempty.
        static let strayKey: ByteString = [0x61]

        case markerV1
        case noMarker

        func apply(to engine: FDBStorageEngine) async throws {
            try await engine.withTransaction { transaction in
                try transaction.clear(key: Layout.strayKey)
                switch self {
                case .markerV1:
                    try transaction.setValue(StorageLayoutMarker.v1, for: StorageLayoutMarker.key)
                case .noMarker:
                    try transaction.clear(key: StorageLayoutMarker.key)
                }
            }
        }
    }

    // MARK: - Shared conformance

    @Test("Root initialization", .timeLimit(.minutes(1)))
    func rootInitialization() async throws {
        try await Self.withConformance { try await $0.verifyRootInitialization() }
    }

    @Test("Layout rejection", .timeLimit(.minutes(1)))
    func layoutRejection() async throws {
        try await Self.withConformance(.noMarker) { try await $0.verifyLayoutRejection() }
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

    @Test("Transactional atomicity", .timeLimit(.minutes(1)))
    func transactionalAtomicity() async throws {
        try await Self.withConformance { try await $0.verifyTransactionalAtomicity() }
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

    /// A layer tag the native layer cannot store is rejected before any I/O.
    @Test("Layer tag that is not UTF-8 is rejected", .timeLimit(.minutes(1)))
    func layerTagThatIsNotUTF8IsRejected() async throws {
        let roots = RootPathAllocator()
        try await Self.withEngine(rootPath: roots.next(), base: roots.base) { engine in
            let catalog = engine.directoryAccess
            let invalid = try LayerTag(ByteString([0xFF, 0xFE]))
            try await engine.withTransaction { transaction in
                let root = try await catalog.openOrInitializeRoot(transaction: transaction)
                await Self.expectFailure(
                    .invalidDirectoryAddress,
                    "creating a node with a non-UTF-8 layer tag"
                ) {
                    _ = try await catalog.openOrCreate("x", layer: invalid, in: root, transaction: transaction)
                }
                let entries = try await catalog.listChildren(
                    in: root,
                    after: nil,
                    limit: 10,
                    transaction: transaction
                )
                #expect(entries.isEmpty)
            }
        }
    }

    /// A native node created outside StorageKit keeps its own layer value, so a
    /// typed open reports a mismatch instead of adopting the node.
    @Test("Foreign layer value is rejected", .timeLimit(.minutes(1)))
    func foreignLayerValueIsRejected() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let layer = DirectoryLayer(database: try FDBClient.openDatabase())
        _ = try await layer.createOrOpen(path: rootPath, type: .custom("foreign.layer"))
        try await Self.withEngine(rootPath: rootPath, base: roots.base) { engine in
            let catalog = engine.directoryAccess
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
            try await Layout.markerV1.apply(to: first)
            try await first.withTransaction { transaction in
                let root = try await first.directoryAccess.openOrInitializeRoot(transaction: transaction)
                _ = try await first.directoryAccess.openOrCreateDirectory("shared", in: root, transaction: transaction)
            }
            // The layout marker is shared, so the second catalog observes an
            // initialized cluster and an absent root of its own.
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
        _ layout: Layout = .markerV1,
        _ body: (DirectoryConformanceCase<FDBStorageEngine>) async throws -> Void
    ) async throws {
        let roots = RootPathAllocator()
        let conformance = DirectoryConformanceCase<FDBStorageEngine> {
            let engine = try await FDBStorageEngine(configuration: .init(rootPath: roots.next()))
            do {
                try await layout.apply(to: engine)
            } catch {
                await engine.shutdown()
                throw error
            }
            return engine
        }
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
            try await Layout.markerV1.apply(to: engine)
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

    /// Removes the per-test base path and the cluster-global layout keys
    /// through a bootstrapped client so cleanup never depends on the engine
    /// under test.
    private static func cleanUp(_ base: [String]) async throws {
        let bootstrap = try await FDBStorageEngine(configuration: .init(rootPath: base))
        do {
            try await bootstrap.withTransaction { transaction in
                try transaction.clear(key: Layout.strayKey)
                try transaction.clear(key: StorageLayoutMarker.key)
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
