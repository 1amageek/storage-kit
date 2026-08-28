import DatabaseTypes
@testable import FDBStorage
import Foundation
import FoundationDB
import StorageKit
import StorageKitConformance
import Synchronization
import Testing

/// Runs the shared Directory conformance case over the native FoundationDB
/// Directory Layer and proves the FoundationDB-specific layout rules.
///
/// Every engine receives its own root path below a per-test base path, so each
/// step starts from an absent root on a shared cluster; the base path is
/// removed after the step, leaving only the shared `storage-kit-conformance`
/// parent node and the native allocator counters behind.
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

    // MARK: - Shared conformance

    @Test(.timeLimit(.minutes(1)))
    func rootInitialization() async throws {
        try await Self.withConformance { try await $0.verifyRootInitialization() }
    }

    @Test(.timeLimit(.minutes(1)))
    func createAndOpen() async throws {
        try await Self.withConformance { try await $0.verifyCreateAndOpen() }
    }

    @Test(.timeLimit(.minutes(1)))
    func listing() async throws {
        try await Self.withConformance { try await $0.verifyListing() }
    }

    @Test(.timeLimit(.minutes(1)))
    func move() async throws {
        try await Self.withConformance { try await $0.verifyMove() }
    }

    @Test(.timeLimit(.minutes(1)))
    func remove() async throws {
        try await Self.withConformance { try await $0.verifyRemove() }
    }

    @Test(.timeLimit(.minutes(1)))
    func domainMismatch() async throws {
        try await Self.withConformance { try await $0.verifyDomainMismatch() }
    }

    @Test(.timeLimit(.minutes(1)))
    func leaseLifecycle() async throws {
        try await Self.withConformance { try await $0.verifyLeaseLifecycle() }
    }

    @Test(.timeLimit(.minutes(1)))
    func transactionalAtomicity() async throws {
        try await Self.withConformance { try await $0.verifyTransactionalAtomicity() }
    }

    // MARK: - FoundationDB layout

    /// The layout marker of this adapter is the native layer type: a root or
    /// child that exists with a foreign type is rejected, never adopted.
    @Test(.timeLimit(.minutes(1)))
    func foreignLayerTypeIsRejected() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let layer = DirectoryLayer(database: try FDBClient.openDatabase())
        var failure: (any Error)?
        do {
            _ = try await layer.createOrOpen(path: rootPath, type: .custom("foreign.layer"))
            let engine = try await FDBStorageEngine(configuration: .init(rootPath: rootPath))
            await Self.expectFailure(.incompatibleStorageLayout, "openRoot on a foreign-typed root") {
                try await engine.withTransaction { transaction in
                    _ = try await engine.directoryAccess.openRoot(transaction: transaction)
                }
            }
            await Self.expectFailure(.incompatibleStorageLayout, "openOrInitializeRoot on a foreign-typed root") {
                try await engine.withTransaction { transaction in
                    _ = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
                }
            }

            try await layer.remove(path: rootPath)
            try await engine.withTransaction { transaction in
                _ = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            }
            _ = try await layer.createOrOpen(path: rootPath + ["dchild"], type: .custom("foreign.layer"))
            _ = try await layer.createOrOpen(path: rootPath + ["p01"], type: .custom("storage-kit.directory.v1"))
            await Self.expectFailure(.incompatibleStorageLayout, "openDirectory on a foreign-typed child") {
                try await engine.withTransaction { transaction in
                    let root = try #require(try await engine.directoryAccess.openRoot(transaction: transaction))
                    _ = try await engine.directoryAccess.openDirectory("child", in: root, transaction: transaction)
                }
            }
            await Self.expectFailure(.incompatibleStorageLayout, "openOrCreateDirectory on a foreign-typed child") {
                try await engine.withTransaction { transaction in
                    let root = try #require(try await engine.directoryAccess.openRoot(transaction: transaction))
                    _ = try await engine.directoryAccess.openOrCreateDirectory("child", in: root, transaction: transaction)
                }
            }
            await Self.expectFailure(.incompatibleStorageLayout, "openPartition on a Directory-typed node") {
                try await engine.withTransaction { transaction in
                    let root = try #require(try await engine.directoryAccess.openRoot(transaction: transaction))
                    _ = try await engine.directoryAccess.openPartition(
                        try PartitionID([0x01]),
                        in: root,
                        transaction: transaction
                    )
                }
            }
            await engine.shutdown()
        } catch {
            failure = error
        }
        try await Self.removeBase(roots.base)
        if let failure {
            throw failure
        }
    }

    /// Native names carry the node kind as a prefix so equal bytes never
    /// collide across kinds and arbitrary Partition identifier bytes survive.
    @Test(.timeLimit(.minutes(1)))
    func nativeNamesCarryKindAndHexIdentifier() async throws {
        let roots = RootPathAllocator()
        let rootPath = roots.next()
        let engine = try await FDBStorageEngine(configuration: .init(rootPath: rootPath))
        var failure: (any Error)?
        do {
            let partitionID = try PartitionID([0x00, 0xFF, 0x10])
            try await engine.withTransaction { transaction in
                let root = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
                _ = try await engine.directoryAccess.openOrCreateDirectory("alpha", in: root, transaction: transaction)
                _ = try await engine.directoryAccess.openOrCreatePartition(partitionID, in: root, transaction: transaction)
                _ = try await engine.directoryAccess.openOrCreateDirectory("00ff10", in: root, transaction: transaction)
                _ = try await engine.directoryAccess.openOrCreatePartition(
                    try PartitionID(utf8: "alpha"),
                    in: root,
                    transaction: transaction
                )
            }
            let layer = DirectoryLayer(database: try FDBClient.openDatabase())
            let nativeNames = try await layer.list(path: rootPath).sorted()
            #expect(nativeNames == ["d00ff10", "dalpha", "p00ff10", "p616c706861"])

            let listed = try await engine.withTransaction { transaction in
                let root = try #require(try await engine.directoryAccess.openRoot(transaction: transaction))
                let directories = try await engine.directoryAccess.listDirectories(
                    in: root, after: nil, limit: 10, transaction: transaction
                )
                let partitions = try await engine.directoryAccess.listPartitions(
                    in: root, after: nil, limit: 10, transaction: transaction
                )
                return (directories, partitions)
            }
            #expect(listed.0 == ["00ff10", "alpha"])
            #expect(listed.1 == [partitionID, try PartitionID(utf8: "alpha")])
        } catch {
            failure = error
        }
        await engine.shutdown()
        try await Self.removeBase(roots.base)
        if let failure {
            throw failure
        }
    }

    @Test(.timeLimit(.minutes(1)))
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
                try await second.directoryAccess.listDirectories(
                    in: secondRoot, after: nil, limit: 10, transaction: transaction
                )
            }
            #expect(children.isEmpty)
        } catch {
            failure = error
        }
        await first.shutdown()
        await second.shutdown()
        try await Self.removeBase(roots.base)
        if let failure {
            throw failure
        }
    }

    @Test(.timeLimit(.minutes(1)))
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
        let conformance = DirectoryConformanceCase<FDBStorageEngine> {
            try await FDBStorageEngine(configuration: .init(rootPath: roots.next()))
        }
        var failure: (any Error)?
        do {
            try await body(conformance)
        } catch {
            failure = error
        }
        try await removeBase(roots.base)
        if let failure {
            throw failure
        }
    }

    /// Removes the per-test base path through a bootstrapped client so that
    /// cleanup never depends on the engine under test.
    private static func removeBase(_ base: [String]) async throws {
        let bootstrap = try await FDBStorageEngine(configuration: .init(rootPath: base))
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
