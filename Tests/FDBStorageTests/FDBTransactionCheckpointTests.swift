import DatabaseTypes
import Foundation
import FoundationDB
import StorageKit
import Testing
@testable import FDBStorage

/// Targeted real-client proof; every case owns a unique native Directory subtree.
@Suite("FoundationDB transaction checkpoints", .serialized)
struct FDBTransactionCheckpointTests {
    private static func limits() throws -> TransactionCheckpointLimits {
        try TransactionCheckpointLimits(maximumEntries: 20_000, maximumDepth: 32, maximumRetainedBytes: 8 * 1024 * 1024, maximumWorkUnits: 1_000_000, maximumReadPageRows: 16)
    }

    @Test("Root, HCA, witness and nested Partition writes roll back together", .timeLimit(.minutes(1)))
    func nativeRootRollback() async throws {
        try await withEngine { engine, path in
            let transaction = try engine.createTransaction()
            try transaction.configureCheckpoints(limits: Self.limits())
            let checkpoints = try #require(transaction.checkpoints)
            let start = try await checkpoints.createCheckpoint()
            let discarded = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            let partition = try await engine.directoryAccess.openOrCreatePartition("discarded", in: discarded, transaction: transaction)
            let nested = try await engine.directoryAccess.openOrCreatePartition("nested", in: partition.root, transaction: transaction)
            try transaction.setValue([1], for: nested.root.root.prefix.appending(0x01))
            try await checkpoints.rollback(to: start)
            #expect(try await engine.directoryAccess.openRoot(transaction: transaction) == nil)
            await expectStorageFailure(.staleLease) {
                _ = try await engine.directoryAccess.open("child", expecting: nil, in: discarded, transaction: transaction)
            }
            let survivor = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            let kept = try await engine.directoryAccess.openOrCreatePartition("kept", in: survivor, transaction: transaction)
            let keptNested = try await engine.directoryAccess.openOrCreatePartition("nested", in: kept.root, transaction: transaction)
            let key = keptNested.root.root.prefix.appending(0x02)
            try transaction.setValue([2], for: key)
            try await transaction.commit()
            let database = try FDBClient.openDatabase()
            let layer = DirectoryLayer(database: database)
            let native = try database.createTransaction()
            #expect(try await layer.exists(path: path + ["discarded"], transaction: native) == false)
            let node = try await layer.open(path: path + ["kept", "nested"], transaction: native)
            #expect(node.type == .partition)
            #expect(try await native.getValue(for: key, snapshot: false) == ByteString([2]))
            native.cancel()
        }
    }

    @Test("Move, removal, clear-range and atomic suffix share one native journal", .timeLimit(.minutes(1)))
    func nativeDirectoryMutationRollback() async throws {
        try await withEngine { engine, path in
            let root = try await engine.withTransaction { transaction in
                let root = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
                _ = try await engine.directoryAccess.openOrCreateDirectory("original", in: root, transaction: transaction)
                _ = try await engine.directoryAccess.openOrCreateDirectory("remove", in: root, transaction: transaction)
                try transaction.setValue([5], for: root.root.prefix.appending(0x10))
                return root
            }
            let transaction = try engine.createTransaction()
            try transaction.configureCheckpoints(limits: Self.limits())
            let checkpoints = try #require(transaction.checkpoints)
            let start = try await checkpoints.createCheckpoint()
            let moved = try await engine.directoryAccess.move("original", in: root, to: "moved", in: root, transaction: transaction)
            try await engine.directoryAccess.remove("remove", in: root, transaction: transaction)
            let key = root.root.prefix.appending(0x10)
            try transaction.clearRange(beginKey: root.root.prefix, endKey: try strinc(root.root.prefix))
            try transaction.atomicOp(key: key, param: [3], mutationType: .add)
            #expect(try await transaction.getValue(for: key) == ByteString([3]))
            try await checkpoints.rollback(to: start)
            #expect(try await transaction.getValue(for: key) == ByteString([5]))
            await expectStorageFailure(.staleLease) {
                _ = try await engine.directoryAccess.open("child", expecting: nil, in: moved, transaction: transaction)
            }
            try transaction.atomicOp(key: key, param: [1], mutationType: .add)
            try await transaction.commit()
            let database = try FDBClient.openDatabase()
            let native = try database.createTransaction()
            let layer = DirectoryLayer(database: database)
            #expect(try await layer.exists(path: path + ["original"], transaction: native))
            #expect(try await layer.exists(path: path + ["remove"], transaction: native))
            #expect(try await layer.exists(path: path + ["moved"], transaction: native) == false)
            #expect(try await native.getValue(for: key, snapshot: false) == ByteString([6]))
            native.cancel()
        }
    }

    @Test("Native reads retain their conflict after rollback", .timeLimit(.minutes(1)))
    func nativeReadConflictSurvivesRollback() async throws {
        try await withEngine { engine, _ in
            let root = try await engine.withTransaction { try await engine.directoryAccess.openOrInitializeRoot(transaction: $0) }
            let key = root.root.prefix.appending(0x20)
            let transaction = try engine.createTransaction()
            try transaction.configureCheckpoints(limits: Self.limits())
            let checkpoints = try #require(transaction.checkpoints)
            let start = try await checkpoints.createCheckpoint()
            #expect(try await transaction.getValue(for: key) == nil)
            try await checkpoints.rollback(to: start)
            try await engine.withTransaction { try $0.setValue([9], for: key) }
            try transaction.setValue([1], for: root.root.prefix.appending(0x21))
            await expectStorageFailure(.transactionConflict) { try await transaction.commit() }
        }
    }

    @Test("Only surviving explicit write conflicts reach the native commit", .timeLimit(.minutes(1)))
    func discardedWriteConflictIsNotReplayed() async throws {
        try await withEngine { engine, _ in
            let root = try await engine.withTransaction { try await engine.directoryAccess.openOrInitializeRoot(transaction: $0) }
            let keptKey = root.root.prefix.appending(0x30)
            let discardedKey = root.root.prefix.appending(0x31)
            let keptReader = try engine.createTransaction()
            let discardedReader = try engine.createTransaction()
            _ = try await keptReader.getValue(for: keptKey)
            _ = try await discardedReader.getValue(for: discardedKey)
            let transaction = try engine.createTransaction()
            try transaction.configureCheckpoints(limits: Self.limits())
            try transaction.addConflictRange(beginKey: keptKey, endKey: keptKey.appending(0), type: .write)
            let checkpoints = try #require(transaction.checkpoints)
            let start = try await checkpoints.createCheckpoint()
            try transaction.addConflictRange(beginKey: discardedKey, endKey: discardedKey.appending(0), type: .write)
            try await checkpoints.rollback(to: start)
            try transaction.setValue([1], for: root.root.prefix.appending(0x32))
            try await transaction.commit()
            try discardedReader.setValue([2], for: root.root.prefix.appending(0x33))
            try await discardedReader.commit()
            try keptReader.setValue([3], for: root.root.prefix.appending(0x34))
            await expectStorageFailure(.transactionConflict) { try await keptReader.commit() }
        }
    }

    @Test("Unresolved versionstamps remain opaque and rollback restores readable state", .timeLimit(.minutes(1)))
    func nativeUnresolvedVersionstampRollback() async throws {
        try await withEngine { engine, _ in
            let root = try await engine.withTransaction { try await engine.directoryAccess.openOrInitializeRoot(transaction: $0) }
            let key = root.root.prefix.appending(0x40)
            let transaction = try engine.createTransaction()
            try transaction.configureCheckpoints(limits: Self.limits())
            let checkpoints = try #require(transaction.checkpoints)
            let start = try await checkpoints.createCheckpoint()
            try transaction.atomicOp(key: key, param: ByteString([UInt8](repeating: 0xff, count: 10) + [0, 0, 0, 0]), mutationType: .setVersionstampedValue)
            await expectStorageFailure(.unsupportedOperation) { _ = try await transaction.getValue(for: key) }
            try await checkpoints.rollback(to: start)
            #expect(try await transaction.getValue(for: key) == nil)
            try transaction.setValue([4], for: key)
            let versionstamp = transaction.requestVersionstamp()
            try await transaction.commit()
            #expect(try await versionstamp.value.bytes.count == 10)
            let persisted = try await engine.withTransaction { try await $0.getValue(for: key) }
            #expect(persisted == ByteString([4]))
        }
    }

    @Test("Cancellation discards native bootstrap and invalidates issued values", .timeLimit(.minutes(1)))
    func nativeCheckpointCancellation() async throws {
        try await withEngine { engine, path in
            let transaction = try engine.createTransaction()
            try transaction.configureCheckpoints(limits: Self.limits())
            let root = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            _ = try await engine.directoryAccess.openOrCreatePartition("discarded", in: root, transaction: transaction)
            try await transaction.cancel()
            let fresh = try engine.createTransaction()
            #expect(try await engine.directoryAccess.openRoot(transaction: fresh) == nil)
            await expectStorageFailure(.staleLease) {
                _ = try await engine.directoryAccess.open("child", expecting: nil, in: root, transaction: fresh)
            }
            try await fresh.cancel()
            let layer = DirectoryLayer(database: try FDBClient.openDatabase())
            #expect(try await layer.exists(path: path) == false)
        }
    }

    private func expectStorageFailure(_ code: StorageError.Code, _ body: () async throws -> Void) async {
        do { try await body(); Issue.record("Expected storage failure: \(code)") }
        catch let error as StorageError { #expect(error.code == code) }
        catch { Issue.record("Unexpected failure: \(error)") }
    }

    private func withEngine(_ body: (FDBStorageEngine, [String]) async throws -> Void) async throws {
        let path = ["storage-kit-checkpoints", UUID().uuidString.lowercased()]
        let cluster = try FDBCheckpointTestEnvironment.clusterFilePath()
        let engine = try await FDBStorageEngine(configuration: .init(clusterFilePath: cluster, rootPath: path))
        let result: Result<Void, any Error>
        do { try await body(engine, path); result = .success(()) }
        catch { result = .failure(error) }
        await engine.shutdown()
        let layer = DirectoryLayer(database: try FDBClient.openDatabase())
        do {
            if try await layer.exists(path: path) { try await layer.remove(path: path) }
        } catch {
            if case .failure(let operationError) = result {
                throw StorageTransactionCleanupError(operationError: operationError, cancellationError: error)
            }
            throw error
        }
        try result.get()
    }
}
