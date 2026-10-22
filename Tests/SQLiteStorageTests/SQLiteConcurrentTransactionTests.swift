import DatabaseTypes
import Foundation
import StorageKit
import Testing
@testable import SQLiteStorage

@Suite("SQLite parallel WAL transactions", .timeLimit(.minutes(1)))
struct SQLiteConcurrentTransactionTests: Sendable {
    private enum Failure: Error { case childRollback, queueDeadline }

    @Test func snapshotsRemainStableAndStaleUpgradesFail() async throws {
        try await withEngine(connectionCount: 3) { engine, _ in
            let seed = try engine.createTransaction()
            try seed.setValue([1], for: [1])
            try await seed.commit()
            let old = try engine.createTransaction()
            #expect(try await old.getValue(for: [1]) == [1])
            let writer = try engine.createTransaction()
            try writer.setValue([2], for: [1])
            #expect(try await writer.getValue(for: [1]) == [2])
            try await writer.commit()
            let latest = try engine.createTransaction()
            #expect(try await latest.getValue(for: [1]) == [2])
            #expect(try await old.getValue(for: [1]) == [1])
            try old.setValue([3], for: [1])
            do {
                try await old.commit()
                Issue.record("A stale SQLite snapshot must not overwrite a committed writer")
            } catch let error as StorageError {
                #expect(error.code == .transactionBusy)
                #expect(error.retryDisposition == .safe)
            }
            try await old.cancel()
            try await latest.cancel()
        }
    }

    @Test func readersOverlapAndCommittedValuesSurviveRestart() async throws {
        try await withEngine(connectionCount: 2) { engine, path in
            let seed = try engine.createTransaction()
            try seed.setValue([7], for: [1])
            try await seed.commit()
            let first = try engine.createTransaction()
            let second = try engine.createTransaction()
            #expect(try await first.getValue(for: [1]) == [7])
            // The second read completes while the first root still owns its snapshot.
            #expect(try await second.getValue(for: [1]) == [7])
            try await first.cancel()
            try await second.cancel()
            await engine.shutdown()
            let reopened = try SQLiteStorageEngine(configuration: .file(path, connectionCount: 2))
            defer { await reopened.shutdown() }
            let read = try reopened.createTransaction()
            #expect(try await read.getValue(for: [1]) == [7])
            try await read.commit()
        }
    }

    @Test func nestedSavepointsAndCheckpointsStayOnTheirRootConnection() async throws {
        try await withEngine(connectionCount: 3) { engine, _ in
            try await engine.withTransaction { outer in
                try outer.setValue([1], for: [1])
                do {
                    try await engine.withTransaction { child in
                        #expect(try await child.getValue(for: [1]) == [1])
                        try child.setValue([2], for: [2])
                        #expect(try await child.getValue(for: [2]) == [2])
                        throw Failure.childRollback
                    }
                } catch Failure.childRollback {}
                #expect(try await outer.getValue(for: [1]) == [1])
                #expect(try await outer.getValue(for: [2]) == nil)
            }
            #expect(await engine.leaseInstrumentation.savepointBeginCount == 1)
            #expect(await engine.leaseInstrumentation.savepointRollbackCount == 1)
            let transaction = try engine.createTransaction()
            try transaction.configureCheckpoints(limits: TransactionCheckpointLimits(
                maximumEntries: 128, maximumDepth: 8, maximumRetainedBytes: 1_048_576,
                maximumWorkUnits: 10_000, maximumReadPageRows: 2))
            let checkpoints = try #require(transaction.checkpoints)
            try transaction.setValue([3], for: [3])
            let marker = try await checkpoints.createCheckpoint()
            try transaction.setValue([4], for: [4])
            #expect(try await transaction.getValue(for: [4]) == [4])
            try await checkpoints.rollback(to: marker)
            try await transaction.commit()
            let read = try engine.createTransaction()
            #expect(try await read.getValue(for: [3]) == [3])
            #expect(try await read.getValue(for: [4]) == nil)
            try await read.cancel()
        }
    }

    @Test func shutdownCompletesEveryQueuedConnectionAndRejectsActiveReads() async throws {
        try await withEngine(connectionCount: 2) { engine, _ in
            let first = try engine.createTransaction()
            let second = try engine.createTransaction()
            _ = try await first.getValue(for: [1])
            _ = try await second.getValue(for: [1])
            let third = try engine.createTransaction()
            let fourth = try engine.createTransaction()
            let left = Task { try await third.getValue(for: [1]) }
            let right = Task { try await fourth.getValue(for: [1]) }
            defer {
                await engine.shutdown()
                _ = await left.result
                _ = await right.result
            }
            let clock = ContinuousClock()
            let deadline = clock.now.advanced(by: .seconds(5))
            while await engine.leaseInstrumentation.waitingRootCount != 2 {
                guard clock.now < deadline else { throw Failure.queueDeadline }
                try await Task.sleep(for: .milliseconds(1))
            }
            await engine.shutdown()
            await #expect(throws: StorageError.self) { _ = try await left.value }
            await #expect(throws: StorageError.self) { _ = try await right.value }
            await #expect(throws: StorageError.self) { _ = try await first.getValue(for: [1]) }
            #expect(await engine.leaseInstrumentation.waitingRootCount == 0)
            #expect(await engine.leaseInstrumentation.hasActiveRoot == false)
            #expect(engine.rangeInstrumentation.openCursorCount == 0)
            #expect(throws: StorageError.self) { _ = try engine.createTransaction() }
        }
    }

    @Test func uriOptionsAreNotDiscardedForAdditionalConnections() async throws {
        try await withEngine(connectionCount: 2) { engine, path in
            await engine.shutdown()
            do {
                let unexpected = try SQLiteStorageEngine(configuration:
                    .file("file:\(path)?mode=ro", connectionCount: 2))
                defer { await unexpected.shutdown() }
                let readOnly = try unexpected.createTransaction()
                let widened = try unexpected.createTransaction()
                try widened.setValue([9], for: [9])
                try await widened.commit()
                try await readOnly.cancel()
                Issue.record("A resolved filename must not discard URI read-only access on another connection")
            } catch let error as StorageError {
                #expect(error.code == .unsupportedOperation)
                #expect(error.operation == .open)
            }
        }
    }

    @Test func invalidCountsAndPrivateMemoryAreRejected() throws {
        for count in [0, -1] {
            var configuration = SQLiteStorageEngine.Configuration.inMemory
            configuration.connectionCount = count
            do {
                _ = try SQLiteStorageEngine(configuration: configuration)
                Issue.record("Invalid connection count was accepted")
            } catch let error as StorageError {
                #expect(error.code == .invalidOperation)
                #expect(error.operation == .open)
            }
        }
        var configuration = SQLiteStorageEngine.Configuration.inMemory
        configuration.connectionCount = 2
        do {
            _ = try SQLiteStorageEngine(configuration: configuration)
            Issue.record("Private in-memory connections must not silently form independent stores")
        } catch let error as StorageError {
            #expect(error.code == .unsupportedOperation)
            #expect(error.operation == .open)
        }
    }

    private func withEngine(connectionCount: Int,
        _ body: @Sendable (SQLiteStorageEngine, String) async throws -> Void) async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("sqlite-parallel-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false)
        defer {
            do { try FileManager.default.removeItem(at: directory) }
            catch { Issue.record(error, "Temporary SQLite directory cleanup failed") }
        }
        let path = directory.appendingPathComponent("database.sqlite").path
        let engine = try SQLiteStorageEngine(configuration: .file(path, connectionCount: connectionCount))
        defer { await engine.shutdown() }
        try await body(engine, path)
    }
}
