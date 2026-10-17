import DatabaseTypes
import Foundation
@testable import SQLiteStorage
import StorageKit
import StorageKitConformance
import Testing

@Suite("SQLite transaction checkpoints", .timeLimit(.minutes(1)))
struct SQLiteTransactionCheckpointTests {
    private let conformance = TransactionCheckpointConformanceCase<SQLiteStorageEngine> {
        try SQLiteStorageEngine(configuration: .inMemory)
    }

    @Test func tokens() async throws {
        try await conformance.verifyTokens()
    }

    @Test func overlay() async throws {
        try await conformance.verifyOverlay()
    }

    @Test func directoryRollback() async throws {
        try await conformance.verifyDirectoryRollback()
    }

    @Test func conflicts() async throws {
        try await conformance.verifyConflicts()
    }

    @Test func lifecycle() async throws {
        try await conformance.verifyLifecycle()
    }

    @Test func budgets() async throws {
        try await conformance.verifyBudgets()
    }

    private func limits() throws -> TransactionCheckpointLimits {
        try TransactionCheckpointLimits(
            maximumEntries: 128, maximumDepth: 8,
            maximumRetainedBytes: 1_048_576, maximumWorkUnits: 10_000,
            maximumReadPageRows: 2
        )
    }

    @Test func fileCommitReopenKeepsOnlySurvivingWrites() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do { try FileManager.default.removeItem(at: directory) }
            catch { Issue.record(error, "Temporary database directory cleanup failed") }
        }
        let path = directory.appendingPathComponent("checkpoint.sqlite").path
        let engine = try SQLiteStorageEngine(configuration: .file(path))
        defer { await engine.shutdown() }
        let transaction = try engine.createTransaction()
        var committed = false
        defer {
            if !committed {
                do { try await transaction.cancel() }
                catch { Issue.record(error, "Transaction cancellation failed") }
            }
        }
        try transaction.configureCheckpoints(limits: limits())
        let access = try #require(transaction.checkpoints)
        try transaction.setValue([1], for: [0x01])
        let marker = try await access.createCheckpoint()
        try transaction.setValue([2], for: [0x02])
        #expect(try await transaction.getValue(for: [0x02]) == [2])
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual([]), to: .firstGreaterOrEqual([0xFF]),
            limit: 0, reverse: false, snapshot: false, streamingMode: .small
        )
        #expect(try await cursor.next()?.0 == [0x01])
        try await access.rollback(to: marker)
        #expect(try await cursor.next() == nil)
        try await transaction.commit()
        committed = true
        await engine.shutdown()

        let reopened = try SQLiteStorageEngine(configuration: .file(path))
        defer { await reopened.shutdown() }
        try await reopened.withTransaction { transaction in
            let surviving = try await transaction.getValue(for: [0x01])
            let discarded = try await transaction.getValue(for: [0x02])
            #expect(surviving == [1])
            #expect(discarded == nil)
        }
    }

    @Test func checkpointModeRejectsNativeChildrenAndCompaction() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { await engine.shutdown() }
        let transaction = try engine.createTransaction()
        try transaction.configureCheckpoints(limits: limits())
        defer {
            do { try await transaction.cancel() }
            catch { Issue.record(error, "Transaction cancellation failed") }
        }
        _ = await ActiveTransactionContext.withActiveTransaction(transaction) { _ in
            #expect(throws: StorageError.self) { _ = try engine.createTransaction() }
        }
        await #expect(throws: StorageError.self) {
            _ = try await transaction.stageCompactionSlice(maximumWorkUnits: 1, continuation: nil)
        }
        try transaction.setValue([1], for: [1])
        #expect(try await transaction.getValue(for: [1]) == [1])
    }

    @Test func oversizedNativeRowIsRejectedBeforePayloadCopies() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { await engine.shutdown() }
        let large = ByteString([UInt8](repeating: 0x41, count: 64))
        try await engine.withTransaction { try $0.setValue(large, for: [1]) }
        let transaction = try engine.createTransaction()
        defer {
            do { try await transaction.cancel() }
            catch { Issue.record(error, "Transaction cancellation failed") }
        }
        try transaction.configureCheckpoints(limits: TransactionCheckpointLimits(
            maximumEntries: 8, maximumDepth: 2, maximumRetainedBytes: 32,
            maximumWorkUnits: 1_000, maximumReadPageRows: 2
        ))
        let before = engine.rangeInstrumentation.payloadCopyCount
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual([]), to: .firstGreaterOrEqual([0xFF]),
            limit: 0, reverse: false, snapshot: false, streamingMode: .small
        )
        defer {
            do { try await cursor.finish() }
            catch { Issue.record(error, "Range cursor cleanup failed") }
        }
        await #expect(throws: StorageError.self) { _ = try await cursor.next() }
        #expect(engine.rangeInstrumentation.payloadCopyCount == before)
    }

    @Test func externalOwnerBorrowCanReenterTransaction() async throws {
        let engine = try SQLiteStorageEngine(configuration: .inMemory)
        defer { await engine.shutdown() }
        let transaction = try engine.createTransaction()
        defer {
            do { try await transaction.cancel() }
            catch { Issue.record(error, "Transaction cancellation failed") }
        }
        try transaction.configureCheckpoints(limits: limits())
        let owner = ReentrantSQLiteBytes(transaction: transaction)
        try transaction.setValue(ByteString(retaining: owner), for: [1])
        #expect(try await transaction.getValue(for: [1]) == [7])
    }

}

private final class ReentrantSQLiteBytes: ByteStringOwner, Sendable {
    private let transaction: SQLiteStorageTransaction
    init(transaction: SQLiteStorageTransaction) { self.transaction = transaction }
    var count: Int { 1 }
    var retainedByteCount: Int? { nil }
    func borrowBytes(_ body: (UnsafeRawBufferPointer) throws -> Void) rethrows {
        // The property takes the adapter mutex, so detachment must run outside it.
        _ = transaction.checkpoints
        try [UInt8(7)].withUnsafeBytes(body)
    }
}
