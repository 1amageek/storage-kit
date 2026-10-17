import DatabaseTypes
import Foundation
@testable import PostgreSQLStorage
import StorageKit
import StorageKitConformance
import Testing
import Synchronization

@Suite("PostgreSQL transaction checkpoints", .timeLimit(.minutes(1)), .enabled(if: PostgreSQLTestEnvironment.isConfigured))
struct PostgreSQLTransactionCheckpointTests {
    private static func configuration(
        isolation: PostgreSQLIsolationLevel = .serializable
    ) throws -> PostgreSQLConfiguration {
        var configuration = try PostgreSQLTestEnvironment.makeConfiguration()
        configuration.tableName = "checkpoint_" + UUID().uuidString.lowercased().replacingOccurrences(of: "-", with: "_")
        configuration.isolationLevel = isolation
        return configuration
    }

    private let conformance = TransactionCheckpointConformanceCase<PostgreSQLStorageEngine> {
        try await PostgreSQLStorageEngine(configuration: Self.configuration())
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

    @Test func configuredIsolationControlsCheckpointAdmission() async throws {
        for isolation in [PostgreSQLIsolationLevel.readCommitted, .repeatableRead] {
            let engine = try await PostgreSQLStorageEngine(configuration: Self.configuration(isolation: isolation))
            defer { await engine.shutdown() }
            let transaction = try engine.createTransaction()
            defer {
                do { try await transaction.cancel() }
                catch { Issue.record(error, "Transaction cancellation failed") }
            }
            do {
                try transaction.configureCheckpoints(limits: limits())
                Issue.record("Non-SERIALIZABLE checkpoint admission must fail")
            } catch let error as StorageError {
                #expect(error.code == .unsupportedOperation)
            }
            #expect(transaction.checkpoints == nil)
            try transaction.setValue([1], for: [1])
            #expect(try await transaction.getValue(for: [1]) == [1])
        }
    }

    @Test func checkpointModeRejectsNativeChildren() async throws {
        let engine = try await PostgreSQLStorageEngine(configuration: Self.configuration())
        defer { await engine.shutdown() }
        let transaction = try engine.createTransaction()
        defer {
            do { try await transaction.cancel() }
            catch { Issue.record(error, "Transaction cancellation failed") }
        }
        try transaction.configureCheckpoints(limits: limits())
        _ = await ActiveTransactionContext.withActiveTransaction(transaction) { _ in
            #expect(throws: StorageError.self) { _ = try engine.createTransaction() }
        }
        try transaction.setValue([1], for: [1])
        #expect(try await transaction.getValue(for: [1]) == [1])
    }

    @Test func oversizedNativePageIsRejectedBeforeRetainingResultOwners() async throws {
        let observation = ResultOwnerCount()
        let engine = try await PostgreSQLStorageEngine(
            configuration: Self.configuration(), resultBytesLifecycleObserver: observation
        )
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
        let before = observation.count
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual([]), to: .firstGreaterOrEqual([0xFF]),
            limit: 0, reverse: false, snapshot: false, streamingMode: .small
        )
        defer {
            do { try await cursor.finish() }
            catch { Issue.record(error, "Range cursor cleanup failed") }
        }
        await #expect(throws: StorageError.self) { _ = try await cursor.next() }
        #expect(observation.count == before)
    }

    private final class ResultOwnerCount: PostgreSQLResultBytesLifecycleObserver {
        private let value = Mutex<UInt64>(0)
        var count: UInt64 { value.withLock { $0 } }
        func resultBytesOwnerCreated(readableByteCount: Int, baseAddress: UInt?) -> UInt64 {
            value.withLock { value in value += 1; return value }
        }
        func resultBytesOwnerReleased(identifier: UInt64) {}
    }

}
