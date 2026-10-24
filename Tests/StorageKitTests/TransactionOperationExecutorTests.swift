import DatabaseTypes
@testable import StorageKit
import Synchronization
import Testing

@Suite("Structured compound operation ownership")
struct TransactionOperationExecutorTests {
    private enum Failure: Error, Equatable { case operation(Int) }

    private func transaction(_ engine: InMemoryEngine) throws -> any Transaction {
        let transaction = try engine.createTransaction()
        try transaction.configureCheckpoints(limits: TransactionCheckpointLimits(
            maximumEntries: 32, maximumDepth: 4, maximumRetainedBytes: 4096,
            maximumWorkUnits: 10000, maximumReadPageRows: 2))
        return transaction
    }

    @Test(.timeLimit(.minutes(1)))
    func nestedResultsRetainTransactionAuthority() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }
        let transaction = try transaction(engine)
        let value = try await ActiveTransactionContext.withActiveTransaction(transaction) { access in
            try await TransactionOperationExecutor(access: access).withOperation {
                #expect(ActiveTransactionContext.isActive)
                try access.setValue([2], for: [1])
                return try await TransactionOperationExecutor(access: access).withOperation {
                    #expect(ActiveTransactionContext.isActive)
                    return try await access.getValue(for: [1])
                }
            }
        }
        #expect(value == [2])
        #expect(!ActiveTransactionContext.isActive)
        try await transaction.commit()
        #expect(try await engine.withTransaction { try await $0.getValue(for: [1]) } == [2])
    }

    @Test(.timeLimit(.minutes(1)))
    func originalFailureRollsBackTheOwningTransaction() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }
        do {
            try await engine.executeTransaction { access in
                _ = try await TransactionOperationExecutor(access: access).withOperation { () async throws -> Int in
                    try access.setValue([2], for: [1])
                    throw Failure.operation(17)
                }
            }
            Issue.record("Failed compound operation unexpectedly committed")
        } catch let error as Failure {
            #expect(error == .operation(17))
        }
        #expect(try await engine.withTransaction { try await $0.getValue(for: [1]) } == nil)
    }

    @Test(.timeLimit(.minutes(1)))
    func cancellationJoinsCleanupAndPreservesItsFailure() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }
        let started = TransactionOperationCompletion()
        let cancelled = TransactionOperationCompletion()
        let release = TransactionOperationCompletion()
        let completion = CompletionState()
        let operation = Task {
            defer { completion.finish() }
            try await engine.executeTransaction { access in
                _ = try await TransactionOperationExecutor(access: access).withOperation { () async throws -> Int in
                    try access.setValue([2], for: [1])
                    return try await withTaskCancellationHandler {
                        started.succeed()
                        try await release.wait()
                        throw Failure.operation(23)
                    } onCancel: { cancelled.succeed() }
                }
            }
        }
        try await started.wait()
        operation.cancel()
        try await cancelled.wait()
        #expect(!completion.isFinished)
        release.succeed()
        do {
            try await operation.value
            Issue.record("Cancelled compound operation unexpectedly committed")
        } catch let error as Failure {
            #expect(error == .operation(23))
        }
        #expect(completion.isFinished)
        #expect(try await engine.withTransaction { try await $0.getValue(for: [1]) } == nil)
    }

    @Test(.timeLimit(.minutes(1)))
    func commitBarrierDrainsTheAdmittedChild() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }
        let transaction = try transaction(engine)
        let journal = try #require(transaction.checkpoints as? TransactionJournal)
        let started = TransactionOperationCompletion(), release = TransactionOperationCompletion()
        let operation = Task {
            try await TransactionOperationExecutor(access: transaction).withOperation {
                started.succeed()
                try await release.wait()
                try transaction.setValue([2], for: [1])
                return 42
            }
        }
        try await started.wait()
        let commit = Task { try await transaction.commit() }
        while journal.activity.activeBarrier == nil { await Task.yield() }
        do {
            try transaction.setValue([9], for: [9])
            Issue.record("Independent write entered a committing transaction")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.operation == .write)
        }
        release.succeed()
        #expect(try await operation.value == 42)
        try await commit.value
        #expect(try await engine.withTransaction { try await $0.getValue(for: [1]) } == [2])
        #expect(try await engine.withTransaction { try await $0.getValue(for: [9]) } == nil)
    }

    @Test(.timeLimit(.minutes(1)))
    func escapedTaskCannotExtendTheOperationLease() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }
        let transaction = try transaction(engine)
        let release = TransactionOperationCompletion(), escaped = EscapedTask()
        _ = try await TransactionOperationExecutor(access: transaction).withOperation {
            await escaped.store(Task {
                try await release.wait()
                try transaction.setValue([2], for: [1])
            })
            return 1
        }
        let task = try #require(await escaped.value)
        release.succeed()
        await #expect(throws: TransactionCheckpointError.closed) { try await task.value }
        try await transaction.cancel()
    }

    private final class CompletionState: Sendable {
        private let state = Mutex(false)
        var isFinished: Bool { state.withLock { $0 } }
        func finish() { state.withLock { $0 = true } }
    }

    private actor EscapedTask {
        var value: Task<Void, any Error>?
        func store(_ task: Task<Void, any Error>) { value = task }
    }
}
