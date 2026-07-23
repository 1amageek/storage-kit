import Testing
@testable import StorageKit

@Suite("Transaction operation completion")
struct TransactionOperationCompletionTests {
    @Test
    func everyWaiterObservesSuccess() async throws {
        let completion = TransactionOperationCompletion()
        let waiters = (0..<16).map { _ in
            Task {
                try await completion.wait()
            }
        }

        await Task.yield()
        completion.resolve(.success(()))

        for waiter in waiters {
            try await waiter.value
        }

        try await completion.wait()
    }

    @Test
    func everyWaiterObservesTheSameFailure() async {
        let completion = TransactionOperationCompletion()
        let expected = StorageError(
            code: .transactionConflict,
            operation: .commit,
            message: "Concurrent commit conflict"
        )
        let waiters = (0..<16).map { _ in
            Task {
                try await completion.wait()
            }
        }

        await Task.yield()
        completion.resolve(.failure(expected))

        for waiter in waiters {
            do {
                try await waiter.value
                Issue.record("Expected transaction completion failure")
            } catch let error as StorageError {
                #expect(error == expected)
            } catch {
                Issue.record("Unexpected error: \(error)")
            }
        }

        do {
            try await completion.wait()
            Issue.record("Expected late waiter to receive failure")
        } catch let error {
            #expect(error == expected)
        }
    }
}
