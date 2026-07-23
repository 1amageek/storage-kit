import Testing
@testable import StorageKit

@Suite("Transaction versionstamp completion")
struct TransactionVersionstampCompletionTests {
    @Test
    func everyWaiterObservesTheCommittedVersionstamp() async throws {
        let completion = TransactionVersionstampCompletion()
        let expected = try TransactionVersionstamp(
            bytes: Bytes(repeating: 7, count: TransactionVersionstamp.byteCount)
        )
        let waiters = (0..<16).map { _ in
            Task {
                try await completion.wait()
            }
        }

        await Task.yield()
        completion.resolveIfPending(.success(expected))

        for waiter in waiters {
            #expect(try await waiter.value == expected)
        }
        #expect(try await completion.wait() == expected)
    }

    @Test
    func initiallyFailedCompletionPreservesItsFailure() async {
        let expected = StorageError(
            code: .transactionCancelled,
            operation: .read,
            message: "Transaction was cancelled"
        )
        let completion = TransactionVersionstampCompletion(failure: expected)

        do {
            _ = try await completion.wait()
            Issue.record("Expected versionstamp completion failure")
        } catch let error {
            #expect(error == expected)
        }
    }

    @Test
    func firstResolutionWins() async throws {
        let completion = TransactionVersionstampCompletion()
        let expected = try TransactionVersionstamp(
            bytes: Bytes(repeating: 3, count: TransactionVersionstamp.byteCount)
        )
        let ignored = StorageError(
            code: .transactionCancelled,
            operation: .read,
            message: "Late cancellation"
        )

        completion.resolveIfPending(.success(expected))
        completion.resolveIfPending(.failure(ignored))

        #expect(try await completion.wait() == expected)
    }
}
