import Testing
@testable import StorageKit

@Suite("StorageError Tests")
struct StorageErrorTests {

    // =========================================================================
    // MARK: - isRetryable
    // =========================================================================

    @Test func transactionConflict_isRetryable() {
        let error = StorageError.transactionConflict
        #expect(error.isRetryable == true)
    }

    @Test func transactionTooOld_isRetryable() {
        let error = StorageError.transactionTooOld
        #expect(error.isRetryable == true)
    }

    @Test func keyNotFound_isNotRetryable() {
        let error = StorageError.keyNotFound
        #expect(error.isRetryable == false)
    }

    @Test func invalidOperation_isNotRetryable() {
        let error = StorageError.invalidOperation("test")
        #expect(error.isRetryable == false)
    }

    @Test func backendError_isNotRetryable() {
        let error = StorageError.backendError("test")
        #expect(error.isRetryable == false)
    }

    // =========================================================================
    // MARK: - Error Conformance
    // =========================================================================

    @Test func conformsToError() {
        let error: any Error = StorageError.keyNotFound
        #expect(error is StorageError)
    }

    @Test func conformsToSendable() {
        let error: any Sendable = StorageError.transactionConflict
        #expect(error is StorageError)
    }

    // =========================================================================
    // MARK: - Associated Values
    // =========================================================================

    @Test func invalidOperation_carriesMessage() {
        let error = StorageError.invalidOperation("Transaction cancelled")
        if case .invalidOperation(let msg) = error {
            #expect(msg == "Transaction cancelled")
        } else {
            Issue.record("Expected invalidOperation")
        }
    }

    @Test func backendError_carriesMessage() {
        let error = StorageError.backendError("SQLite open failed")
        if case .backendError(let msg) = error {
            #expect(msg == "SQLite open failed")
        } else {
            Issue.record("Expected backendError")
        }
    }
}
