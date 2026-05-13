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
        #expect(error.code == .transactionConflict)
    }

    @Test func transactionTooOld_isRetryable() {
        let error = StorageError.transactionTooOld
        #expect(error.isRetryable == true)
        #expect(error.code == .transactionTooOld)
    }

    @Test func transactionBusy_isRetryable() {
        let error = StorageError.transactionBusy
        #expect(error.isRetryable == true)
        #expect(error.code == .transactionBusy)
    }

    @Test func keyNotFound_isNotRetryable() {
        let error = StorageError.keyNotFound
        #expect(error.isRetryable == false)
    }

    @Test func invalidOperation_isNotRetryable() {
        let error = StorageError.invalidOperation("test")
        #expect(error.isRetryable == false)
        #expect(error.code == .invalidOperation)
    }

    @Test func backendError_isNotRetryable() {
        let error = StorageError.backendError("test")
        #expect(error.isRetryable == false)
        #expect(error.code == .backendFailure)
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
        #expect(error.code == .invalidOperation)
        #expect(error.message == "Transaction cancelled")
    }

    @Test func backendError_carriesMessage() {
        let error = StorageError.backendError("SQLite open failed")
        #expect(error.code == .backendFailure)
        #expect(error.message == "SQLite open failed")
    }

    @Test func localizedDescription_isStructured() {
        let error = StorageError(
            code: .transactionBusy,
            operation: .beginTransaction,
            backend: .sqlite,
            message: "SQLite begin failed",
            underlyingDescription: "rc=5: database is locked"
        )

        #expect(error.localizedDescription.contains("transaction_busy"))
        #expect(error.localizedDescription.contains("backend=sqlite"))
        #expect(error.localizedDescription.contains("operation=begin_transaction"))
        #expect(error.localizedDescription.contains("SQLite begin failed"))
    }
}
