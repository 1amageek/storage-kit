/// Internal owner of exactly one transaction's completion.
///
/// The owner is the sole commit and cancellation authority for the
/// transaction it holds. Operations receive the transaction only as
/// `any TransactionAccess`, so they can neither complete it nor retain
/// completion authority beyond the owner's lifetime. Subtree intents that the
/// transaction registered through Directory operations 7 and 8 are released
/// when the owner finishes, whatever the outcome.
package struct TransactionLifecycleOwner: ~Copyable {
    private let transaction: any Transaction

    package init(transaction: any Transaction) {
        self.transaction = transaction
    }

    /// Runs `operation`, then commits; cancels on any failure.
    ///
    /// A cancellation failure after an operation failure is reported as
    /// `StorageTransactionCleanupError` so neither error is lost.
    package consuming func execute(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        let transaction = self.transaction
        defer {
            transaction.transactionDomain.leases.releaseIntents(for: transaction as AnyObject)
        }
        try await ActiveTransactionContext.withActiveTransaction(transaction) { access in
            do {
                try await operation(access)
                try await transaction.commit()
            } catch {
                let operationError = error
                do {
                    try await transaction.cancel()
                } catch {
                    throw StorageTransactionCleanupError(
                        operationError: operationError,
                        cancellationError: error
                    )
                }
                throw operationError
            }
        }
    }
}
