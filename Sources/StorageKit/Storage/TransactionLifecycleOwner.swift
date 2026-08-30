/// Internal owner of exactly one transaction's completion.
///
/// The owner is the sole commit and cancellation authority for the
/// transaction it holds. Operations receive the transaction only as
/// `any TransactionAccess`, so they can neither complete it nor retain
/// completion authority beyond the owner's lifetime. A backend adapter that
/// needs its own construction or native-error conversion still completes
/// through this owner instead of committing or cancelling around it.
package struct TransactionLifecycleOwner: ~Copyable {
    private let transaction: any Transaction

    package init(transaction: any Transaction) {
        self.transaction = transaction
    }

    /// Runs `operation`, then commits.
    ///
    /// An operation failure, and a commit failure whose outcome is known,
    /// are followed by the cancellation. A cancellation failure after either
    /// is reported as `StorageTransactionCleanupError` so neither error is
    /// lost.
    ///
    /// A commit that failed `commitUnknownResult` is rethrown unwrapped and is
    /// not followed by a cancellation: the transaction may already have been
    /// applied, and a backend that reached that state reports the same unknown
    /// outcome from its own cancellation, which would replace the caller's
    /// `requiresIdempotency` disposition with a cleanup failure.
    package consuming func execute(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        let transaction = self.transaction
        try await ActiveTransactionContext.withActiveTransaction(transaction) { access in
            do {
                try await operation(access)
            } catch {
                try await Self.cancel(transaction, preserving: error)
                throw error
            }

            do {
                try await transaction.commit()
            } catch {
                if let storageError = error as? StorageError,
                   storageError.code == .commitUnknownResult {
                    throw storageError
                }
                try await Self.cancel(transaction, preserving: error)
                throw error
            }
        }
    }

    private static func cancel(
        _ transaction: any Transaction,
        preserving operationError: any Error
    ) async throws {
        do {
            try await transaction.cancel()
        } catch {
            throw StorageTransactionCleanupError(
                operationError: operationError,
                cancellationError: error
            )
        }
    }
}
