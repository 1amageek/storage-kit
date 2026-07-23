/// Task-scoped active transaction tracking.
///
/// Prevents nested transaction deadlocks in single-connection backends (SQLite).
/// When a transaction is active on the current Task, engines can route nested
/// calls without acquiring an incompatible second transaction.
///
/// ## How it works
///
/// 1. The lifecycle owner calls `withActiveTransaction(_:operation:)`
/// 2. `withTransaction()` checks the TaskLocal before creating a new transaction
/// 3. Backend-specific `createTransaction()` implementations may return a
///    nested child transaction that composes with the active parent
///
/// ## Thread-safety
///
/// `@TaskLocal` is scoped to Swift Concurrency Tasks, not OS threads.
/// This is correct for async/await where the same Task may hop between threads.
public enum ActiveTransactionScope: Sendable {
    @TaskLocal package static var current: (any Transaction)? = nil

    /// Whether the current task is already executing a storage transaction.
    public static var isActive: Bool {
        current != nil
    }

    /// Runs one operation with task-scoped access to an owned transaction.
    ///
    /// The operation receives no commit or cancellation authority.
    public static func withActiveTransaction<T: Sendable>(
        _ transaction: any Transaction,
        operation: (any TransactionAccess) async throws -> T
    ) async rethrows -> T {
        try await $current.withValue(transaction) {
            try await operation(transaction)
        }
    }
}
