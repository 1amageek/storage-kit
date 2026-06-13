/// Task-scoped active transaction tracking.
///
/// Prevents nested transaction deadlocks in single-connection backends (SQLite).
/// When a transaction is active on the current Task, engines can route nested
/// calls without acquiring an incompatible second transaction.
///
/// ## How it works
///
/// 1. `TransactionRunner` sets `ActiveTransactionScope.current` after creating a transaction
/// 2. `withTransaction()` checks the TaskLocal before creating a new transaction
/// 3. Backend-specific `createTransaction()` implementations may return a
///    nested child transaction that composes with the active parent
///
/// ## Thread-safety
///
/// `@TaskLocal` is scoped to Swift Concurrency Tasks, not OS threads.
/// This is correct for async/await where the same Task may hop between threads.
public enum ActiveTransactionScope: Sendable {
    @TaskLocal public static var current: (any Transaction)? = nil
}
