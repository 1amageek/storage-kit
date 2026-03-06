/// Task-scoped active transaction tracking.
///
/// Prevents nested transaction deadlocks in single-connection backends (SQLite).
/// When a transaction is active on the current Task, nested `withTransaction()` or
/// `createTransaction()` calls reuse it instead of acquiring a new lock.
///
/// ## How it works
///
/// 1. `TransactionRunner` sets `ActiveTransactionScope.current` after creating a transaction
/// 2. `SQLiteStorageEngine.withTransaction()` checks the TaskLocal before creating a new transaction
/// 3. If a transaction is already active, the existing one is reused (no new BEGIN/COMMIT/lock)
///
/// ## Thread-safety
///
/// `@TaskLocal` is scoped to Swift Concurrency Tasks, not OS threads.
/// This is correct for async/await where the same Task may hop between threads.
public enum ActiveTransactionScope: Sendable {
    @TaskLocal public static var current: (any Transaction)? = nil
}
