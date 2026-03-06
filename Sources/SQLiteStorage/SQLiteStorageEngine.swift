import StorageKit
import Foundation
import Synchronization

/// SQLite backend StorageEngine implementation.
///
/// Uses a `WITHOUT ROWID` table for efficient BLOB primary key B-tree storage.
/// SQLite is single-writer, so transactions are serialized with `NSLock`.
///
/// ## Nested Transaction Safety
///
/// SQLite does not support concurrent transactions on a single connection.
/// This engine uses `ActiveTransactionScope` (TaskLocal) to detect nested
/// `withTransaction()` / `createTransaction()` calls and reuse the existing
/// transaction instead of acquiring a new lock (which would deadlock).
///
/// ## Usage
/// ```swift
/// // File-based
/// let engine = try SQLiteStorageEngine(path: "/path/to/db.sqlite")
///
/// // In-memory (for testing)
/// let engine = try SQLiteStorageEngine()
/// ```
public final class SQLiteStorageEngine: StorageEngine, Sendable {
    public typealias TransactionType = SQLiteStorageTransaction

    private let transactionLock = NSLock()
    private let _connection: Mutex<SQLiteConnection?>

    /// Opens a file-based database.
    public init(path: String) throws {
        let conn = try SQLiteConnection(path: path)
        try conn.initialize()
        self._connection = Mutex(conn)
    }

    /// Opens an in-memory database (for testing).
    public init() throws {
        let conn = try SQLiteConnection(path: ":memory:")
        try conn.initialize()
        self._connection = Mutex(conn)
    }

    public func createTransaction() throws -> SQLiteStorageTransaction {
        // Detect nested call via TaskLocal — return child transaction (no lock, no BEGIN)
        if let existing = ActiveTransactionScope.current as? SQLiteStorageTransaction {
            return SQLiteStorageTransaction(connection: existing.connection, lock: nil)
        }

        transactionLock.lock()
        guard let conn = _connection.withLock({ $0 }) else {
            transactionLock.unlock()
            throw StorageError.invalidOperation("Database closed")
        }
        do {
            try conn.execute("BEGIN IMMEDIATE")
        } catch {
            transactionLock.unlock()
            throw error
        }
        return SQLiteStorageTransaction(connection: conn, lock: transactionLock)
    }

    public func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        // Detect nested call via TaskLocal — reuse existing transaction
        if let existing = ActiveTransactionScope.current {
            return try await operation(existing)
        }

        let tx = try createTransaction()
        return try await ActiveTransactionScope.$current.withValue(tx) {
            do {
                let result = try await operation(tx)
                try await tx.commit()
                return result
            } catch {
                tx.cancel()
                throw error
            }
        }
    }

    /// Closes the database connection.
    public func close() {
        transactionLock.lock()
        defer { transactionLock.unlock() }
        _connection.withLock { conn in
            conn?.close()
            conn = nil
        }
    }

    public func shutdown() {
        close()
    }
}
