import StorageKit
import Foundation

/// SQLite backend StorageEngine implementation.
///
/// Uses a `WITHOUT ROWID` table for efficient BLOB primary key B-tree storage.
/// SQLite is single-writer, so transactions are serialized with `NSLock`.
///
/// ## Usage
/// ```swift
/// // File-based
/// let engine = try SQLiteStorageEngine(path: "/path/to/db.sqlite")
///
/// // In-memory (for testing)
/// let engine = try SQLiteStorageEngine()
/// ```
public final class SQLiteStorageEngine: StorageEngine, @unchecked Sendable {
    public typealias TransactionType = SQLiteStorageTransaction

    private let lock = NSLock()
    private var connection: SQLiteConnection?

    /// Opens a file-based database.
    public init(path: String) throws {
        let conn = try SQLiteConnection(path: path)
        try conn.initialize()
        self.connection = conn
    }

    /// Opens an in-memory database (for testing).
    public init() throws {
        let conn = try SQLiteConnection(path: ":memory:")
        try conn.initialize()
        self.connection = conn
    }

    public func createTransaction() throws -> SQLiteStorageTransaction {
        lock.lock()
        guard let conn = connection else {
            lock.unlock()
            throw StorageError.invalidOperation("Database closed")
        }
        do {
            try conn.execute("BEGIN IMMEDIATE")
        } catch {
            lock.unlock()
            throw error
        }
        return SQLiteStorageTransaction(connection: conn, lock: lock)
    }

    public func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        let tx = try createTransaction()
        do {
            let result = try await operation(tx)
            try await tx.commit()
            return result
        } catch {
            tx.cancel()
            throw error
        }
    }

    /// Closes the database connection.
    public func close() {
        lock.lock()
        defer { lock.unlock() }
        connection?.close()
        connection = nil
    }
}
