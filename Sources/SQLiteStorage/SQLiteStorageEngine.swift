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
/// let engine = try SQLiteStorageEngine(configuration: .file("/path/to/db.sqlite"))
///
/// // In-memory (for testing)
/// let engine = try SQLiteStorageEngine(configuration: .inMemory)
/// ```
public final class SQLiteStorageEngine: StorageEngine, Sendable {

    public struct Configuration: Sendable {
        public var path: String

        public init(path: String) {
            self.path = path
        }

        /// File-based database.
        public static func file(_ path: String) -> Configuration {
            Configuration(path: path)
        }

        /// In-memory database (for testing).
        public static var inMemory: Configuration {
            Configuration(path: ":memory:")
        }
    }

    public typealias TransactionType = SQLiteStorageTransaction

    private let transactionLock = NSLock()
    private let _connection: Mutex<SQLiteConnection?>

    public init(configuration: Configuration) throws {
        let conn = try SQLiteConnection(path: configuration.path)
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
