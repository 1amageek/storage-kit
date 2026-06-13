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
/// calls without acquiring a second writer lock. Nested `withTransaction()`
/// reuses the active transaction. Nested `createTransaction()` returns a
/// buffered child transaction whose commit merges into the parent and whose
/// cancel only discards child writes.
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
    private let connection: SQLiteConnectionHandle

    public init(configuration: Configuration) throws {
        self.connection = try SQLiteConnectionHandle(path: configuration.path)
    }

    public func createTransaction() throws -> SQLiteStorageTransaction {
        // Detect nested call via TaskLocal and return a buffered child.
        if let existing = ActiveTransactionScope.current as? SQLiteStorageTransaction {
            return SQLiteStorageTransaction(parent: existing)
        }

        transactionLock.lock()
        do {
            try connection.execute("BEGIN IMMEDIATE", operation: .beginTransaction)
        } catch {
            transactionLock.unlock()
            throw error
        }
        return SQLiteStorageTransaction(connection: connection, lock: transactionLock)
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
        connection.close()
    }

    public func shutdown() {
        close()
    }
}
