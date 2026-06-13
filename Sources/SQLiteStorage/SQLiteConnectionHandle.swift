import StorageKit
import Synchronization

/// Sendable, synchronized access point for the SQLite connection.
///
/// SQLite itself is opened in FULLMUTEX mode, but the wrapper also serializes
/// access at the Swift boundary and keeps the non-Sendable C handle out of
/// `SQLiteStorageTransaction`.
final class SQLiteConnectionHandle: Sendable {
    private let connection: Mutex<SQLiteConnection?>

    init(path: String) throws {
        let connection = try SQLiteConnection(path: path)
        try connection.initialize()
        self.connection = Mutex(connection)
    }

    func execute(_ sql: String, operation: StorageOperation = .execute) throws {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            try connection.execute(sql, operation: operation)
        }
    }

    func insertOrReplace(key: Bytes, value: Bytes) throws {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            try connection.insertOrReplace(key: key, value: value)
        }
    }

    func get(key: Bytes) throws -> Bytes? {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            return try connection.get(key: key)
        }
    }

    func delete(key: Bytes) throws {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            try connection.delete(key: key)
        }
    }

    func deleteRange(begin: Bytes, end: Bytes) throws {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            try connection.deleteRange(begin: begin, end: end)
        }
    }

    func getRange(
        begin: SQLRangeBoundary,
        end: SQLRangeBoundary,
        limit: Int,
        reverse: Bool
    ) throws -> [(key: Bytes, value: Bytes)] {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            return try connection.getRange(begin: begin, end: end, limit: limit, reverse: reverse)
        }
    }

    func getAllEntries() throws -> [(key: Bytes, value: Bytes)] {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            return try connection.getAllEntries()
        }
    }

    func close() {
        connection.withLock { connection in
            connection?.close()
            connection = nil
        }
    }

    private static func unwrap(_ connection: SQLiteConnection?) throws -> SQLiteConnection {
        guard let connection else {
            throw StorageError(
                code: .invalidOperation,
                operation: .unknown,
                backend: .sqlite,
                message: "Database closed"
            )
        }
        return connection
    }
}
