import DatabaseTypes
import StorageKit
import Synchronization

/// Sendable, synchronized access point for the SQLite connection.
///
/// SQLite itself is opened in FULLMUTEX mode, but the wrapper also serializes
/// access at the Swift boundary and keeps the non-Sendable C handle out of
/// `SQLiteStorageTransaction`.
final class SQLiteConnectionHandle: Sendable {
    private let connection: Mutex<SQLiteConnection?>

    init(path: String, busyTimeoutMilliseconds: Int32 = 100) throws {
        let connection = try SQLiteConnection(path: path)
        try connection.initialize(
            busyTimeoutMilliseconds: busyTimeoutMilliseconds
        )
        self.connection = Mutex(connection)
    }

    func execute(_ sql: String, operation: StorageOperation = .execute) throws {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            try connection.execute(sql, operation: operation)
        }
    }

    func insertOrReplace(key: ByteString, value: ByteString) throws {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            try connection.insertOrReplace(key: key, value: value)
        }
    }

    func get(key: ByteString) throws -> ByteString? {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            return try connection.get(key: key)
        }
    }

    func getKey(plan: SQLiteKeySelectionPlan) throws -> ByteString? {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            return try connection.getKey(plan: plan)
        }
    }

    func delete(key: ByteString) throws {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            try connection.delete(key: key)
        }
    }

    func deleteRange(begin: ByteString, end: ByteString) throws {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            try connection.deleteRange(begin: begin, end: end)
        }
    }

    func openRangeCursor(
        ownerTransactionIdentifier: UInt64,
        begin: SQLRangeBoundary,
        end: SQLRangeBoundary,
        limit: Int,
        reverse: Bool
    ) throws -> UInt64 {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            return try connection.openRangeCursor(
                ownerTransactionIdentifier: ownerTransactionIdentifier,
                begin: begin,
                end: end,
                limit: limit,
                reverse: reverse
            )
        }
    }

    func nextRangeCursor(
        identifier: UInt64
    ) throws -> (key: ByteString, value: ByteString)? {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            return try connection.nextRangeCursor(identifier: identifier)
        }
    }

    func closeRangeCursor(identifier: UInt64) {
        connection.withLock { connection in
            connection?.closeRangeCursor(identifier: identifier)
        }
    }

    func closeRangeCursors(ownerTransactionIdentifier: UInt64) {
        connection.withLock { connection in
            connection?.closeRangeCursors(
                ownerTransactionIdentifier: ownerTransactionIdentifier
            )
        }
    }

    var rangeInstrumentation: SQLiteRangeInstrumentation {
        connection.withLock { connection in
            connection?.rangeInstrumentation
                ?? SQLiteRangeInstrumentation(
                    prepareCount: 0,
                    stepCount: 0,
                    payloadCopyCount: 0,
                    finalizeCount: 0,
                    openCursorCount: 0
                )
        }
    }

    func pragmaInt64(_ name: String) throws -> Int64 {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            return try connection.pragmaInt64(name)
        }
    }

    func runIncrementalVacuum(
        maximumPages: UInt64
    ) throws -> SQLiteIncrementalCompactionMetrics {
        try connection.withLock { connection in
            let connection = try Self.unwrap(connection)
            return try connection.runIncrementalVacuum(maximumPages: maximumPages)
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
