import SQLite3
import StorageKit

/// Thin wrapper around the SQLite3 C API.
///
/// Not thread-safe — callers must provide external synchronization.
/// Uses a `WITHOUT ROWID` table for efficient BLOB primary key B-tree storage.
final class SQLiteConnection {
    private var db: OpaquePointer?

    /// Opens a database at the given file path, or ":memory:" for in-memory.
    init(path: String) throws {
        var dbPointer: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let rc = sqlite3_open_v2(path, &dbPointer, flags, nil)
        guard rc == SQLITE_OK, let opened = dbPointer else {
            let message = dbPointer.map { String(cString: sqlite3_errmsg($0)) } ?? "Unknown error"
            sqlite3_close(dbPointer)
            throw StorageError.backendError("SQLite open failed: \(message)")
        }
        self.db = opened
    }

    /// Creates the KV table and enables WAL mode.
    func initialize() throws {
        try execute("PRAGMA journal_mode=WAL")
        try execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key BLOB NOT NULL PRIMARY KEY,
                value BLOB NOT NULL
            ) WITHOUT ROWID
            """)
    }

    /// Executes a SQL statement without parameter bindings.
    func execute(_ sql: String) throws {
        guard let db else {
            throw StorageError.invalidOperation("Database closed")
        }
        var errorMessage: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(db, sql, nil, nil, &errorMessage)
        if rc != SQLITE_OK {
            let message = errorMessage.map { String(cString: $0) } ?? "Unknown error"
            sqlite3_free(errorMessage)
            throw StorageError.backendError("SQLite exec failed: \(message)")
        }
    }

    /// INSERT OR REPLACE (key, value)
    func insertOrReplace(key: Bytes, value: Bytes) throws {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let sql = "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)"
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }

        try prepareStatement(sql, into: &stmt)
        bindBlob(stmt, index: 1, data: key)
        bindBlob(stmt, index: 2, data: value)

        let rc = sqlite3_step(stmt)
        guard rc == SQLITE_DONE else {
            throw StorageError.backendError("SQLite insert failed: \(currentErrorMessage)")
        }
    }

    /// SELECT value WHERE key = ?
    func get(key: Bytes) throws -> Bytes? {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let sql = "SELECT value FROM kv_store WHERE key = ?"
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }

        try prepareStatement(sql, into: &stmt)
        bindBlob(stmt, index: 1, data: key)

        let rc = sqlite3_step(stmt)
        if rc == SQLITE_ROW {
            return extractBlob(stmt, column: 0)
        } else if rc == SQLITE_DONE {
            return nil
        } else {
            throw StorageError.backendError("SQLite get failed: \(currentErrorMessage)")
        }
    }

    /// DELETE WHERE key = ?
    func delete(key: Bytes) throws {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let sql = "DELETE FROM kv_store WHERE key = ?"
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }

        try prepareStatement(sql, into: &stmt)
        bindBlob(stmt, index: 1, data: key)

        let rc = sqlite3_step(stmt)
        guard rc == SQLITE_DONE else {
            throw StorageError.backendError("SQLite delete failed: \(currentErrorMessage)")
        }
    }

    /// DELETE WHERE key >= ? AND key < ?
    func deleteRange(begin: Bytes, end: Bytes) throws {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let sql = "DELETE FROM kv_store WHERE key >= ? AND key < ?"
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }

        try prepareStatement(sql, into: &stmt)
        bindBlob(stmt, index: 1, data: begin)
        bindBlob(stmt, index: 2, data: end)

        let rc = sqlite3_step(stmt)
        guard rc == SQLITE_DONE else {
            throw StorageError.backendError("SQLite deleteRange failed: \(currentErrorMessage)")
        }
    }

    /// SELECT key, value WHERE key >= ? AND key < ? ORDER BY key [DESC] LIMIT ?
    func getRange(
        begin: Bytes,
        end: Bytes,
        limit: Int,
        reverse: Bool
    ) throws -> [(key: Bytes, value: Bytes)] {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let order = reverse ? "DESC" : "ASC"
        let limitClause = limit > 0 ? "LIMIT ?" : ""
        let sql = "SELECT key, value FROM kv_store WHERE key >= ? AND key < ? ORDER BY key \(order) \(limitClause)"
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }

        try prepareStatement(sql, into: &stmt)
        bindBlob(stmt, index: 1, data: begin)
        bindBlob(stmt, index: 2, data: end)
        if limit > 0 {
            sqlite3_bind_int64(stmt, 3, Int64(limit))
        }

        var results: [(key: Bytes, value: Bytes)] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            guard let key = extractBlob(stmt, column: 0),
                  let value = extractBlob(stmt, column: 1) else {
                continue
            }
            results.append((key: key, value: value))
        }
        return results
    }

    /// Closes the database connection.
    func close() {
        guard let db else { return }
        sqlite3_close_v2(db)
        self.db = nil
    }

    deinit {
        close()
    }

    // MARK: - Internal Helpers

    private func prepareStatement(_ sql: String, into stmt: inout OpaquePointer?) throws {
        guard let db else {
            throw StorageError.invalidOperation("Database closed")
        }
        let rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
        guard rc == SQLITE_OK else {
            throw StorageError.backendError("SQLite prepare failed: \(currentErrorMessage)")
        }
    }

    private func bindBlob(_ stmt: OpaquePointer?, index: Int32, data: Bytes) {
        data.withUnsafeBufferPointer { buf in
            _ = sqlite3_bind_blob(stmt, index, buf.baseAddress, Int32(buf.count), SQLITE_TRANSIENT_PTR)
        }
    }

    private var currentErrorMessage: String {
        guard let db else { return "Database closed" }
        return String(cString: sqlite3_errmsg(db))
    }

    private func extractBlob(_ stmt: OpaquePointer?, column: Int32) -> Bytes? {
        guard let blob = sqlite3_column_blob(stmt, column) else { return nil }
        let count = Int(sqlite3_column_bytes(stmt, column))
        return Array(UnsafeBufferPointer(start: blob.assumingMemoryBound(to: UInt8.self), count: count))
    }
}

// MARK: - SQLITE_TRANSIENT workaround

/// Equivalent to the C macro `((sqlite3_destructor_type)-1)` for SQLITE_TRANSIENT.
private let SQLITE_TRANSIENT_PTR = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
