import SQLite3
import StorageKit

/// SQLite copies transient cursor bindings before the Swift borrow ends.
private let sqliteTransientDestructor = unsafeBitCast(
    -1,
    to: sqlite3_destructor_type.self
)

/// Thin wrapper around the SQLite3 C API.
///
/// Not thread-safe — callers must provide external synchronization.
/// Uses a `WITHOUT ROWID` table for efficient BLOB primary key B-tree storage.
final class SQLiteConnection {
    private var db: OpaquePointer?
    private var rangeCursors: [UInt64: RangeCursor] = [:]
    private var rangeCursorIdentifiersByOwner: [UInt64: Set<UInt64>] = [:]
    private var nextRangeCursorIdentifier: UInt64 = 1
    private var rangePrepareCount: UInt64 = 0
    private var rangeStepCount: UInt64 = 0
    private var rangePayloadCopyCount: UInt64 = 0
    private var rangeFinalizeCount: UInt64 = 0

    private struct RangeCursor {
        let statement: OpaquePointer
        let ownerTransactionIdentifier: UInt64
    }

    /// Opens a database at the given file path, or ":memory:" for in-memory.
    init(path: String) throws {
        var dbPointer: OpaquePointer?
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX
        let rc = sqlite3_open_v2(path, &dbPointer, flags, nil)
        guard rc == SQLITE_OK, let opened = dbPointer else {
            let message = dbPointer.map { String(cString: sqlite3_errmsg($0)) } ?? "Unknown error"
            sqlite3_close(dbPointer)
            throw SQLiteErrorMapper.map(rc: rc, operation: .open, message: message)
        }
        self.db = opened
    }

    /// Creates the KV table and enables WAL mode.
    func initialize() throws {
        let applicationTableCount = try scalarInt64(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )
        let autoVacuumMode = try pragmaInt64("auto_vacuum")
        if applicationTableCount == 0, autoVacuumMode == 0 {
            try execute("PRAGMA auto_vacuum=INCREMENTAL")
        }
        try execute("PRAGMA journal_mode=WAL")
        try execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key BLOB NOT NULL PRIMARY KEY,
                value BLOB NOT NULL
            ) WITHOUT ROWID
            """)
    }

    /// Returns the integer result of a single-row SQL query.
    func scalarInt64(_ sql: String) throws -> Int64 {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }

        try prepareStatement(sql, into: &stmt)
        let rc = sqlite3_step(stmt)
        guard rc == SQLITE_ROW else {
            throw SQLiteErrorMapper.map(rc: rc, operation: .read, message: currentErrorMessage)
        }
        return sqlite3_column_int64(stmt, 0)
    }

    /// Returns an integer SQLite pragma value.
    func pragmaInt64(_ name: String) throws -> Int64 {
        try scalarInt64("PRAGMA \(name)")
    }

    /// Reclaims at most `maximumPages` from SQLite's freelist.
    func runIncrementalVacuum(maximumPages: UInt64) throws -> SQLiteIncrementalCompactionMetrics {
        let freePagesBefore = try nonnegativeUInt64(pragmaInt64("freelist_count"))
        try execute("PRAGMA incremental_vacuum(\(maximumPages))")
        let freePagesAfter = try nonnegativeUInt64(pragmaInt64("freelist_count"))
        return SQLiteIncrementalCompactionMetrics(
            freePagesBefore: freePagesBefore,
            freePagesAfter: freePagesAfter
        )
    }

    /// Executes a SQL statement without parameter bindings.
    func execute(_ sql: String, operation: StorageOperation = .execute) throws {
        guard let db else {
            throw StorageError.invalidOperation("Database closed")
        }
        var errorMessage: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(db, sql, nil, nil, &errorMessage)
        if rc != SQLITE_OK {
            let message = errorMessage.map { String(cString: $0) } ?? "Unknown error"
            sqlite3_free(errorMessage)
            throw SQLiteErrorMapper.map(rc: rc, operation: operation, message: message)
        }
    }

    /// INSERT OR REPLACE (key, value)
    func insertOrReplace(key: Bytes, value: Bytes) throws {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let sql = "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)"
        try withPreparedStatement(
            sql,
            blobBindings: [key, value],
            operation: .write
        ) { stmt in
            let rc = sqlite3_step(stmt)
            guard rc == SQLITE_DONE else {
                throw SQLiteErrorMapper.map(
                    rc: rc,
                    operation: .write,
                    message: currentErrorMessage
                )
            }
        }
    }

    /// SELECT value WHERE key = ?
    func get(key: Bytes) throws -> Bytes? {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let sql = "SELECT value FROM kv_store WHERE key = ?"
        return try withPreparedStatement(
            sql,
            blobBindings: [key],
            operation: .read
        ) { stmt in
            let rc = sqlite3_step(stmt)
            if rc == SQLITE_ROW {
                return try extractBlob(
                    stmt,
                    column: 0,
                    operation: .read
                )
            }
            if rc == SQLITE_DONE {
                return nil
            }
            throw SQLiteErrorMapper.map(
                rc: rc,
                operation: .read,
                message: currentErrorMessage
            )
        }
    }

    /// Resolves one key selector without materializing a range value payload.
    func getKey(plan: SQLiteKeySelectionPlan) throws -> Bytes? {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let sql = "SELECT key FROM kv_store WHERE key \(plan.comparison.sql) ? "
            + "ORDER BY key \(plan.order.sql) LIMIT 1 OFFSET ?"
        return try withPreparedStatement(
            sql,
            blobBindings: [plan.key],
            operation: .rangeRead
        ) { statement in
            let bindResult = sqlite3_bind_int64(
                statement,
                2,
                plan.offset
            )
            guard bindResult == SQLITE_OK else {
                throw SQLiteErrorMapper.map(
                    rc: bindResult,
                    operation: .rangeRead,
                    message: currentErrorMessage
                )
            }

            let stepResult = sqlite3_step(statement)
            if stepResult == SQLITE_DONE { return nil }
            guard stepResult == SQLITE_ROW else {
                throw SQLiteErrorMapper.map(
                    rc: stepResult,
                    operation: .rangeRead,
                    message: currentErrorMessage
                )
            }
            guard let key = try extractBlob(
                statement,
                column: 0,
                operation: .rangeRead
            ) else {
                throw StorageError(
                    code: .dataCorruption,
                    operation: .rangeRead,
                    backend: .sqlite,
                    message: "SQLite returned NULL for a NOT NULL key column"
                )
            }
            return key
        }
    }

    /// DELETE WHERE key = ?
    func delete(key: Bytes) throws {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let sql = "DELETE FROM kv_store WHERE key = ?"
        try withPreparedStatement(
            sql,
            blobBindings: [key],
            operation: .delete
        ) { stmt in
            let rc = sqlite3_step(stmt)
            guard rc == SQLITE_DONE else {
                throw SQLiteErrorMapper.map(
                    rc: rc,
                    operation: .delete,
                    message: currentErrorMessage
                )
            }
        }
    }

    /// DELETE WHERE key >= ? AND key < ?
    func deleteRange(begin: Bytes, end: Bytes) throws {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        let sql = "DELETE FROM kv_store WHERE key >= ? AND key < ?"
        try withPreparedStatement(
            sql,
            blobBindings: [begin, end],
            operation: .deleteRange
        ) { stmt in
            let rc = sqlite3_step(stmt)
            guard rc == SQLITE_DONE else {
                throw SQLiteErrorMapper.map(
                    rc: rc,
                    operation: .deleteRange,
                    message: currentErrorMessage
                )
            }
        }
    }

    /// Prepares a lazy range cursor without stepping or copying result rows.
    ///
    /// SQLite must retain the boundary values beyond this call. The bindings
    /// therefore use `SQLITE_TRANSIENT`, which performs the single required
    /// copy at the API lifetime boundary instead of retaining an escaped Swift
    /// borrow. Result payloads remain untouched until `nextRangeCursor`.
    func openRangeCursor(
        ownerTransactionIdentifier: UInt64,
        begin: SQLRangeBoundary,
        end: SQLRangeBoundary,
        limit: Int,
        reverse: Bool
    ) throws -> UInt64 {
        guard db != nil else {
            throw StorageError.invalidOperation("Database closed")
        }
        guard limit >= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .sqlite,
                message: "SQLite range limit must not be negative"
            )
        }

        var clauses: [String] = []
        clauses.reserveCapacity(2)
        var bindings: [Bytes] = []
        bindings.reserveCapacity(2)
        appendRangeBoundary(begin, clauses: &clauses, bindings: &bindings)
        appendRangeBoundary(end, clauses: &clauses, bindings: &bindings)

        let order = reverse ? "DESC" : "ASC"
        let limitClause = limit > 0 ? " LIMIT ?" : ""
        let sql = "SELECT key, value FROM kv_store WHERE \(clauses[0]) AND \(clauses[1]) ORDER BY key \(order)\(limitClause)"

        var statement: OpaquePointer?
        rangePrepareCount &+= 1
        do {
            try prepareStatement(sql, into: &statement)
            guard let statement else {
                throw StorageError(
                    code: .backendFailure,
                    operation: .prepare,
                    backend: .sqlite,
                    message: "SQLite did not return a prepared range statement"
                )
            }
            for (offset, value) in bindings.enumerated() {
                try bindTransientBlob(
                    value,
                    statement: statement,
                    parameterIndex: Int32(offset + 1),
                    operation: .rangeRead
                )
            }
            if limit > 0 {
                guard let sqliteLimit = Int64(exactly: limit) else {
                    throw StorageError(
                        code: .resourceUnavailable,
                        operation: .rangeRead,
                        backend: .sqlite,
                        message: "SQLite range limit exceeds Int64.max"
                    )
                }
                let rc = sqlite3_bind_int64(
                    statement,
                    Int32(bindings.count + 1),
                    sqliteLimit
                )
                guard rc == SQLITE_OK else {
                    throw SQLiteErrorMapper.map(
                        rc: rc,
                        operation: .rangeRead,
                        message: currentErrorMessage
                    )
                }
            }

            let identifier = nextRangeCursorIdentifier
            let (nextIdentifier, overflow) = identifier.addingReportingOverflow(1)
            guard !overflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    backend: .sqlite,
                    message: "SQLite range cursor identifier space is exhausted"
                )
            }
            nextRangeCursorIdentifier = nextIdentifier
            rangeCursors[identifier] = RangeCursor(
                statement: statement,
                ownerTransactionIdentifier: ownerTransactionIdentifier
            )
            rangeCursorIdentifiersByOwner[
                ownerTransactionIdentifier,
                default: []
            ].insert(identifier)
            return identifier
        } catch {
            if let statement {
                sqlite3_finalize(statement)
                rangeFinalizeCount &+= 1
            }
            throw error
        }
    }

    /// Advances exactly one SQLite row and copies key/value directly into their
    /// final owners. SQLite invalidates both column pointers at the next step,
    /// reset, or finalize, so one owned copy per column is mandatory here.
    func nextRangeCursor(
        identifier: UInt64
    ) throws -> (key: Bytes, value: Bytes)? {
        guard let cursor = rangeCursors[identifier] else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .sqlite,
                message: "SQLite range cursor is not open"
            )
        }

        rangeStepCount &+= 1
        let rc = sqlite3_step(cursor.statement)
        if rc == SQLITE_ROW {
            do {
                guard let key = try extractBlob(
                    cursor.statement,
                    column: 0,
                    operation: .rangeRead
                ), let value = try extractBlob(
                    cursor.statement,
                    column: 1,
                    operation: .rangeRead
                ) else {
                    throw StorageError(
                        code: .dataCorruption,
                        operation: .rangeRead,
                        backend: .sqlite,
                        message: "SQLite returned NULL for a NOT NULL key-value column"
                    )
                }
                return (key: key, value: value)
            } catch {
                finalizeRangeCursor(identifier: identifier)
                throw error
            }
        }
        if rc == SQLITE_DONE {
            finalizeRangeCursor(identifier: identifier)
            return nil
        }

        let error = SQLiteErrorMapper.map(
            rc: rc,
            operation: .rangeRead,
            message: currentErrorMessage
        )
        finalizeRangeCursor(identifier: identifier)
        throw error
    }

    func closeRangeCursor(identifier: UInt64) {
        finalizeRangeCursor(identifier: identifier)
    }

    func closeRangeCursors(ownerTransactionIdentifier: UInt64) {
        while let identifier = rangeCursorIdentifiersByOwner[
            ownerTransactionIdentifier
        ]?.first {
            finalizeRangeCursor(identifier: identifier)
        }
    }

    var rangeInstrumentation: SQLiteRangeInstrumentation {
        SQLiteRangeInstrumentation(
            prepareCount: rangePrepareCount,
            stepCount: rangeStepCount,
            payloadCopyCount: rangePayloadCopyCount,
            finalizeCount: rangeFinalizeCount,
            openCursorCount: rangeCursors.count
        )
    }

    /// Closes the database connection.
    func close() {
        guard let db else { return }
        while let identifier = rangeCursors.keys.first {
            finalizeRangeCursor(identifier: identifier)
        }
        sqlite3_close_v2(db)
        self.db = nil
    }

    deinit {
        close()
    }

    // MARK: - Statement Preparation and Range Binding

    private func prepareStatement(_ sql: String, into stmt: inout OpaquePointer?) throws {
        guard let db else {
            throw StorageError.invalidOperation("Database closed")
        }
        let rc = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
        guard rc == SQLITE_OK else {
            throw SQLiteErrorMapper.map(rc: rc, operation: .prepare, message: currentErrorMessage)
        }
    }

    private func appendRangeBoundary(
        _ boundary: SQLRangeBoundary,
        clauses: inout [String],
        bindings: inout [Bytes]
    ) {
        switch boundary {
        case .direct(let operation, let key):
            clauses.append("key \(operation) ?")
            bindings.append(key)
        case .resolvedSubquery(let operation, let subqueryOperation, let key):
            clauses.append(
                "key \(operation) COALESCE((SELECT max(key) FROM kv_store WHERE key \(subqueryOperation) ?), x'')"
            )
            bindings.append(key)
        }
    }

    private func bindTransientBlob(
        _ value: Bytes,
        statement: OpaquePointer,
        parameterIndex: Int32,
        operation: StorageOperation
    ) throws {
        try value.withUnsafeBytes { bytes in
            let rc: Int32
            if bytes.isEmpty {
                rc = sqlite3_bind_zeroblob(statement, parameterIndex, 0)
            } else if let byteCount = Int32(exactly: bytes.count) {
                rc = sqlite3_bind_blob(
                    statement,
                    parameterIndex,
                    bytes.baseAddress,
                    byteCount,
                    sqliteTransientDestructor
                )
            } else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: operation,
                    backend: .sqlite,
                    message: "SQLite blob binding exceeds Int32.max bytes"
                )
            }
            guard rc == SQLITE_OK else {
                throw SQLiteErrorMapper.map(
                    rc: rc,
                    operation: operation,
                    message: currentErrorMessage
                )
            }
        }
    }

    private func finalizeRangeCursor(identifier: UInt64) {
        guard let cursor = rangeCursors.removeValue(forKey: identifier) else {
            return
        }
        rangeCursorIdentifiersByOwner[
            cursor.ownerTransactionIdentifier
        ]?.remove(identifier)
        if rangeCursorIdentifiersByOwner[
            cursor.ownerTransactionIdentifier
        ]?.isEmpty == true {
            rangeCursorIdentifiersByOwner[
                cursor.ownerTransactionIdentifier
            ] = nil
        }
        sqlite3_finalize(cursor.statement)
        rangeFinalizeCount &+= 1
    }

    /// Keeps every blob borrow active through all SQLite steps and clears the
    /// static bindings before any borrow scope ends.
    private func withPreparedStatement<Output>(
        _ sql: String,
        blobBindings: [Bytes],
        operation: StorageOperation,
        _ body: (OpaquePointer?) throws -> Output
    ) throws -> Output {
        var stmt: OpaquePointer?
        do {
            try prepareStatement(sql, into: &stmt)
        } catch {
            sqlite3_finalize(stmt)
            throw error
        }
        let finalizeOnCleanupFailure = { () -> Int32 in
            guard let statement = stmt else { return SQLITE_OK }
            stmt = nil
            return sqlite3_finalize(statement)
        }
        defer {
            if let statement = stmt {
                sqlite3_finalize(statement)
            }
        }
        return try withBoundBlobs(
            blobBindings,
            at: 0,
            statement: stmt,
            operation: operation,
            finalizeOnCleanupFailure: finalizeOnCleanupFailure,
            body
        )
    }

    private func withBoundBlobs<Output>(
        _ bindings: [Bytes],
        at offset: Int,
        statement: OpaquePointer?,
        operation: StorageOperation,
        finalizeOnCleanupFailure: () -> Int32,
        _ body: (OpaquePointer?) throws -> Output
    ) throws -> Output {
        guard offset < bindings.count else {
            let outcome: Result<Output, any Error>
            do {
                outcome = .success(try body(statement))
            } catch {
                outcome = .failure(error)
            }
            let clearResult = sqlite3_clear_bindings(statement)
            guard clearResult == SQLITE_OK else {
                let originalError: (any Error)?
                switch outcome {
                case .success:
                    originalError = nil
                case .failure(let error):
                    originalError = error
                }
                let finalizeResult = finalizeOnCleanupFailure()
                throw bindingCleanupError(
                    clearResult: clearResult,
                    finalizeResult: finalizeResult,
                    originalError: originalError,
                    operation: operation
                )
            }
            return try outcome.get()
        }

        return try bindings[offset].withUnsafeBytes { buffer in
            let parameterIndex = Int32(offset + 1)
            let bindResult: Int32
            if buffer.isEmpty {
                bindResult = sqlite3_bind_zeroblob(
                    statement,
                    parameterIndex,
                    0
                )
            } else if let byteCount = Int32(exactly: buffer.count) {
                bindResult = sqlite3_bind_blob(
                    statement,
                    parameterIndex,
                    buffer.baseAddress,
                    byteCount,
                    nil
                )
            } else {
                let error = StorageError(
                    code: .resourceUnavailable,
                    operation: operation,
                    backend: .sqlite,
                    message: "SQLite blob binding exceeds Int32.max bytes"
                )
                throw failureAfterClearingBindings(
                    error,
                    statement: statement,
                    operation: operation,
                    finalizeOnCleanupFailure: finalizeOnCleanupFailure
                )
            }

            guard bindResult == SQLITE_OK else {
                let error = SQLiteErrorMapper.map(
                    rc: bindResult,
                    operation: operation,
                    message: currentErrorMessage
                )
                throw failureAfterClearingBindings(
                    error,
                    statement: statement,
                    operation: operation,
                    finalizeOnCleanupFailure: finalizeOnCleanupFailure
                )
            }
            return try withBoundBlobs(
                bindings,
                at: offset + 1,
                statement: statement,
                operation: operation,
                finalizeOnCleanupFailure: finalizeOnCleanupFailure,
                body
            )
        }
    }

    private func failureAfterClearingBindings(
        _ originalError: any Error,
        statement: OpaquePointer?,
        operation: StorageOperation,
        finalizeOnCleanupFailure: () -> Int32
    ) -> any Error {
        let clearResult = sqlite3_clear_bindings(statement)
        guard clearResult != SQLITE_OK else {
            return originalError
        }
        let finalizeResult = finalizeOnCleanupFailure()
        return bindingCleanupError(
            clearResult: clearResult,
            finalizeResult: finalizeResult,
            originalError: originalError,
            operation: operation
        )
    }

    private func bindingCleanupError(
        clearResult: Int32,
        finalizeResult: Int32,
        originalError: (any Error)?,
        operation: StorageOperation
    ) -> StorageError {
        var details = "clearBindingsRC=\(clearResult); finalizeRC=\(finalizeResult)"
        if let originalError {
            details += "; original=\(String(describing: originalError))"
        }
        return StorageError(
            code: .backendFailure,
            operation: operation,
            backend: .sqlite,
            message: "SQLite statement binding cleanup failed",
            underlyingDescription: details
        )
    }

    private var currentErrorMessage: String {
        guard let db else { return "Database closed" }
        return String(cString: sqlite3_errmsg(db))
    }

    private func extractBlob(
        _ stmt: OpaquePointer?,
        column: Int32,
        operation: StorageOperation
    ) throws -> Bytes? {
        let colType = sqlite3_column_type(stmt, column)
        guard colType != SQLITE_NULL else { return nil }
        let blob = sqlite3_column_blob(stmt, column)
        let count = Int(sqlite3_column_bytes(stmt, column))
        if let db, sqlite3_errcode(db) == SQLITE_NOMEM {
            throw SQLiteErrorMapper.map(
                rc: SQLITE_NOMEM,
                operation: operation,
                message: currentErrorMessage
            )
        }
        guard count >= 0 else {
            throw StorageError(
                code: .dataCorruption,
                operation: operation,
                backend: .sqlite,
                message: "SQLite returned a negative blob byte count"
            )
        }
        if count == 0 { return [] }
        guard let blob else {
            throw StorageError(
                code: .dataCorruption,
                operation: operation,
                backend: .sqlite,
                message: "SQLite returned a null blob pointer for a non-empty value"
            )
        }
        // This copy is required at the SQLite lifetime boundary because the
        // column pointer becomes invalid after the next step, reset, or finalize.
        if operation == .rangeRead {
            rangePayloadCopyCount &+= 1
        }
        return Bytes.copying(count: count) { destination in
            destination.copyMemory(
                from: UnsafeRawBufferPointer(start: blob, count: count)
            )
        }
    }

    private func nonnegativeUInt64(_ value: Int64) throws -> UInt64 {
        guard value >= 0 else {
            throw StorageError(
                code: .dataCorruption,
                operation: .read,
                backend: .sqlite,
                message: "SQLite returned a negative page count",
                underlyingDescription: "value=\(value)"
            )
        }
        return UInt64(value)
    }
}

private enum SQLiteErrorMapper {
    static func map(rc: Int32, operation: StorageOperation, message: String) -> StorageError {
        let code: StorageError.Code
        switch rc {
        case SQLITE_BUSY, SQLITE_LOCKED:
            code = .transactionBusy
        case SQLITE_CORRUPT, SQLITE_NOTADB:
            code = .dataCorruption
        case SQLITE_FULL, SQLITE_NOMEM, SQLITE_IOERR:
            code = .resourceUnavailable
        default:
            code = .backendFailure
        }

        return StorageError(
            code: code,
            operation: operation,
            backend: .sqlite,
            message: "SQLite \(operation.rawValue) failed",
            underlyingDescription: "rc=\(rc): \(message)"
        )
    }
}
