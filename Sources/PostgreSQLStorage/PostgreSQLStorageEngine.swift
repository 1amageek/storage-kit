import StorageKit
import PostgresNIO
import NIOCore
import Logging
import Synchronization

/// PostgreSQL backend StorageEngine implementation.
///
/// Uses PostgresNIO's `PostgresClient` for connection pooling and async query
/// execution. Stores all data in a single key/value table (BYTEA key/value
/// columns); the table name is configurable and validated at initialization.
///
/// ## Transaction Creation
///
/// Both `createTransaction()` and `withTransaction()` are supported.
///
/// `createTransaction()` returns a transaction that lazily acquires a connection
/// on first use and issues BEGIN. The caller MUST call `commit()` or `cancel()`
/// to release the connection back to the pool. This is the path used by
/// database-framework's `TransactionRunner`.
///
/// `withTransaction()` is a convenience that manages the connection lifecycle
/// automatically (BEGIN → operation → COMMIT/ROLLBACK). Retry is owned by
/// higher-level transaction runners, not by this engine.
///
/// ## Nested Transaction Safety
///
/// `ActiveTransactionScope` (TaskLocal) detects nested calls and reuses the
/// parent's connection for efficiency.
///
/// ## Concurrency
///
/// PostgreSQL supports concurrent transactions via MVCC. Each transaction
/// acquires an independent connection from the pool. The default isolation level
/// is SERIALIZABLE to match FoundationDB semantics.
///
/// ## Usage
/// ```swift
/// let config = PostgreSQLConfiguration(
///     host: "localhost",
///     username: "postgres",
///     password: "secret",
///     database: "mydb"
/// )
/// let engine = try await PostgreSQLStorageEngine(configuration: config)
/// try await engine.withTransaction { tx in
///     tx.setValue([1, 2, 3], for: [0, 1])
/// }
/// engine.shutdown()
/// ```
public final class PostgreSQLStorageEngine: StorageEngine, Sendable {

    public typealias Configuration = PostgreSQLConfiguration
    public typealias TransactionType = PostgreSQLStorageTransaction

    private struct OperationClosureError: Error {
        let underlying: any Error
    }

    let client: PostgresClient
    private let configuration: PostgreSQLConfiguration
    private let logger: Logger
    private let runTask: Mutex<Task<Void, Never>?>

    public init(configuration: PostgreSQLConfiguration) async throws {
        // Validate the table name before constructing any SQL. The name is
        // interpolated into DDL/DML text, so an invalid identifier must fail
        // loudly here rather than corrupt a query downstream.
        try Self.validateTableName(configuration.tableName)

        self.configuration = configuration
        self.logger = configuration.backgroundLogger
        self.client = PostgresClient(
            configuration: configuration.clientConfiguration,
            backgroundLogger: configuration.backgroundLogger
        )

        // Start the connection pool's run loop (required by PostgresNIO). The
        // pool queues connection requests issued before run() is scheduled, so
        // the first query below blocks until the pool is ready rather than
        // racing it — no warm-up yield is needed.
        let client = self.client
        self.runTask = Mutex(Task { await client.run() })

        try await initializeSchema()
    }

    // MARK: - Table Name Validation

    /// Validate that `tableName` is a bare SQL identifier safe to interpolate.
    ///
    /// Accepts ASCII letters, digits, and underscore; the first character must be
    /// a letter or underscore; length at most 63 bytes (PostgreSQL's identifier
    /// limit). This is intentionally stricter than PostgreSQL's quoted-identifier
    /// rules because the name is interpolated unquoted into SQL text.
    static func validateTableName(_ tableName: String) throws {
        func invalid(_ reason: String) -> StorageError {
            StorageError(
                code: .invalidOperation,
                operation: .initialize,
                backend: .postgreSQL,
                message: "Invalid table name '\(tableName)': \(reason)"
            )
        }

        guard !tableName.isEmpty else {
            throw invalid("must not be empty")
        }
        guard tableName.utf8.count <= 63 else {
            throw invalid("must be at most 63 bytes")
        }

        for (offset, scalar) in tableName.unicodeScalars.enumerated() {
            let isLetter = (scalar >= "a" && scalar <= "z") || (scalar >= "A" && scalar <= "Z")
            let isUnderscore = scalar == "_"
            let isDigit = scalar >= "0" && scalar <= "9"
            if offset == 0 {
                guard isLetter || isUnderscore else {
                    throw invalid("must start with a letter or underscore")
                }
            } else {
                guard isLetter || isUnderscore || isDigit else {
                    throw invalid("may only contain letters, digits, and underscores")
                }
            }
        }
    }

    // MARK: - Schema

    private func initializeSchema() async throws {
        // Skip DDL when the caller guarantees the table exists. This supports
        // pre-provisioned databases whose connecting role lacks DDL privileges
        // (e.g. IAM-managed Cloud SQL users restricted to DML).
        guard configuration.schemaManagement == .createIfNeeded else {
            return
        }

        let tableName = configuration.tableName
        do {
            _ = try await client.withConnection { [logger] conn in
                try await conn.query(
                    PostgresQuery(unsafeSQL: """
                        CREATE TABLE IF NOT EXISTS \(tableName) (
                            key BYTEA NOT NULL PRIMARY KEY,
                            value BYTEA NOT NULL
                        )
                        """),
                    logger: logger
                )
            }
        } catch {
            throw Self.mapError(error, operation: .initialize)
        }
    }

    // MARK: - StorageEngine

    /// Create a new transaction that lazily acquires a dedicated connection.
    ///
    /// The caller MUST call `commit()` or `cancel()` to release the connection
    /// back to the pool. Failing to do so will leak the connection.
    ///
    /// If called within an existing `ActiveTransactionScope`, returns a nested
    /// transaction that reuses the parent's connection.
    public func createTransaction() throws -> PostgreSQLStorageTransaction {
        if let existing = ActiveTransactionScope.current as? PostgreSQLStorageTransaction {
            return PostgreSQLStorageTransaction(parent: existing, logger: logger)
        }

        return PostgreSQLStorageTransaction(
            client: client,
            beginStatement: configuration.beginStatement,
            isInTransactionBlock: true,
            tableName: configuration.tableName,
            logger: logger
        )
    }

    public func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        // Nested call — reuse the existing transaction.
        if let existing = ActiveTransactionScope.current {
            return try await operation(existing)
        }

        do {
            return try await client.withConnection { [configuration, logger] conn in
                try await conn.query(
                    PostgresQuery(unsafeSQL: configuration.beginStatement),
                    logger: logger
                )

                let tx = PostgreSQLStorageTransaction(
                    connection: conn,
                    isInTransactionBlock: true,
                    tableName: configuration.tableName,
                    logger: logger
                )

                return try await ActiveTransactionScope.$current.withValue(tx) {
                    do {
                        let result: T
                        do {
                            result = try await operation(tx)
                        } catch {
                            await tx.rollbackInternal(connection: conn)
                            throw Self.operationClosureError(from: error)
                        }
                        try await tx.commitInternal(connection: conn)
                        return result
                    } catch {
                        throw error
                    }
                }
            }
        } catch {
            throw Self.storageBoundaryError(from: error)
        }
    }

    /// Execute an operation in auto-commit mode (no BEGIN/COMMIT).
    ///
    /// Each SQL statement issued by the transaction commits individually, saving
    /// two round-trips compared to `withTransaction()`. Because there is no
    /// surrounding transaction block, multi-statement flushes are NOT atomic and
    /// `atomicOp` is rejected (see `PostgreSQLStorageTransaction`).
    public func withAutoCommit<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        // Nested: reuse the existing transaction (already inside BEGIN/COMMIT).
        if let existing = ActiveTransactionScope.current {
            return try await operation(existing)
        }

        do {
            return try await client.withConnection { [configuration, logger] conn in
                let tx = PostgreSQLStorageTransaction(
                    connection: conn,
                    isInTransactionBlock: false,
                    tableName: configuration.tableName,
                    logger: logger
                )

                return try await ActiveTransactionScope.$current.withValue(tx) {
                    let result: T
                    do {
                        result = try await operation(tx)
                    } catch {
                        throw Self.operationClosureError(from: error)
                    }
                    // Flush buffered writes; each executes as its own auto-commit.
                    try await tx.commitInternal(connection: conn, skipCommitStatement: true)
                    return result
                }
            }
        } catch {
            throw Self.storageBoundaryError(from: error)
        }
    }

    // DirectoryService: uses StaticDirectoryService (default from protocol extension).

    public func shutdown() {
        runTask.withLock { task in
            task?.cancel()
            task = nil
        }
    }

    /// Verify that PostgreSQL is reachable and that the configured KV table exists.
    ///
    /// Intended for startup/readiness probes. This method does not mutate data and
    /// maps backend failures into `StorageError` so callers get the same retryable
    /// classification as normal storage operations.
    public func checkReadiness() async throws -> PostgreSQLReadinessReport {
        do {
            try await client.withConnection { [configuration, logger] conn in
                _ = try await conn.query(PostgresQuery(unsafeSQL: "SELECT 1"), logger: logger)
                _ = try await conn.query(
                    PostgresQuery(unsafeSQL: "SELECT 1 FROM \(configuration.tableName) LIMIT 0"),
                    logger: logger
                )
            }
            return PostgreSQLReadinessReport(
                tableName: configuration.tableName,
                schemaManagement: configuration.schemaManagement
            )
        } catch let error as StorageError {
            throw error
        } catch {
            throw Self.mapError(error, operation: .read)
        }
    }

    // MARK: - Error Mapping

    private static func operationClosureError(from error: any Error) -> any Error {
        if error is CancellationError {
            return error
        }
        if let storageError = error as? StorageError {
            return storageError
        }
        return OperationClosureError(underlying: error)
    }

    private static func storageBoundaryError(from error: any Error) -> any Error {
        if let operationError = error as? OperationClosureError {
            return operationError.underlying
        }
        if error is CancellationError {
            return error
        }
        if let storageError = error as? StorageError {
            return storageError
        }
        return mapError(error)
    }

    /// Map an arbitrary error into a `StorageError`.
    ///
    /// Existing `StorageError`s pass through unchanged. `PSQLError`s are
    /// classified by SQLSTATE (server-reported) or client-side error code.
    /// Everything else becomes a non-retryable `.backendFailure`.
    static func mapError(_ error: any Error, operation: StorageOperation = .unknown) -> StorageError {
        if let storageError = error as? StorageError {
            return storageError
        }
        if let psqlError = error as? PSQLError {
            return mapPSQLError(psqlError, operation: operation)
        }
        return StorageError(
            code: .backendFailure,
            operation: operation,
            backend: .postgreSQL,
            message: "PostgreSQL error",
            underlyingDescription: error.localizedDescription
        )
    }

    /// Classify a `PSQLError`.
    ///
    /// Server SQLSTATE is the most reliable signal when present:
    /// - `08xxx` connection_exception class → retryable connection failure
    /// - `40001` serialization_failure, `40P01` deadlock_detected → retryable conflict
    /// - `23505` unique_violation → retryable conflict (the KV upsert uses
    ///   `ON CONFLICT`, so this only surfaces from a concurrent INSERT race)
    /// - `57P01`/`57P02`/`57P03` server shutdown/startup states → retryable
    ///   connection failure
    ///
    /// With no server info, a connection-class client code maps to
    /// `.connectionFailure` — or `.commitUnknownResult` if it happened during a
    /// commit, where the transaction's fate is genuinely undetermined.
    private static func mapPSQLError(
        _ error: PSQLError,
        operation: StorageOperation
    ) -> StorageError {
        if let serverInfo = error.serverInfo {
            return mapSQLState(
                serverInfo[.sqlState],
                serverMessage: serverInfo[.message],
                fallbackDescription: error.localizedDescription,
                operation: operation
            )
        }

        switch error.code {
        case .clientClosedConnection, .serverClosedConnection, .connectionError,
             .uncleanShutdown, .poolClosed:
            return connectionFailureError(
                operation: operation,
                underlyingDescription: error.localizedDescription
            )
        default:
            return StorageError(
                code: .backendFailure,
                operation: operation,
                backend: .postgreSQL,
                message: "PostgreSQL error",
                underlyingDescription: error.localizedDescription
            )
        }
    }

    static func mapSQLState(
        _ sqlState: String?,
        serverMessage: String?,
        fallbackDescription: String,
        operation: StorageOperation
    ) -> StorageError {
        let description = serverMessage ?? fallbackDescription

        if let sqlState, sqlState.hasPrefix("08") {
            return connectionFailureError(
                operation: operation,
                underlyingDescription: description
            )
        }

        switch sqlState {
        case "40001":
            return StorageError(
                code: .transactionConflict,
                operation: operation,
                backend: .postgreSQL,
                message: "PostgreSQL serialization failure",
                underlyingDescription: serverMessage
            )
        case "40P01":
            return StorageError(
                code: .transactionConflict,
                operation: operation,
                backend: .postgreSQL,
                message: "PostgreSQL deadlock detected",
                underlyingDescription: serverMessage
            )
        case "23505":
            return StorageError(
                code: .transactionConflict,
                operation: operation,
                backend: .postgreSQL,
                message: "PostgreSQL unique constraint violation",
                underlyingDescription: serverMessage
            )
        case "54000":
            return StorageError(
                code: .backendFailure,
                operation: operation,
                backend: .postgreSQL,
                message: "PostgreSQL program limit exceeded",
                underlyingDescription: "sqlState=54000: \(description)"
            )
        case "57P01", "57P02", "57P03":
            return connectionFailureError(
                operation: operation,
                underlyingDescription: description
            )
        default:
            return StorageError(
                code: .backendFailure,
                operation: operation,
                backend: .postgreSQL,
                message: "PostgreSQL error",
                underlyingDescription: "sqlState=\(sqlState ?? "unknown"): \(description)"
            )
        }
    }

    private static func connectionFailureError(
        operation: StorageOperation,
        underlyingDescription: String?
    ) -> StorageError {
        StorageError(
            code: operation == .commit ? .commitUnknownResult : .connectionFailure,
            operation: operation,
            backend: .postgreSQL,
            message: "PostgreSQL connection failure",
            underlyingDescription: underlyingDescription
        )
    }
}
