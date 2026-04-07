import StorageKit
import PostgresNIO
import NIOCore
import Logging
import Synchronization

/// PostgreSQL backend StorageEngine implementation.
///
/// Uses PostgresNIO's `PostgresClient` for connection pooling and async query execution.
/// Stores all data in a single `kv_store` table with BYTEA key/value columns.
///
/// ## Transaction Creation
///
/// Both `createTransaction()` and `withTransaction()` are supported.
///
/// `createTransaction()` acquires a connection from the pool and issues BEGIN.
/// The caller MUST call `commit()` or `cancel()` to release the connection.
/// This is the path used by database-framework's `TransactionRunner`.
///
/// `withTransaction()` is a convenience that manages the connection lifecycle
/// automatically (BEGIN → operation → COMMIT/ROLLBACK).
///
/// ## Nested Transaction Safety
///
/// `ActiveTransactionScope` (TaskLocal) detects nested calls and reuses the
/// parent's connection for efficiency.
///
/// ## Concurrency
///
/// PostgreSQL supports concurrent transactions via MVCC. Each transaction
/// acquires an independent connection from the pool. Default isolation level
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

    let client: PostgresClient
    private let configuration: PostgreSQLConfiguration
    private let logger: Logger
    private let runTask: Mutex<Task<Void, Never>?>

    public init(configuration: PostgreSQLConfiguration) async throws {
        self.configuration = configuration
        self.logger = configuration.backgroundLogger
        self.client = PostgresClient(
            configuration: configuration.clientConfiguration,
            backgroundLogger: configuration.backgroundLogger
        )

        // Start the connection pool event loop (required by PostgresNIO).
        // Yield once to let the run() task begin accepting connections
        // before we attempt to use the client.
        let client = self.client
        let task = Task { await client.run() }
        self.runTask = Mutex(task)
        await Task.yield()

        // Initialize schema
        try await initializeSchema()
    }

    private func initializeSchema() async throws {
        _ = try await client.withConnection { [logger] conn in
            try await conn.query(
                """
                CREATE TABLE IF NOT EXISTS kv_store (
                    key BYTEA NOT NULL PRIMARY KEY,
                    value BYTEA NOT NULL
                )
                """,
                logger: logger
            )
        }
    }

    // MARK: - StorageEngine

    /// Create a new transaction with a dedicated PostgreSQL connection.
    ///
    /// The caller MUST call `commit()` or `cancel()` to release the connection
    /// back to the pool. Failing to do so will leak the connection.
    ///
    /// If called within an existing `ActiveTransactionScope`, returns a nested
    /// transaction that reuses the parent's connection.
    public func createTransaction() throws -> PostgreSQLStorageTransaction {
        // Nested transaction detection via ActiveTransactionScope
        if let existing = ActiveTransactionScope.current as? PostgreSQLStorageTransaction {
            return PostgreSQLStorageTransaction(
                parent: existing,
                logger: logger
            )
        }

        // Top-level: return a transaction that will lazily acquire a connection.
        // The connection is obtained on the first async operation (getValue,
        // getRange iteration, or commit with buffered writes).
        return PostgreSQLStorageTransaction(
            client: client,
            beginStatement: configuration.beginStatement,
            isNested: false,
            logger: logger
        )
    }

    public func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        // Nested transaction detection — reuse existing transaction
        if let existing = ActiveTransactionScope.current {
            return try await operation(existing)
        }

        let maxRetries = configuration.maxRetries
        guard maxRetries > 0 else {
            throw StorageError.invalidOperation("maxRetries must be greater than 0")
        }
        for attempt in 0..<maxRetries {
            do {
                let result: T = try await client.withConnection { [configuration, logger] conn in
                    // BEGIN transaction
                    try await conn.query(
                        PostgresQuery(unsafeSQL: configuration.beginStatement),
                        logger: logger
                    )

                    let tx = PostgreSQLStorageTransaction(
                        connection: conn,
                        isNested: false,
                        logger: logger
                    )

                    return try await ActiveTransactionScope.$current.withValue(tx) {
                        do {
                            let result = try await operation(tx)
                            try await tx.commitInternal(connection: conn)
                            return result
                        } catch {
                            try await tx.rollbackInternal(connection: conn)
                            throw error
                        }
                    }
                }
                return result
            } catch let error as StorageError where error.isRetryable {
                if attempt < maxRetries - 1 { continue }
                throw StorageError.backendError(
                    "Max retries (\(maxRetries)) exceeded: \(error)"
                )
            } catch {
                // Only map PostgreSQL-specific errors for retry logic.
                // User-thrown errors pass through unchanged.
                guard error is PSQLError else {
                    throw error
                }
                let mapped = Self.mapError(error)
                if mapped.isRetryable, attempt < maxRetries - 1 { continue }
                if mapped.isRetryable {
                    throw StorageError.backendError(
                        "Max retries (\(maxRetries)) exceeded: \(error)"
                    )
                }
                throw mapped
            }
        }
        // Unreachable when maxRetries > 0 — all loop iterations either return or throw.
        throw StorageError.backendError("Transaction failed after \(maxRetries) attempts")
    }

    /// Execute an operation in auto-commit mode (no BEGIN/COMMIT).
    ///
    /// Each SQL statement issued by the transaction commits individually.
    /// Write buffer is flushed directly to the connection without transaction wrapping.
    /// This saves 2 SQL round-trips compared to `withTransaction()`.
    public func withAutoCommit<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        // Nested: reuse existing transaction (already inside BEGIN/COMMIT)
        if let existing = ActiveTransactionScope.current {
            return try await operation(existing)
        }

        return try await client.withConnection { [logger] conn in
            // No BEGIN — PostgreSQL auto-commits each statement
            let tx = PostgreSQLStorageTransaction(
                connection: conn,
                isNested: false,
                logger: logger
            )

            return try await ActiveTransactionScope.$current.withValue(tx) {
                let result = try await operation(tx)

                // Flush any buffered writes (each executes as auto-commit)
                try await tx.commitInternal(connection: conn, skipCommitStatement: true)

                return result
            }
        }
    }

    // DirectoryService: uses StaticDirectoryService (default from protocol extension)

    public func shutdown() {
        runTask.withLock { task in
            task?.cancel()
            task = nil
        }
    }

    // MARK: - Error Mapping

    /// Map PostgreSQL errors to StorageError.
    ///
    /// Retryable SQL states:
    /// - 40001: serialization_failure (SERIALIZABLE conflict)
    /// - 40P01: deadlock_detected
    /// - 23505: unique_violation (mapped as conflict for retry)
    static func mapError(_ error: any Error) -> StorageError {
        // Already a StorageError — pass through
        if let storageError = error as? StorageError {
            return storageError
        }

        // PSQLError with server info
        if let psqlError = error as? PSQLError {
            if let serverInfo = psqlError.serverInfo {
                let sqlState = serverInfo[.sqlState]
                switch sqlState {
                case "40001":
                    return .transactionConflict
                case "40P01":
                    return .transactionConflict
                case "23505":
                    return .transactionConflict
                default:
                    let message = serverInfo[.message] ?? psqlError.localizedDescription
                    return .backendError("PostgreSQL error (\(sqlState ?? "unknown")): \(message)")
                }
            }
            return .backendError("PostgreSQL error: \(psqlError.localizedDescription)")
        }

        return .backendError("PostgreSQL error: \(error.localizedDescription)")
    }
}
