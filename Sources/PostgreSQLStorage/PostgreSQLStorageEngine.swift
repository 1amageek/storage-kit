import StorageKit
import PostgresNIO
import NIOCore
import Logging

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
/// `ActiveTransactionContext` (TaskLocal) detects nested calls and reuses the
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
/// await engine.shutdown()
/// ```
public final class PostgreSQLStorageEngine: StorageEngine, Sendable {

    public typealias Configuration = PostgreSQLConfiguration
    public typealias TransactionType = PostgreSQLStorageTransaction

    private struct TransactionBodyFailure: Error {
        let underlying: any Error
    }

    let client: PostgresClient
    private let configuration: PostgreSQLConfiguration
    private let logger: Logger
    private let runTask: Task<Void, Never>
    public let transactionDomain: StorageTransactionDomain
    public let directoryAccess: any DirectoryAccess
    private let storageLifecycle = StorageEngineLifecycle()
    private let resultBytesFactory: PostgreSQLResultBytesFactory

    /// The Directory catalog decides existence by reading a node and then
    /// writing elsewhere in the same transaction, and a Partition write binding
    /// reads the Partition's node and then writes inside its prefix. Both
    /// depend on that read conflicting with a concurrent transaction that
    /// removes the node.
    ///
    /// REPEATABLE READ does not give that conflict. The reader's row and the
    /// remover's rows are disjoint, so PostgreSQL's snapshot isolation lets
    /// both commit and leaves a child under a removed parent or data inside a
    /// removed Partition. Only SERIALIZABLE turns the read-write dependency
    /// into a serialization failure the caller's runner retries, so every
    /// other level refuses a catalog write and a write binding.
    ///
    /// A Partition read binding asks for less. It promises the leased
    /// generation for the span of its closure, which holds as long as the
    /// closure's reads see what the generation walk saw. REPEATABLE READ gives
    /// exactly that with its transaction-level snapshot. READ COMMITTED takes
    /// a fresh snapshot per statement, so a Partition removed after the walk
    /// reads back as an empty one — a removed Partition reported as success —
    /// and the read binding is refused there too.
    ///
    /// Catalog reads, lease issuance, and data-row operations outside a
    /// binding never reach this gate and stay available at every level.
    private static func directoryOperationAdmission(
        isolationLevel: PostgreSQLIsolationLevel
    ) -> KeyValueDirectoryCatalog.OperationAdmission? {
        guard isolationLevel != .serializable else { return nil }
        let configuredLevel = isolationLevel.sqlName
        let holdsSnapshotAcrossStatements = isolationLevel == .repeatableRead
        return { operation in
            switch operation {
            case .read, .rangeRead:
                guard !holdsSnapshotAcrossStatements else { return }
                throw StorageError.unsupportedOperation(
                    "A Partition read binding requires REPEATABLE READ or SERIALIZABLE isolation; this engine is configured with \(configuredLevel)",
                    operation: operation,
                    backend: .postgreSQL
                )
            default:
                throw StorageError.unsupportedOperation(
                    "Directory catalog mutation and Partition write binding require SERIALIZABLE isolation; this engine is configured with \(configuredLevel)",
                    operation: operation,
                    backend: .postgreSQL
                )
            }
        }
    }

    public convenience init(
        configuration: PostgreSQLConfiguration
    ) async throws {
        try await self.init(
            configuration: configuration,
            resultBytesFactory: .production
        )
    }

    /// Internal composition point for integration evidence from real queries.
    convenience init(
        configuration: PostgreSQLConfiguration,
        resultBytesLifecycleObserver:
            any PostgreSQLResultBytesLifecycleObserver
    ) async throws {
        try await self.init(
            configuration: configuration,
            resultBytesFactory: PostgreSQLResultBytesFactory(
                lifecycleObserver: resultBytesLifecycleObserver
            )
        )
    }

    private init(
        configuration: PostgreSQLConfiguration,
        resultBytesFactory: PostgreSQLResultBytesFactory
    ) async throws {
        // Validate the table name before constructing any SQL. The name is
        // interpolated into DDL/DML text, so an invalid identifier must fail
        // loudly here rather than corrupt a query downstream.
        try Self.validateTableName(configuration.tableName)

        let domain = StorageTransactionDomain()
        self.transactionDomain = domain
        self.directoryAccess = KeyValueDirectoryCatalog(
            transactionDomain: domain,
            backend: .postgreSQL,
            operationAdmission: Self.directoryOperationAdmission(
                isolationLevel: configuration.isolationLevel
            )
        )
        self.configuration = configuration
        self.logger = configuration.backgroundLogger
        self.resultBytesFactory = resultBytesFactory
        self.client = PostgresClient(
            configuration: configuration.clientConfiguration,
            backgroundLogger: configuration.backgroundLogger
        )

        // Start the connection pool's run loop (required by PostgresNIO). On
        // runtimes that provide synchronous task start, run() reaches its first
        // suspension before this initializer proceeds. That establishes the
        // client's running state before initializeSchema() leases a connection.
        // Older Apple runtimes use PostgresNIO's documented queued-lease
        // behavior because Task.immediate is not back-deployable there.
        let client = self.client
        self.runTask = Self.startClientRunLoop(client)

        do {
            try await initializeSchema()
        } catch {
            requestShutdown()
            await waitUntilShutdown()
            throw error
        }
    }

    private static func startClientRunLoop(
        _ client: PostgresClient
    ) -> Task<Void, Never> {
        if #available(
            macOS 26.0,
            iOS 26.0,
            tvOS 26.0,
            watchOS 26.0,
            visionOS 26.0,
            *
        ) {
            return Task.immediate { await client.run() }
        }
        return Task { await client.run() }
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
    /// If called within an existing `ActiveTransactionContext`, returns a nested
    /// transaction that reuses the parent's connection.
    public func createTransaction() throws -> PostgreSQLStorageTransaction {
        try storageLifecycle.withActiveAdmission(
            backend: .postgreSQL,
            operation: .beginTransaction
        ) {
            let contextLease = ActiveTransactionContext
                .acquireCurrentTransaction()
            if let contextLease,
               let existing = contextLease.transaction
                as? PostgreSQLStorageTransaction,
               existing.transactionDomain === transactionDomain {
                return PostgreSQLStorageTransaction(
                    parent: existing,
                    logger: logger,
                    contextLease: contextLease
                )
            }
            contextLease?.release()

            return PostgreSQLStorageTransaction(
                client: client,
                beginStatement: configuration.beginStatement,
                tableName: configuration.tableName,
                logger: logger,
                transactionDomain: transactionDomain,
                resultBytesFactory: resultBytesFactory
            )
        }
    }

    /// Overridden only to reuse an already-active transaction, to hold the
    /// pooled connection for the whole block, and to keep a non-`StorageError`
    /// operation failure from being remapped by the connection scope. Commit
    /// and cancellation stay with `TransactionLifecycleOwner`, which owns the
    /// unknown-commit rule.
    public func executeTransaction(
        _ operation: @escaping @Sendable (any TransactionAccess) async throws -> Void
    ) async throws {
        try storageLifecycle.requireActive(
            backend: .postgreSQL,
            operation: .execute
        )
        // Nested call — reuse the existing transaction.
        let contextLease = ActiveTransactionContext
            .acquireCurrentTransaction()
        if let contextLease,
           let existing = contextLease.transaction
            as? PostgreSQLStorageTransaction,
           existing.transactionDomain === transactionDomain {
            defer { contextLease.release() }
            try await operation(existing)
            return
        }
        contextLease?.release()

        do {
            try await client.withConnection { [configuration, logger] conn in
                try await conn.query(
                    PostgresQuery(unsafeSQL: configuration.beginStatement),
                    logger: logger
                )

                let tx = PostgreSQLStorageTransaction(
                    connection: conn,
                    tableName: configuration.tableName,
                    logger: logger,
                    transactionDomain: transactionDomain,
                    resultBytesFactory: resultBytesFactory
                )

                let owner = TransactionLifecycleOwner(transaction: tx)
                try await owner.execute { access in
                    do {
                        try await operation(access)
                    } catch {
                        throw Self.preserveTransactionBodyFailure(from: error)
                    }
                }
            }
        } catch {
            throw Self.recoverTransactionBodyFailure(from: error)
        }
    }

    public func requestShutdown() {
        transactionDomain.requestShutdown()
        let runTask = runTask
        storageLifecycle.requestShutdown(
            prepare: {
                runTask.cancel()
            },
            cleanup: {
                await runTask.value
            }
        )
    }

    public func waitUntilShutdown() async {
        requestShutdown()
        await storageLifecycle.waitUntilShutdown()
    }

    deinit {
        requestShutdown()
    }

    /// Verify that PostgreSQL is reachable and that the configured KV table exists.
    ///
    /// Intended for startup/readiness probes. This method does not mutate data and
    /// maps backend failures into `StorageError` so callers get the same retryable
    /// classification as normal storage operations.
    public func checkReadiness() async throws -> PostgreSQLReadinessReport {
        try storageLifecycle.requireActive(
            backend: .postgreSQL,
            operation: .read
        )
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

    private static func preserveTransactionBodyFailure(
        from error: any Error
    ) -> any Error {
        if error is CancellationError {
            return error
        }
        if let storageError = error as? StorageError {
            return storageError
        }
        return TransactionBodyFailure(underlying: error)
    }

    private static func recoverTransactionBodyFailure(
        from error: any Error
    ) -> any Error {
        if let cleanupError = error as? StorageTransactionCleanupError {
            guard
                let bodyFailure = cleanupError.operationError
                    as? TransactionBodyFailure
            else {
                return cleanupError
            }
            return cleanupError.replacingOperationError(bodyFailure.underlying)
        }
        if let operationError = error as? TransactionBodyFailure {
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
