import DatabaseTypes
import StorageKit
import PostgresNIO
import NIOCore
import Logging
import Synchronization

/// StorageKit.Transaction implementation for PostgreSQL.
///
/// Writes are buffered and synchronously reject every non-open lifecycle state.
/// `getValue` replays the buffer (read-your-writes); `getRange` flushes it.
///
/// ## Connection Lifecycle
///
/// A transaction is created in one of three modes:
///
/// 1. **Eager** (`init(connection:...)`, used by the engine's
///    `withTransaction`): the connection is supplied up front and the engine
///    owns BEGIN/COMMIT/ROLLBACK and connection release.
/// 2. **Lazy** (`init(client:...)`, used by `createTransaction` at top level):
///    a connection is acquired from the pool on the first async operation, and
///    this transaction owns BEGIN/COMMIT/ROLLBACK. The caller MUST call `commit()`
///    or `cancel()` to release the connection back to the pool.
/// 3. **Nested** (`init(parent:...)`, used by `createTransaction` under an active
///    transaction context): the parent's connection is reused. `commit()` merges
///    the child buffer into the parent; `cancel()` only discards. The parent
///    controls the real transaction lifecycle.
///
/// ## Lazy Acquisition and Parking
///
/// `PostgresClient` exposes only the scoped `withConnection`, so a lazily-acquired
/// connection is "parked": a background task enters `withConnection` and suspends
/// on a continuation until `releaseConnection()` resumes it. Concurrent first-touch
/// callers share a single cached acquisition `Task`, guaranteeing exactly one
/// connection is leased.
///
/// Commit and cancellation are single-assignment operations. Concurrent callers
/// wait for the same completion and connection release remains exactly-once.
public final class PostgreSQLStorageTransaction: Transaction, Sendable {

    public typealias RangeResult = PostgreSQLRangeResult

    public static let declaredCapabilities = TransactionCapabilities(
            transactionCheckpoints: true,
            readVersion: true,
            committedVersion: true,
            committedVersionstamp: true
    )

    public var capabilities: TransactionCapabilities { Self.declaredCapabilities }
    public var compaction: StorageCompactionAccess? { nil }
    public var mutationByteLimit: Int? { mutationByteMeter.maximumBytes }
    public let transactionDomain: StorageTransactionDomain

    public var storageFailure: StorageError? {
        state.withLock { state in
            switch state.lifecycle {
            case .failed(let error, _):
                return error as? StorageError
            case .commitUnknown(let error):
                return error
            case .open, .committing, .committed, .cancelling, .cancelled:
                return nil
            }
        }
    }

    private let isNested: Bool
    private let tableName: String
    private let logger: Logger

    /// Parent transaction for nested transactions; nil otherwise.
    private let parent: PostgreSQLStorageTransaction?
    private let contextLease: ActiveTransactionContext.Lease?

    /// Client for lazy acquisition (set only for the top-level lazy path).
    private let client: PostgresClient?

    /// BEGIN statement issued after lazy acquisition (set only for the lazy path).
    private let beginStatement: String?

    private let state: NativeState
    private let connectionOwner: ConnectionOwner
    private let checkpointState = Mutex(CheckpointState())
    private let checkpointIsolationAllowed: Bool
    private let mutationByteMeter: TransactionMutationByteMeter
    private let versionstampCompletion: TransactionVersionstampCompletion
    private let resultBytesFactory: PostgreSQLResultBytesFactory

    /// Maximum rows bound per chunked statement. Each upsert row uses two
    /// parameters, so 1000 rows stays well under PostgreSQL's 65535 limit.
    static let maxBindRows = 1000

    fileprivate enum Lifecycle: Sendable {
        case open
        case committing(TransactionOperationCompletion)
        case committed
        case cancelling(TransactionOperationCompletion)
        case cancelled
        case failed(any Error, cleanupRequired: Bool)
        case commitUnknown(StorageError)
    }

    fileprivate struct MutableState {
        var writeBuffer: [WriteOp] = []
        var lifecycle: Lifecycle = .open
        var connection: PostgresConnection? = nil
        var acquireTask: Task<PostgresConnection, any Error>? = nil
        var releaseContinuation: CheckedContinuation<Void, Never>? = nil
        var readVersion: Int64? = nil
        var committedVersion: Int64? = nil

        init(connection: PostgresConnection? = nil) {
            self.connection = connection
        }
    }

    fileprivate enum WriteOp: Sendable {
        case set(key: ByteString, value: ByteString)
        case clear(key: ByteString)
        case clearRange(begin: ByteString, end: ByteString)
        case atomic(key: ByteString, param: ByteString, mutationType: MutationType)
    }

    fileprivate enum ConnectionOutcome {
        case existing(PostgresConnection)
        case pending(Task<PostgresConnection, any Error>)
    }

    private enum CommitStart {
        case leader(TransactionOperationCompletion)
        case waitForCommit(TransactionOperationCompletion)
        case waitForCancellation(TransactionOperationCompletion)
        case committed
        case cancelled
        case failed(any Error)
    }

    // MARK: - Initializers

    /// Eager-connection init (engine-managed lifecycle).
    init(
        connection: PostgresConnection,
        tableName: String,
        logger: Logger,
        transactionDomain: StorageTransactionDomain,
        resultBytesFactory: PostgreSQLResultBytesFactory = .production,
        checkpointIsolationAllowed: Bool = true
    ) {
        self.isNested = false
        self.tableName = tableName
        self.logger = logger
        self.parent = nil
        self.contextLease = nil
        self.client = nil
        self.beginStatement = nil
        self.transactionDomain = transactionDomain
        let state = NativeState(connection: connection)
        let completion = TransactionVersionstampCompletion()
        self.state = state
        self.mutationByteMeter = TransactionMutationByteMeter()
        self.versionstampCompletion = completion
        self.connectionOwner = ConnectionOwner(
            state: state, client: nil, beginStatement: nil,
            logger: logger, versionstampCompletion: completion
        )
        self.checkpointIsolationAllowed = checkpointIsolationAllowed
        self.resultBytesFactory = resultBytesFactory
    }

    /// Nested-transaction init (parent owns the connection).
    init(
        parent: PostgreSQLStorageTransaction,
        logger: Logger,
        contextLease: ActiveTransactionContext.Lease
    ) {
        self.isNested = true
        self.tableName = parent.tableName
        self.logger = logger
        self.parent = parent
        self.contextLease = contextLease
        self.client = nil
        self.beginStatement = nil
        self.transactionDomain = parent.transactionDomain
        self.state = NativeState()
        self.mutationByteMeter = parent.mutationByteMeter
        self.versionstampCompletion = parent.versionstampCompletion
        self.connectionOwner = parent.connectionOwner
        self.checkpointIsolationAllowed = false
        self.resultBytesFactory = parent.resultBytesFactory
    }

    /// Lazy-connection init (this transaction owns the connection).
    init(
        client: PostgresClient,
        beginStatement: String,
        tableName: String,
        logger: Logger,
        transactionDomain: StorageTransactionDomain,
        resultBytesFactory: PostgreSQLResultBytesFactory = .production,
        checkpointIsolationAllowed: Bool = true
    ) {
        self.isNested = false
        self.tableName = tableName
        self.logger = logger
        self.parent = nil
        self.contextLease = nil
        self.client = client
        self.beginStatement = beginStatement
        self.transactionDomain = transactionDomain
        let state = NativeState()
        let completion = TransactionVersionstampCompletion()
        self.state = state
        self.mutationByteMeter = TransactionMutationByteMeter()
        self.versionstampCompletion = completion
        self.connectionOwner = ConnectionOwner(
            state: state, client: client, beginStatement: beginStatement,
            logger: logger, versionstampCompletion: completion
        )
        self.checkpointIsolationAllowed = checkpointIsolationAllowed
        self.resultBytesFactory = resultBytesFactory
    }

    private struct CheckpointState {
        var journal: TransactionJournal?
        var accessStarted = false
    }

    public var checkpoints: (any TransactionCheckpointAccess)? {
        checkpointState.withLock { $0.journal }
    }

    public func configureCheckpoints(limits: TransactionCheckpointLimits) throws {
        try checkpointState.withLock { checkpoint in
            try state.withLock { try Self.validateOpen($0.lifecycle, operation: .beginTransaction) }
            guard checkpointIsolationAllowed, !isNested else {
                throw StorageError.unsupportedOperation(
                    "PostgreSQL checkpoints require a root transaction configured with SERIALIZABLE isolation",
                    operation: .beginTransaction, backend: .postgreSQL
                )
            }
            guard !checkpoint.accessStarted, checkpoint.journal == nil else {
                throw TransactionCheckpointError.configurationAfterAdmission
            }
            checkpoint.journal = TransactionJournal(
                limits: limits, backend: .postgreSQL, mutationByteMeter: mutationByteMeter
            )
        }
    }

    func requireNativeChildAdmission() throws {
        try checkpointState.withLock { checkpoint in
            guard checkpoint.journal == nil else {
                throw StorageError.unsupportedOperation(
                    "PostgreSQL checkpoint transactions cannot create native children",
                    operation: .beginTransaction, backend: .postgreSQL
                )
            }
            checkpoint.accessStarted = true
        }
    }

    public func performOperation(
        _ operation: @escaping @Sendable () async throws -> Void
    ) async throws {
        if let journal = try admittedJournal() {
            try await journal.performOperation(operation)
        } else {
            try await operation()
        }
    }

    private func admittedJournal() throws -> TransactionJournal? {
        let journal = noteAdmission()
        if journal != nil {
            try state.withLock { state in
                switch state.lifecycle {
                case .failed(let error, _): throw error
                case .commitUnknown(let error): throw error
                default: break
                }
            }
        }
        return journal
    }

    private func noteAdmission() -> TransactionJournal? {
        checkpointState.withLock { checkpoint in
            checkpoint.accessStarted = true
            return checkpoint.journal
        }
    }

    private var journalBackend: JournalBackend {
        JournalBackend(
            connectionOwner: connectionOwner, tableName: tableName,
            logger: logger, resultBytesFactory: resultBytesFactory
        )
    }

    public func configureMutationByteLimit(maximumBytes: Int?) throws {
        try mutationByteMeter.configure(maximumBytes: maximumBytes)
    }

    fileprivate final class NativeState: Sendable {
        private let value: Mutex<MutableState>

        init(connection: PostgresConnection? = nil) {
            value = Mutex(MutableState(connection: connection))
        }

        func withLock<Result>(
            _ body: (inout sending MutableState) throws -> sending Result
        ) rethrows -> sending Result {
            try value.withLock(body)
        }
    }

    fileprivate final class ConnectionOwner: Sendable {
        let state: NativeState
        let client: PostgresClient?
        let beginStatement: String?
        let logger: Logger
        let versionstampCompletion: TransactionVersionstampCompletion

        init(
            state: NativeState, client: PostgresClient?, beginStatement: String?,
            logger: Logger, versionstampCompletion: TransactionVersionstampCompletion
        ) {
            self.state = state
            self.client = client
            self.beginStatement = beginStatement
            self.logger = logger
            self.versionstampCompletion = versionstampCompletion
        }

        func ensureConnection(allowCancelling: Bool = false) async throws -> PostgresConnection {
            try state.withLock { state in
                switch state.lifecycle {
                case .open, .committing:
                    break
                case .cancelling where allowCancelling:
                    break
                default:
                    throw PostgreSQLStorageTransaction.error(for: state.lifecycle, operation: .beginTransaction)
                }
            }

            let outcome: ConnectionOutcome = try state.withLock { state in
                switch state.lifecycle {
                case .open, .committing:
                    break
                case .cancelling where allowCancelling:
                    break
                default:
                    throw PostgreSQLStorageTransaction.error(for: state.lifecycle, operation: .beginTransaction)
                }
                if let connection = state.connection {
                    return .existing(connection)
                }
                if let task = state.acquireTask {
                    return .pending(task)
                }
                guard let client else {
                    throw StorageError.invalidOperation(
                        "No connection available and no client for lazy acquisition"
                    )
                }
                let beginStatement = self.beginStatement
                let task = Task {
                    try await self.acquireAndBegin(client: client, beginStatement: beginStatement, allowCancelling: allowCancelling)
                }
                state.acquireTask = task
                return .pending(task)
            }

            switch outcome {
            case .existing(let connection):
                return connection
            case .pending(let task):
                return try await task.value
            }
        }

        /// Acquire a connection from the pool, issue BEGIN, and publish it.
        private func acquireAndBegin(
            client: PostgresClient,
            beginStatement: String?,
            allowCancelling: Bool
        ) async throws -> PostgresConnection {
            // The acquisition Task captures self; clearing the stored reference on
            // every exit breaks the self -> state -> acquireTask -> self retain cycle.
            defer { state.withLock { $0.acquireTask = nil } }

            let connection: PostgresConnection
            do {
                connection = try await park(client: client)
            } catch {
                throw markFailed(error, operation: .beginTransaction)
            }

            // Cancelled while parking? No BEGIN ran yet — release and abort.
            let parkingError = state.withLock { state -> (any Error)? in
                switch state.lifecycle {
                case .open, .committing:
                    return nil
                case .cancelling where allowCancelling:
                    return nil
                default:
                    return PostgreSQLStorageTransaction.error(for: state.lifecycle, operation: .beginTransaction)
                }
            }
            if let parkingError {
                releaseConnection()
                throw parkingError
            }

            if let beginStatement {
                do {
                    try await connection.query(PostgresQuery(unsafeSQL: beginStatement), logger: logger)
                } catch {
                    releaseConnection()
                    throw markFailed(error, operation: .beginTransaction)
                }
            }

            // Publish the connection unless a concurrent cancel() beat us to it.
            let publishError = state.withLock { state -> (any Error)? in
                switch state.lifecycle {
                case .open, .committing:
                    break
                case .cancelling where allowCancelling:
                    break
                default:
                    return PostgreSQLStorageTransaction.error(for: state.lifecycle, operation: .beginTransaction)
                }
                state.connection = connection
                return nil
            }
            if let publishError {
                if beginStatement != nil {
                    let rollbackResult = await rollback(
                        connection: connection,
                        reason: "cancelled during acquire"
                    )
                    if case .failure(let rollbackError) = rollbackResult {
                        releaseConnection()
                        if let original = publishError as? StorageError {
                            throw combinedRollbackError(originalError: original, rollbackError: rollbackError)
                        }
                        throw StorageTransactionCleanupError(operationError: publishError, cancellationError: rollbackError)
                    }
                }
                releaseConnection()
                throw publishError
            }
            return connection
        }

        /// Lease a connection and keep it parked until `releaseConnection()` runs.
        ///
        /// The holder task retains this native owner while a connection is leased. If the caller never calls `commit()`/`cancel()`
        /// the connection leaks — that is a documented caller-contract violation.
        private func park(client: PostgresClient) async throws -> PostgresConnection {
            try await withCheckedThrowingContinuation { (handoff: CheckedContinuation<PostgresConnection, any Error>) in
                let didResume = Mutex(false)

                func shouldResumeHandoff() -> Bool {
                    didResume.withLock { didResume in
                        guard !didResume else { return false }
                        didResume = true
                        return true
                    }
                }

                func resumeHandoff(returning connection: PostgresConnection) {
                    guard shouldResumeHandoff() else { return }
                    handoff.resume(returning: connection)
                }

                func resumeHandoff(throwing error: any Error) {
                    guard shouldResumeHandoff() else { return }
                    handoff.resume(throwing: error)
                }

                Task { [self] in
                    do {
                        try await client.withConnection { connection in
                            await withCheckedContinuation { (release: CheckedContinuation<Void, Never>) in
                                // Assign the release continuation BEFORE resuming the
                                // handoff so releaseConnection() can never miss it.
                                state.withLock { $0.releaseContinuation = release }
                                resumeHandoff(returning: connection)
                            }
                        }
                    } catch {
                        resumeHandoff(throwing: error)
                    }
                }
            }
        }

        /// Resume the parked holder task, returning the connection to the pool.
        /// Idempotent: clears both the continuation and the published connection.
        func releaseConnection() {
            let continuation = state.withLock { state -> CheckedContinuation<Void, Never>? in
                let continuation = state.releaseContinuation
                state.releaseContinuation = nil
                state.connection = nil
                return continuation
            }
            continuation?.resume()
        }


        func markFailed(_ error: any Error, operation: StorageOperation) -> any Error {
            let mapped = error is CancellationError
                ? PostgreSQLStorageTransaction.invalidOperation(
                    "PostgreSQL transaction operation was cancelled", operation: operation
                )
                : PostgreSQLStorageEngine.mapError(error, operation: operation)
            state.withLock { state in
                if case .open = state.lifecycle {
                    state.lifecycle = .failed(
                        mapped, cleanupRequired: state.connection != nil || state.acquireTask != nil
                    )
                    state.writeBuffer.removeAll(keepingCapacity: false)
                }
            }
            versionstampCompletion.fail(mapped)
            return error is CancellationError ? error : mapped
        }

        private func rollback(connection: PostgresConnection, reason: String) async -> Result<Void, StorageError> {
            do {
                try await connection.query(PostgresQuery(unsafeSQL: "ROLLBACK"), logger: logger)
                return .success(())
            } catch {
                return .failure(PostgreSQLStorageEngine.mapError(error, operation: .rollback))
            }
        }

        private func combinedRollbackError(originalError: StorageError, rollbackError: StorageError) -> StorageError {
            StorageError(
                code: .backendFailure, operation: .rollback, backend: .postgreSQL,
                message: "PostgreSQL operation failed and rollback also failed",
                underlyingDescription: "operation=\(originalError); rollback=\(rollbackError)"
            )
        }
    }

    struct JournalBackend: TransactionJournalBackendAccess {
        fileprivate let connectionOwner: ConnectionOwner
        let tableName: String
        let logger: Logger
        let resultBytesFactory: PostgreSQLResultBytesFactory

        func getValue(
            for key: ByteString, snapshot: Bool, maximumByteCount: Int
        ) async throws -> ByteString? {
            do {
                let connection = try await connectionOwner.ensureConnection(allowCancelling: true)
                var bindings = PostgresBindings()
                bindings.append(PostgreSQLBindingBytes.copyToOwnedBuffer(key), context: .default)
                let rows = try await connection.query(
                    PostgresQuery(unsafeSQL: "SELECT value FROM \(tableName) WHERE key = $1", binds: bindings),
                    logger: logger
                )
                for try await value in rows.decode(ByteBuffer.self) {
                    guard value.readableBytes <= maximumByteCount else {
                        throw StorageError.pointReadValueTooLarge(
                            observedByteCount: value.readableBytes,
                            maximumByteCount: maximumByteCount, backend: .postgreSQL
                        )
                    }
                    return resultBytesFactory.makeByteString(retaining: value)
                }
                return nil
            } catch {
                if let storage = error as? StorageError, storage.isPointReadValueTooLarge { throw storage }
                throw connectionOwner.markFailed(error, operation: .read)
            }
        }

        func rangeCursor(
            in range: TransactionJournalRange, reverse: Bool, snapshot: Bool,
            pageSize: Int, maximumByteCount: Int
        ) -> KeyValueCursor {
            KeyValueCursor(consuming: PostgreSQLRangeResult(
                backend: self,
                plan: RangeScanPlan(
                    begin: Self.boundary(range.lower, lower: true),
                    end: Self.boundary(range.upper, lower: false),
                    limit: 0, reverse: reverse, batchSize: pageSize,
                    tableName: tableName, maximumByteCount: maximumByteCount
                )
            ))
        }

        private static func boundary(
            _ boundary: TransactionJournalBoundary, lower: Bool
        ) -> SQLRangeBoundary? {
            switch boundary {
            case .unbounded: nil
            case .inclusive(let key): .direct(op: lower ? ">=" : "<=", key: key)
            case .exclusive(let key): .direct(op: lower ? ">" : "<", key: key)
            }
        }

        func fetchRangeBatch(
            plan: RangeScanPlan, after lastKey: ByteString?, remaining: Int
        ) async throws -> [(ByteString, ByteString)] {
            do {
                let connection = try await connectionOwner.ensureConnection(allowCancelling: true)
                let batchLimit = plan.limit > 0 ? min(plan.batchSize, remaining) : plan.batchSize
                guard batchLimit > 0 else { return [] }

                var bindValues: [ByteString] = []
                func placeholder(for key: ByteString) -> String {
                    bindValues.append(key)
                    return "$\(bindValues.count)"
                }

                var clauses: [String] = []
                if let begin = plan.begin {
                    clauses.append(PostgreSQLStorageTransaction.boundaryClause(begin, tableName: plan.tableName, placeholder: placeholder))
                }
                if let end = plan.end {
                    clauses.append(PostgreSQLStorageTransaction.boundaryClause(end, tableName: plan.tableName, placeholder: placeholder))
                }
                if clauses.isEmpty { clauses.append("TRUE") }
                if let lastKey {
                    // Keyset pagination: advance strictly past the last emitted key.
                    let op = plan.reverse ? "<" : ">"
                    clauses.append("key \(op) \(placeholder(for: lastKey))")
                }
                let order = plan.reverse ? "DESC" : "ASC"
                let sql = "SELECT key, value FROM \(plan.tableName) WHERE "
                    + clauses.joined(separator: " AND ")
                    + " ORDER BY key \(order) LIMIT \(batchLimit)"

                var bindings = PostgresBindings()
                for value in bindValues {
                    bindings.append(
                        PostgreSQLBindingBytes.copyToOwnedBuffer(value),
                        context: .default
                    )
                }
                let rows = try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
                var results: [(ByteString, ByteString)] = []
                var transferredBytes = 0
                for try await (keyBuffer, valueBuffer) in rows.decode((ByteBuffer, ByteBuffer).self) {
                    if let maximum = plan.maximumByteCount {
                        let (rowBytes, rowOverflow) = keyBuffer.readableBytes.addingReportingOverflow(valueBuffer.readableBytes)
                        let (total, totalOverflow) = transferredBytes.addingReportingOverflow(rowBytes)
                        guard !rowOverflow, !totalOverflow, total <= maximum else {
                            throw StorageError(
                                code: .resourceUnavailable, operation: .rangeRead, backend: .postgreSQL,
                                message: "PostgreSQL journal page exceeds the transfer byte limit"
                            )
                        }
                        transferredBytes = total
                    }
                    results.append(
                        (
                            resultBytesFactory.makeByteString(retaining: keyBuffer),
                            resultBytesFactory.makeByteString(retaining: valueBuffer)
                        )
                    )
                }
                return results
            } catch {
                if let storage = error as? StorageError, storage.code == .resourceUnavailable { throw storage }
                throw connectionOwner.markFailed(error, operation: .rangeRead)
            }
        }
    }


    // MARK: - Connection Acquisition

    /// Return the active connection, acquiring one lazily on first use.
    ///
    /// Nested transactions delegate to the parent. Top-level lazy transactions
    /// cache a single acquisition `Task` so concurrent first-touch callers all
    /// await the same lease.
    private func ensureConnection() async throws -> PostgresConnection {
        try state.withLock { state in
            switch state.lifecycle {
            case .open, .committing: break
            default: throw Self.error(for: state.lifecycle, operation: .beginTransaction)
            }
        }
        if let parent { return try await parent.ensureConnection() }
        return try await connectionOwner.ensureConnection()
    }

    private func releaseConnection() {
        connectionOwner.releaseConnection()
    }

    /// Append writes transferred from a nested child into this buffer.
    private func appendWrites(_ writes: [WriteOp]) throws {
        try state.withLock { state in
            switch state.lifecycle {
            case .open:
                state.writeBuffer.append(contentsOf: writes)
            default:
                throw Self.error(for: state.lifecycle, operation: .write)
            }
        }
    }

    // MARK: - Error Mapping

    private static func invalidOperation(_ message: String, operation: StorageOperation) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: .postgreSQL,
            message: message
        )
    }

    private static func error(for lifecycle: Lifecycle, operation: StorageOperation) -> any Error {
        switch lifecycle {
        case .open:
            return invalidOperation("Transaction is open", operation: operation)
        case .committing:
            return invalidOperation("Transaction is already committing", operation: operation)
        case .committed:
            return invalidOperation("Transaction is already committed", operation: operation)
        case .cancelling:
            return invalidOperation("Transaction is cancelling", operation: operation)
        case .cancelled:
            return invalidOperation("Transaction is cancelled", operation: operation)
        case .failed(let error, _): return error
        case .commitUnknown(let error): return error
        }
    }

    private static func validateOpen(
        _ lifecycle: Lifecycle,
        operation: StorageOperation
    ) throws {
        guard case .open = lifecycle else {
            throw error(for: lifecycle, operation: operation)
        }
    }

    /// Normalize an arbitrary error for throwing. Cancellation and existing
    /// `StorageError`s pass through unchanged; everything else is mapped.
    private func storageError(from error: any Error, operation: StorageOperation) -> StorageError {
        if let storageError = error as? StorageError {
            return storageError
        }
        return PostgreSQLStorageEngine.mapError(error, operation: operation)
    }

    /// Mark this transaction terminal after an operation failure. This prevents
    /// a drained write buffer from making a later `commit()` appear successful.
    private func markFailed(_ error: any Error, operation: StorageOperation) -> any Error {
        if error is CancellationError {
            let cancellationStateError = Self.invalidOperation(
                "PostgreSQL transaction operation was cancelled",
                operation: operation
            )
            let transitioned = state.withLock { state -> Bool in
                guard case .open = state.lifecycle else { return false }
                state.lifecycle = .failed(
                    cancellationStateError,
                    cleanupRequired: requiresCleanup(state)
                )
                state.writeBuffer.removeAll(keepingCapacity: false)
                return true
            }
            if transitioned, !isNested {
                versionstampCompletion.fail(cancellationStateError)
            }
            return error
        }

        let mapped = storageError(from: error, operation: operation)
        let transitioned = state.withLock { state -> Bool in
            guard case .open = state.lifecycle else { return false }
            state.lifecycle = .failed(
                mapped,
                cleanupRequired: requiresCleanup(state)
            )
            state.writeBuffer.removeAll(keepingCapacity: false)
            return true
        }
        if transitioned, !isNested {
            versionstampCompletion.fail(mapped)
        }
        return mapped
    }

    private func requiresCleanup(_ state: MutableState) -> Bool {
        guard !isNested else {
            return false
        }
        return state.connection != nil || state.acquireTask != nil
    }

    private func rollback(
        connection: PostgresConnection,
        reason: String
    ) async -> Result<Void, StorageError> {
        do {
            try await connection.query(
                PostgresQuery(unsafeSQL: "ROLLBACK"),
                logger: logger
            )
            return .success(())
        } catch {
            let rollbackError = storageError(from: error, operation: .rollback)
            logger.error("PostgreSQL rollback failed", metadata: [
                "reason": "\(reason)",
                "error": "\(rollbackError)"
            ])
            return .failure(rollbackError)
        }
    }

    private func combinedRollbackError(
        originalError: StorageError,
        rollbackError: StorageError
    ) -> StorageError {
        StorageError(
            code: .backendFailure,
            operation: .rollback,
            backend: .postgreSQL,
            message: "PostgreSQL operation failed and rollback also failed",
            underlyingDescription: "operation=\(originalError); rollback=\(rollbackError)"
        )
    }

    // MARK: - Read

    public func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await readValue(
            for: key,
            snapshot: snapshot,
            maximumByteCount: nil
        )
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        guard maximumByteCount >= 0 else {
            throw StorageError.invalidPointReadMaximum(
                maximumByteCount,
                backend: .postgreSQL
            )
        }
        return try await readValue(
            for: key,
            snapshot: snapshot,
            maximumByteCount: maximumByteCount
        )
    }

    private func readValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int?
    ) async throws -> ByteString? {
        if let journal = try admittedJournal() {
            return try await journal.getValue(
                for: key, snapshot: snapshot, maximumByteCount: maximumByteCount,
                using: journalBackend
            )
        }
        let writeBuffer = try state.withLock { state in
            switch state.lifecycle {
            case .open:
                return state.writeBuffer
            default:
                throw Self.error(for: state.lifecycle, operation: .read)
            }
        }

        // Scan the buffer in reverse (read-your-writes), collecting atomic
        // mutations until an operation that determines the base value is found.
        // Atomics depend on the preceding value, so they replay in forward order
        // on top of the determined base (or the database value).
        var collectedAtomics: [(param: ByteString, mutationType: MutationType)] = []
        var base: ByteString?
        var baseDetermined = false

        scan: for op in writeBuffer.reversed() {
            switch op {
            case .set(let k, let v) where k == key:
                base = v
                baseDetermined = true
                break scan
            case .clear(let k) where k == key:
                base = nil
                baseDetermined = true
                break scan
            case .clearRange(let b, let e)
                where compareBytes(key, b) >= 0 && compareBytes(key, e) < 0:
                base = nil
                baseDetermined = true
                break scan
            case .atomic(let k, let param, let mutationType) where k == key:
                collectedAtomics.append((param, mutationType))
            default:
                continue
            }
        }

        // A native bound is safe only when the base value is the final value.
        // Atomic replay can reduce an oversized base (for example, a short
        // add or compare-and-clear), so fetch that base unbounded and enforce
        // the caller's bound after replay at the API boundary.
        let fetchMaximumByteCount = collectedAtomics.isEmpty
            ? maximumByteCount
            : nil
        var value = baseDetermined
            ? base
            : try await fetchBaseValue(
                key: key,
                snapshot: snapshot,
                maximumByteCount: fetchMaximumByteCount
            )
        // collectedAtomics is newest-first; replay oldest-first.
        for entry in collectedAtomics.reversed() {
            switch try entry.mutationType.apply(to: value, param: entry.param) {
            case .set(let bytes):
                value = bytes
            case .clear:
                value = nil
            case .unchanged:
                break
            }
        }
        if let maximumByteCount, let value {
            guard value.count <= maximumByteCount else {
                throw StorageError.pointReadValueTooLarge(
                    observedByteCount: value.count,
                    maximumByteCount: maximumByteCount,
                    backend: .postgreSQL
                )
            }
        }
        return value
    }

    private func fetchBaseValue(
        key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int?
    ) async throws -> ByteString? {
        if let parent {
            if let maximumByteCount {
                return try await parent.getValue(
                    for: key,
                    snapshot: snapshot,
                    maximumByteCount: maximumByteCount
                )
            }
            return try await parent.getValue(for: key, snapshot: snapshot)
        }
        return try await fetchValueFromDatabase(
            key: key,
            maximumByteCount: maximumByteCount
        )
    }

    private func fetchValueFromDatabase(
        key: ByteString,
        maximumByteCount: Int?
    ) async throws -> ByteString? {
        do {
            let connection = try await ensureConnection()
            var bindings = PostgresBindings()
            bindings.append(
                PostgreSQLBindingBytes.copyToOwnedBuffer(key),
                context: .default
            )
            let sql = "SELECT value FROM \(tableName) WHERE key = $1"
            let rows = try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
            for try await (value) in rows.decode(ByteBuffer.self) {
                if let maximumByteCount,
                   value.readableBytes > maximumByteCount {
                    throw StorageError.pointReadValueTooLarge(
                        observedByteCount: value.readableBytes,
                        maximumByteCount: maximumByteCount,
                        backend: .postgreSQL
                    )
                }
                return resultBytesFactory.makeByteString(retaining: value)
            }
            return nil
        } catch {
            if let storageError = error as? StorageError,
               storageError.isPointReadValueTooLarge {
                throw storageError
            }
            throw markFailed(error, operation: .read)
        }
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> PostgreSQLRangeResult {
        let admitted: TransactionJournal?
        do { admitted = try admittedJournal() }
        catch { return PostgreSQLRangeResult(error: error) }
        if let journal = admitted {
            return PostgreSQLRangeResult(cursor: journal.rangeCursor(
                from: begin, to: end, limit: limit, reverse: reverse,
                snapshot: snapshot, streamingMode: streamingMode, using: journalBackend
            ))
        }
        let stateError = state.withLock { state -> (any Error)? in
            switch state.lifecycle {
            case .open:
                return nil
            default:
                return Self.error(for: state.lifecycle, operation: .rangeRead)
            }
        }
        if let stateError {
            return PostgreSQLRangeResult(error: stateError)
        }

        let nestedParent = parent
        let nestedWriteCount = state.withLock { state in
            isNested ? state.writeBuffer.count : 0
        }
        if isNested {
            guard nestedWriteCount == 0 else {
                return PostgreSQLRangeResult(error: Self.invalidOperation(
                    "Nested PostgreSQL range reads with uncommitted child writes are not supported; "
                        + "commit or cancel the nested transaction before scanning",
                    operation: .rangeRead
                ))
            }
            if let nestedParent {
                return nestedParent.getRange(
                    from: begin,
                    to: end,
                    limit: limit,
                    reverse: reverse,
                    snapshot: snapshot,
                    streamingMode: streamingMode
                )
            }
        }

        // Resolve KeySelectors to SQL boundaries (see SQLRangeBoundary for the
        // full FDB-semantics mapping, including the lastLess* selectors).
        do {
            let plan = RangeScanPlan(
                begin: try SQLRangeBoundary.begin(begin),
                end: try SQLRangeBoundary.end(end),
                limit: limit,
                reverse: reverse,
                batchSize: Self.batchSize(for: streamingMode),
                tableName: tableName
            )
            return PostgreSQLRangeResult(transaction: self, plan: plan)
        } catch {
            return PostgreSQLRangeResult(error: error)
        }
    }

    /// Translate a streaming-mode hint into a keyset-pagination batch size.
    static func batchSize(for streamingMode: StreamingMode) -> Int {
        switch streamingMode {
        case .small:
            return 256
        case .iterator, .medium:
            return 1024
        case .wantAll, .exact, .large, .serial:
            return 4096
        }
    }

    /// Fetch the next page of a range scan via keyset pagination.
    ///
    /// Called by `PostgreSQLRangeResult.Cursor`. Always bounded to `batchSize`
    /// rows, so memory stays O(`batchSize`) however large the range is.
    func fetchRangeBatch(
        plan: RangeScanPlan,
        after lastKey: ByteString?,
        remaining: Int,
        flushFirst: Bool
    ) async throws -> [(ByteString, ByteString)] {
        do {
            let connection = try await ensureConnection()
            if flushFirst {
                try await flushWriteBuffer(connection: connection)
            }
            return try await journalBackend.fetchRangeBatch(
                plan: plan, after: lastKey, remaining: remaining
            )
        } catch {
            throw markFailed(error, operation: .rangeRead)
        }
    }

    /// Resolves one standard FDB key selector without synthesizing a second
    /// range boundary. SQL backends cannot represent the arbitrary offset that
    /// `TransactionKeySelection` uses as an exclusive end selector.
    private func selectKey(
        _ selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        if let journal = try admittedJournal() {
            return try await journal.getKey(selector: selector, snapshot: snapshot, using: journalBackend)
        }
        let nestedWriteCount = try state.withLock { state -> Int in
            switch state.lifecycle {
            case .open:
                return isNested ? state.writeBuffer.count : 0
            default:
                throw Self.error(
                    for: state.lifecycle,
                    operation: .rangeRead
                )
            }
        }
        if isNested {
            guard nestedWriteCount == 0 else {
                throw Self.invalidOperation(
                    "Nested PostgreSQL key selection with uncommitted child writes is not supported; "
                        + "commit or cancel the nested transaction before selecting a key",
                    operation: .rangeRead
                )
            }
            guard let parent else {
                throw Self.invalidOperation(
                    "Nested PostgreSQL key selection has no parent transaction",
                    operation: .rangeRead
                )
            }
            return try await parent.selectKey(
                selector,
                snapshot: snapshot
            )
        }

        let boundary = try SQLRangeBoundary.begin(selector)
        do {
            let connection = try await ensureConnection()
            try await flushWriteBuffer(connection: connection)

            var bindValues: [ByteString] = []
            let clause = Self.boundaryClause(
                boundary,
                tableName: tableName
            ) { key in
                bindValues.append(key)
                return "$\(bindValues.count)"
            }
            let sql = "SELECT key FROM \(tableName) WHERE \(clause) "
                + "ORDER BY key ASC LIMIT 1"
            var bindings = PostgresBindings()
            for value in bindValues {
                bindings.append(
                    PostgreSQLBindingBytes.copyToOwnedBuffer(value),
                    context: .default
                )
            }
            let rows = try await connection.query(
                PostgresQuery(unsafeSQL: sql, binds: bindings),
                logger: logger
            )
            for try await keyBuffer in rows.decode(ByteBuffer.self) {
                return resultBytesFactory.makeByteString(
                    retaining: keyBuffer
                )
            }
            return nil
        } catch {
            throw markFailed(error, operation: .rangeRead)
        }
    }

    /// Render a resolved range boundary into a SQL predicate, appending its bind.
    ///
    /// The `'\x'::bytea` fallback is the empty byte string (the minimum key):
    /// for a begin boundary `key >= ''` matches everything; for an end boundary
    /// `key < ''` matches nothing. This mirrors FDB's "before all keys" clamp.
    private static func boundaryClause(
        _ boundary: SQLRangeBoundary,
        tableName: String,
        placeholder: (ByteString) -> String
    ) -> String {
        switch boundary {
        case .direct(let op, let key):
            return "key \(op) \(placeholder(key))"
        case .resolvedSubquery(let op, let subqueryOp, let key):
            return "key \(op) COALESCE("
                + "(SELECT key FROM \(tableName) "
                + "WHERE key \(subqueryOp) \(placeholder(key)) "
                + "ORDER BY key DESC LIMIT 1), "
                + "'\\x'::bytea)"
        }
    }

    // MARK: - Write

    public func setValue(_ value: ByteString, for key: ByteString) throws {
        if let journal = try admittedJournal() {
            try journal.append(.set(key: key, value: value, suppressWriteConflict: false))
            return
        }
        try state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .write)
            try mutationByteMeter.recordSet(
                key: key,
                value: value
            )
            state.writeBuffer.append(.set(key: key, value: value))
        }
    }

    public func clear(key: ByteString) throws {
        if let journal = try admittedJournal() {
            try journal.append(.clear(key: key, suppressWriteConflict: false))
            return
        }
        try state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .delete)
            try mutationByteMeter.recordClear(key: key)
            state.writeBuffer.append(.clear(key: key))
        }
    }

    public func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        if let journal = try admittedJournal() {
            try journal.append(.clearRange(begin: beginKey, end: endKey, suppressWriteConflict: false))
            return
        }
        try state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .deleteRange)
            try mutationByteMeter.recordClearRange(
                beginKey: beginKey,
                endKey: endKey
            )
            state.writeBuffer.append(.clearRange(begin: beginKey, end: endKey))
        }
    }

    public func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        if let journal = try admittedJournal() {
            try journal.append(.atomic(
                key: key, parameter: param, type: mutationType, suppressWriteConflict: false
            ))
            return
        }
        try state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .write)
            try mutationByteMeter.recordAtomic(
                key: key,
                parameter: param
            )
            state.writeBuffer.append(.atomic(key: key, param: param, mutationType: mutationType))
        }
    }

    // MARK: - Transaction Control

    public func commit() async throws {
        defer { releaseContextLeaseIfTerminal() }
        try await commitWithoutReleasingContextLease()
    }

    private func commitWithoutReleasingContextLease() async throws {
        let start = beginCommit()
        switch start {
        case .leader(let completion):
            let result = await performCommit()
            switch result {
            case .success:
                completion.succeed()
            case .failure(let error):
                completion.fail(error)
            }
            try result.get()
        case .waitForCommit(let completion):
            try await completion.wait()
        case .waitForCancellation(let completion):
            try await completion.wait()
            throw Self.invalidOperation(
                "Transaction was cancelled",
                operation: .commit
            )
        case .committed:
            return
        case .cancelled:
            throw Self.invalidOperation(
                "Transaction was cancelled",
                operation: .commit
            )
        case .failed(let error):
            throw error
        }
    }

    public func cancel() async throws {
        defer { releaseContextLeaseIfTerminal() }
        try await cancelWithoutReleasingContextLease()
    }

    private func cancelWithoutReleasingContextLease() async throws {
        _ = noteAdmission()
        enum Start {
            case leader(TransactionOperationCompletion)
            case waitForCancellation(TransactionOperationCompletion)
            case waitForCommit(TransactionOperationCompletion)
            case cancelled
            case committed
            case cleanedFailure
            case commitUnknown(StorageError)
        }

        let start = state.withLock { state -> Start in
            switch state.lifecycle {
            case .open:
                let completion = TransactionOperationCompletion()
                state.lifecycle = .cancelling(completion)
                state.writeBuffer.removeAll(keepingCapacity: false)
                return .leader(completion)
            case .committing(let completion):
                return .waitForCommit(completion)
            case .committed:
                return .committed
            case .cancelling(let completion):
                return .waitForCancellation(completion)
            case .cancelled:
                return .cancelled
            case .failed(_, let cleanupRequired):
                guard cleanupRequired else {
                    return .cleanedFailure
                }
                let completion = TransactionOperationCompletion()
                state.lifecycle = .cancelling(completion)
                state.writeBuffer.removeAll(keepingCapacity: false)
                return .leader(completion)
            case .commitUnknown(let error):
                return .commitUnknown(error)
            }
        }

        switch start {
        case .leader(let completion):
            let result = await performCancellation()
            switch result {
            case .success:
                completion.succeed()
            case .failure(let error):
                completion.fail(error)
            }
            try result.get()
        case .waitForCancellation(let completion):
            try await completion.wait()
        case .waitForCommit(let completion):
            do {
                try await completion.wait()
            } catch {
                if let error = error as? StorageError, error.code == .commitUnknownResult {
                    throw error
                }
                return
            }
            throw Self.invalidOperation(
                "Transaction committed",
                operation: .cancel
            )
        case .cancelled, .cleanedFailure:
            return
        case .committed:
            throw Self.invalidOperation(
                "Transaction committed",
                operation: .cancel
            )
        case .commitUnknown(let error):
            throw error
        }
    }

    public func setReadVersion(_ version: Int64) throws {
        try state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .read)
        }
        throw StorageError.unsupportedOperation(
            "PostgreSQL cannot reopen a transaction at a historical transaction ID",
            operation: .read,
            backend: .postgreSQL
        )
    }

    public func getReadVersion() async throws -> Int64 {
        if try admittedJournal() != nil {
            return try await TransactionOperationExecutor(access: self).withOperation { [self] in
                do {
                    let connection = try await connectionOwner.ensureConnection(allowCancelling: true)
                    return try await transactionVersion(connection: connection, allowCancelling: true)
                } catch {
                    throw markFailed(error, operation: .read)
                }
            }
        }
        try state.withLock { try Self.validateOpen($0.lifecycle, operation: .read) }
        do {
            let connection = try await ensureConnection()
            return try await transactionVersion(connection: connection)
        } catch {
            throw markFailed(error, operation: .read)
        }
    }

    public func getCommittedVersion() throws -> Int64 {
        try state.withLock { state in
            guard case .committed = state.lifecycle,
                  let committedVersion = state.committedVersion else {
                throw Self.error(for: state.lifecycle, operation: .read)
            }
            return committedVersion
        }
    }

    public func requestVersionstamp() -> any PendingTransactionVersionstamp {
        _ = noteAdmission()
        let failure = state.withLock { state -> (any Error)? in
            guard case .open = state.lifecycle else {
                return Self.error(for: state.lifecycle, operation: .read)
            }
            return nil
        }
        if let failure {
            return TransactionVersionstampRequest(failure: failure)
        }
        return TransactionVersionstampRequest(
            completion: versionstampCompletion
        )
    }

    private func transactionVersion(
        connection: PostgresConnection,
        allowCancelling: Bool = false
    ) async throws -> Int64 {
        if let cached = state.withLock({ $0.readVersion }) {
            return cached
        }
        let rows = try await connection.query(
            PostgresQuery(unsafeSQL: "SELECT txid_current()::bigint"),
            logger: logger
        )
        var decodedVersion: Int64?
        for try await version in rows.decode(Int64.self) {
            guard decodedVersion == nil else {
                throw StorageError(
                    code: .dataCorruption,
                    operation: .read,
                    backend: .postgreSQL,
                    message: "PostgreSQL returned multiple transaction IDs"
                )
            }
            decodedVersion = version
        }
        guard let version = decodedVersion, version >= 0 else {
            throw StorageError(
                code: .dataCorruption,
                operation: .read,
                backend: .postgreSQL,
                message: "PostgreSQL returned an invalid transaction ID"
            )
        }
        try state.withLock { state in
            switch state.lifecycle {
            case .open, .committing:
                state.readVersion = version
            case .cancelling where allowCancelling:
                state.readVersion = version
            default:
                throw Self.error(for: state.lifecycle, operation: .read)
            }
        }
        return version
    }

    private func beginCommit() -> CommitStart {
        _ = noteAdmission()
        return state.withLock { state in
            switch state.lifecycle {
            case .open:
                let completion = TransactionOperationCompletion()
                state.lifecycle = .committing(completion)
                return .leader(completion)
            case .committing(let completion):
                return .waitForCommit(completion)
            case .committed:
                return .committed
            case .cancelling(let completion):
                return .waitForCancellation(completion)
            case .cancelled:
                return .cancelled
            case .failed(let error, _): return .failed(error)
            case .commitUnknown(let error): return .failed(error)
            }
        }
    }

    private func performCommit() async -> Result<Void, any Error> {
        guard let journal = checkpointState.withLock({ $0.journal }) else {
            return await performNativeCommit().mapError { $0 as any Error }
        }
        do {
            let writes = try await journal.sealForCommit().map(Self.nativeMutation)
            state.withLock { $0.writeBuffer = writes }
        } catch {
            var failure: any Error = error
            do { try await journal.close(committed: false) }
            catch {
                failure = StorageTransactionCleanupError(operationError: failure, cancellationError: error)
            }
            let cleanup = await performNativeCancellation(resolveVersionstamp: false)
            if case .failure(let cleanupError) = cleanup {
                if let combined = failure as? StorageTransactionCleanupError {
                    failure = combined.addingCancellationError(cleanupError)
                } else {
                    failure = StorageTransactionCleanupError(operationError: failure, cancellationError: cleanupError)
                }
            }
            state.withLock { $0.lifecycle = .failed(failure, cleanupRequired: false) }
            versionstampCompletion.fail(failure)
            return .failure(failure)
        }
        let result = await performNativeCommit()
        do {
            switch result {
            case .success: try await journal.close(committed: true)
            case .failure(let error) where error.code == .commitUnknownResult:
                try await journal.closeWithUnknownCommit()
            case .failure: try await journal.close(committed: false)
            }
        } catch {
            let failure: any Error
            if case .failure(let original) = result {
                failure = StorageTransactionCleanupError(operationError: original, cancellationError: error)
            } else { failure = error }
            state.withLock { $0.lifecycle = .failed(failure, cleanupRequired: false) }
            versionstampCompletion.fail(failure)
            return .failure(failure)
        }
        return result.mapError { $0 as any Error }
    }

    private static func nativeMutation(_ mutation: TransactionJournalMutation) throws -> WriteOp {
        switch mutation {
        case .set(let key, let value, _): return .set(key: key, value: value)
        case .clear(let key, _): return .clear(key: key)
        case .clearRange(let begin, let end, _): return .clearRange(begin: begin, end: end)
        case .atomic(let key, let parameter, let type, _):
            return .atomic(key: key, param: parameter, mutationType: type)
        case .writeConflict:
            throw StorageError.unsupportedOperation(
                "PostgreSQL does not support explicit write conflict ranges",
                operation: .write, backend: .postgreSQL
            )
        }
    }

    private func performNativeCommit() async -> Result<Void, StorageError> {
        if isNested {
            do {
                try commitNested()
                return .success(())
            } catch {
                let mapped = storageError(from: error, operation: .commit)
                return finishCommit(.failure(mapped))
            }
        }

        let ownsConnection = client != nil
        let connection: PostgresConnection
        do {
            connection = try await ensureConnection()
        } catch {
            let mapped = storageError(from: error, operation: .beginTransaction)
            if ownsConnection {
                releaseConnection()
            }
            return finishCommit(.failure(mapped))
        }

        do {
            _ = try await transactionVersion(connection: connection)
            try await flushWriteBuffer(connection: connection)
        } catch {
            let originalError = storageError(from: error, operation: .write)
            let result = await rollbackAfterKnownFailure(
                originalError,
                connection: connection,
                reason: "commit write flush"
            )
            if ownsConnection {
                releaseConnection()
            }
            return finishCommit(result)
        }

        do {
            try await connection.query(
                PostgresQuery(unsafeSQL: "COMMIT"),
                logger: logger
            )
            if ownsConnection {
                releaseConnection()
            }
            return finishCommit(.success(()))
        } catch {
            let mapped: StorageError
            if error is CancellationError {
                mapped = StorageError(
                    code: .commitUnknownResult,
                    operation: .commit,
                    backend: .postgreSQL,
                    message: "PostgreSQL commit result is unknown after task cancellation"
                )
            } else {
                mapped = storageError(from: error, operation: .commit)
            }

            let result: Result<Void, StorageError>
            if mapped.code == .commitUnknownResult {
                result = .failure(mapped)
            } else {
                result = await rollbackAfterKnownFailure(
                    mapped,
                    connection: connection,
                    reason: "commit statement"
                )
            }
            if ownsConnection {
                releaseConnection()
            }
            return finishCommit(result)
        }
    }

    private func releaseContextLeaseIfTerminal() {
        let mayRelease = state.withLock { state in
            switch state.lifecycle {
            case .committed, .cancelled, .commitUnknown,
                 .failed(_, cleanupRequired: false):
                return true
            case .open, .committing, .cancelling,
                 .failed(_, cleanupRequired: true):
                return false
            }
        }
        if mayRelease {
            contextLease?.release()
        }
    }

    private func finishCommit(
        _ result: Result<Void, StorageError>
    ) -> Result<Void, StorageError> {
        let versionstampResult = state.withLock {
            state -> Result<TransactionVersionstamp, StorageError> in
            let versionstampResult:
                Result<TransactionVersionstamp, StorageError>
            switch result {
            case .success:
                state.lifecycle = .committed
                state.committedVersion = state.readVersion
                guard let committedVersion = state.committedVersion else {
                    let error = StorageError(
                        code: .backendContractViolation,
                        operation: .commit,
                        backend: .postgreSQL,
                        message: "Committed PostgreSQL transaction has no transaction ID"
                    )
                    state.lifecycle = .failed(
                        error,
                        cleanupRequired: false
                    )
                    state.writeBuffer.removeAll(keepingCapacity: false)
                    return .failure(error)
                }
                do {
                    versionstampResult = .success(
                        try TransactionVersionstamp(
                            committedVersion: committedVersion
                        )
                    )
                } catch let error as StorageError {
                    state.lifecycle = .failed(
                        error,
                        cleanupRequired: false
                    )
                    versionstampResult = .failure(error)
                } catch {
                    let storageError = StorageError(
                        code: .backendContractViolation,
                        operation: .read,
                        backend: .postgreSQL,
                        message: "Unable to encode the committed versionstamp",
                        underlyingDescription: String(describing: error)
                    )
                    state.lifecycle = .failed(
                        storageError,
                        cleanupRequired: false
                    )
                    versionstampResult = .failure(storageError)
                }
            case .failure(let error) where error.code == .commitUnknownResult:
                state.lifecycle = .commitUnknown(error)
                versionstampResult = .failure(error)
            case .failure(let error):
                state.lifecycle = .failed(error, cleanupRequired: false)
                versionstampResult = .failure(error)
            }
            state.writeBuffer.removeAll(keepingCapacity: false)
            return versionstampResult
        }
        if !isNested {
            switch versionstampResult {
            case .success(let versionstamp):
                versionstampCompletion.succeed(versionstamp)
            case .failure(let error):
                versionstampCompletion.fail(error)
            }
        }
        switch versionstampResult {
        case .success:
            return result
        case .failure(let error):
            return .failure(error)
        }
    }

    private func rollbackAfterKnownFailure(
        _ originalError: StorageError,
        connection: PostgresConnection,
        reason: String
    ) async -> Result<Void, StorageError> {
        switch await rollback(connection: connection, reason: reason) {
        case .success:
            return .failure(originalError)
        case .failure(let rollbackError):
            return .failure(
                combinedRollbackError(
                    originalError: originalError,
                    rollbackError: rollbackError
                )
            )
        }
    }

    /// Commit a nested child by transferring its still-owned buffer only after
    /// the parent accepts the complete batch.
    private func commitNested() throws {
        guard let parent else {
            state.withLock { $0.lifecycle = .committed }
            return
        }
        let writes = try state.withLock { state -> [WriteOp] in
            guard case .committing = state.lifecycle else {
                throw Self.error(for: state.lifecycle, operation: .commit)
            }
            return state.writeBuffer
        }
        if !writes.isEmpty {
            try parent.appendWrites(writes)
        }
        state.withLock { state in
            state.writeBuffer.removeAll(keepingCapacity: false)
            state.lifecycle = .committed
        }
    }

    private func performCancellation() async -> Result<Void, any Error> {
        var journalFailure: (any Error)?
        if let journal = checkpointState.withLock({ $0.journal }) {
            do { try await journal.close(committed: false) }
            catch { journalFailure = error }
        }
        let result = await performNativeCancellation(resolveVersionstamp: journalFailure == nil)
        if let journalFailure {
            let failure: any Error
            if case .failure(let nativeError) = result {
                failure = StorageTransactionCleanupError(operationError: journalFailure, cancellationError: nativeError)
            } else { failure = journalFailure }
            state.withLock { $0.lifecycle = .failed(failure, cleanupRequired: false) }
            versionstampCompletion.fail(failure)
            return .failure(failure)
        }
        return result.mapError { $0 as any Error }
    }

    private func performNativeCancellation(resolveVersionstamp: Bool = true) async -> Result<Void, StorageError> {
        guard !isNested else {
            state.withLock { state in
                state.lifecycle = .cancelled
                state.writeBuffer.removeAll(keepingCapacity: false)
            }
            return .success(())
        }

        let acquisition = state.withLock { $0.acquireTask }
        if let acquisition {
            let acquisitionResult = await acquisition.result
            if case .failure(let error) = acquisitionResult {
                let mapped = storageError(from: error, operation: .rollback)
                if mapped.operation == .rollback,
                   mapped.code == .backendFailure {
                    finishCancellation(.failure(mapped), resolveVersionstamp: resolveVersionstamp)
                    return .failure(mapped)
                }
            }
        }

        let connection = state.withLock { $0.connection }
        let result: Result<Void, StorageError>
        if let connection {
            result = await rollback(
                connection: connection,
                reason: "transaction cancellation"
            )
        } else {
            result = .success(())
        }
        if client != nil {
            releaseConnection()
        }
        finishCancellation(result, resolveVersionstamp: resolveVersionstamp)
        return result
    }

    private func finishCancellation(_ result: Result<Void, StorageError>, resolveVersionstamp: Bool = true) {
        let versionstampFailure = state.withLock {
            state -> StorageError in
            switch result {
            case .success:
                state.lifecycle = .cancelled
                let error = Self.invalidOperation(
                    "Transaction was cancelled",
                    operation: .read
                )
                state.writeBuffer.removeAll(keepingCapacity: false)
                return error
            case .failure(let error):
                state.lifecycle = .failed(error, cleanupRequired: false)
                state.writeBuffer.removeAll(keepingCapacity: false)
                return error
            }
        }
        if !isNested && resolveVersionstamp {
            versionstampCompletion.fail(versionstampFailure)
        }
    }

    // MARK: - Internal (engine-managed eager path)

    func commitInternal(connection: PostgresConnection) async throws {
        _ = connection
        try await commit()
    }

    func rollbackInternal(connection: PostgresConnection) async throws {
        _ = connection
        try await cancel()
    }

    // MARK: - Write Buffer Flush

    /// Drain the buffer and apply it to the connection.
    ///
    /// Consecutive same-kind operations are grouped: runs of `set` become a single
    /// chunked upsert, runs of `clear` a single chunked delete. Draining before a
    /// throw is safe because the surrounding transaction rolls back on error.
    private func flushWriteBuffer(connection: PostgresConnection) async throws {
        let ops = try state.withLock { state -> [WriteOp] in
            switch state.lifecycle {
            case .open, .committing:
                break
            default:
                throw Self.error(for: state.lifecycle, operation: .write)
            }
            let ops = state.writeBuffer
            state.writeBuffer.removeAll()
            return ops
        }
        guard !ops.isEmpty else { return }

        var index = 0
        while index < ops.count {
            switch ops[index] {
            case .set:
                var pairs: [(ByteString, ByteString)] = []
                while index < ops.count, case .set(let key, let value) = ops[index] {
                    pairs.append((key, value))
                    index += 1
                }
                try await upsertBatch(connection: connection, pairs: pairs)

            case .clear:
                var keys: [ByteString] = []
                while index < ops.count, case .clear(let key) = ops[index] {
                    keys.append(key)
                    index += 1
                }
                try await deleteBatch(connection: connection, keys: keys)

            case .clearRange(let begin, let end):
                try await deleteRange(connection: connection, begin: begin, end: end)
                index += 1

            case .atomic(let key, let param, let mutationType):
                try await executeAtomicOp(
                    connection: connection,
                    key: key,
                    param: param,
                    mutationType: mutationType
                )
                index += 1
            }
        }
    }

    private func upsertBatch(connection: PostgresConnection, pairs: [(ByteString, ByteString)]) async throws {
        guard !pairs.isEmpty else { return }

        // Deduplicate within the batch (last write wins, first-seen order). A
        // single INSERT ... VALUES that names the same key twice would fail with
        // "ON CONFLICT DO UPDATE command cannot affect row a second time".
        var indexByKey: [ByteString: Int] = [:]
        var deduped: [(key: ByteString, value: ByteString)] = []
        for (key, value) in pairs {
            if let existing = indexByKey[key] {
                deduped[existing].value = value
            } else {
                indexByKey[key] = deduped.count
                deduped.append((key, value))
            }
        }

        var start = 0
        while start < deduped.count {
            let end = min(start + Self.maxBindRows, deduped.count)
            var tuples: [String] = []
            var bindings = PostgresBindings()
            var parameterIndex = 0
            for entry in deduped[start..<end] {
                tuples.append("($\(parameterIndex + 1), $\(parameterIndex + 2))")
                parameterIndex += 2
                bindings.append(
                    PostgreSQLBindingBytes.copyToOwnedBuffer(entry.key),
                    context: .default
                )
                bindings.append(
                    PostgreSQLBindingBytes.copyToOwnedBuffer(entry.value),
                    context: .default
                )
            }
            let sql = "INSERT INTO \(tableName) (key, value) VALUES \(tuples.joined(separator: ", ")) "
                + "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
            try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
            start = end
        }
    }

    private func deleteBatch(connection: PostgresConnection, keys: [ByteString]) async throws {
        guard !keys.isEmpty else { return }
        var start = 0
        while start < keys.count {
            let end = min(start + Self.maxBindRows, keys.count)
            var placeholders: [String] = []
            var bindings = PostgresBindings()
            var parameterIndex = 0
            for key in keys[start..<end] {
                placeholders.append("$\(parameterIndex + 1)")
                parameterIndex += 1
                bindings.append(
                    PostgreSQLBindingBytes.copyToOwnedBuffer(key),
                    context: .default
                )
            }
            let sql = "DELETE FROM \(tableName) WHERE key IN (\(placeholders.joined(separator: ", ")))"
            try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
            start = end
        }
    }

    private func deleteRange(connection: PostgresConnection, begin: ByteString, end: ByteString) async throws {
        var bindings = PostgresBindings()
        bindings.append(
            PostgreSQLBindingBytes.copyToOwnedBuffer(begin),
            context: .default
        )
        bindings.append(
            PostgreSQLBindingBytes.copyToOwnedBuffer(end),
            context: .default
        )
        let sql = "DELETE FROM \(tableName) WHERE key >= $1 AND key < $2"
        try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
    }

    /// Apply one atomic mutation via a transaction-scoped lock plus
    /// row-locked read-modify-write.
    ///
    /// `pg_advisory_xact_lock` covers missing rows that `FOR UPDATE` cannot
    /// lock; `FOR UPDATE` still protects existing rows.
    private func executeAtomicOp(
        connection: PostgresConnection,
        key: ByteString, param: ByteString, mutationType: MutationType
    ) async throws {
        try await lockAtomicKey(connection: connection, key: key)

        var selectBindings = PostgresBindings()
        selectBindings.append(
            PostgreSQLBindingBytes.copyToOwnedBuffer(key),
            context: .default
        )
        let selectSQL = "SELECT value FROM \(tableName) WHERE key = $1 FOR UPDATE"
        let rows = try await connection.query(
            PostgresQuery(unsafeSQL: selectSQL, binds: selectBindings),
            logger: logger
        )
        var current: ByteString?
        for try await (value) in rows.decode(ByteBuffer.self) {
            current = resultBytesFactory.makeByteString(retaining: value)
        }

        // Versionstamp mutations throw here (unsupported by non-FDB backends).
        switch try mutationType.apply(to: current, param: param) {
        case .set(let bytes):
            try await upsertBatch(connection: connection, pairs: [(key, bytes)])
        case .clear:
            try await deleteBatch(connection: connection, keys: [key])
        case .unchanged:
            break
        }
    }

    private func lockAtomicKey(connection: PostgresConnection, key: ByteString) async throws {
        var bindings = PostgresBindings()
        bindings.append(Self.advisoryLockID(for: key), context: .default)
        try await connection.query(
            PostgresQuery(unsafeSQL: "SELECT pg_advisory_xact_lock($1)", binds: bindings),
            logger: logger
        )
    }

    static func advisoryLockID(for key: ByteString) -> Int64 {
        var hash: UInt64 = 0xcbf29ce484222325
        for byte in key {
            hash ^= UInt64(byte)
            hash = hash &* 0x100000001b3
        }
        return Int64(bitPattern: hash)
    }
}

extension PostgreSQLStorageTransaction {
    public func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await selectKey(selector, snapshot: snapshot)
    }

    public func getValue(for key: ByteString) async throws -> ByteString? {
        try await getValue(for: key, snapshot: false)
    }

    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        KeyValueCursor(
            consuming: getRange(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        )
    }

    public func collectRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) async throws -> [(ByteString, ByteString)] {
        try await TransactionRangeCollection.collect(
            using: self,
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }
}
