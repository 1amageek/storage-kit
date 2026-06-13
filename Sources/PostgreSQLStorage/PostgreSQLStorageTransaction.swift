import StorageKit
import PostgresNIO
import NIOCore
import Logging
import Synchronization

/// StorageKit.Transaction implementation for PostgreSQL.
///
/// Write operations (`setValue`/`clear`/`clearRange`/`atomicOp`) are non-throwing
/// per the protocol, so they are buffered and flushed on commit or before a range
/// scan. `getValue` replays the buffer (read-your-writes); `getRange` flushes it.
///
/// ## Connection Lifecycle
///
/// A transaction is created in one of three modes:
///
/// 1. **Eager** (`init(connection:...)`, used by the engine's `withTransaction`/
///    `withAutoCommit`): the connection is supplied up front and the engine owns
///    BEGIN/COMMIT/ROLLBACK and connection release.
/// 2. **Lazy** (`init(client:...)`, used by `createTransaction` at top level):
///    a connection is acquired from the pool on the first async operation, and
///    this transaction owns BEGIN/COMMIT/ROLLBACK. The caller MUST call `commit()`
///    or `cancel()` to release the connection back to the pool.
/// 3. **Nested** (`init(parent:...)`, used by `createTransaction` under an active
///    scope): the parent's connection is reused. `commit()` merges the child buffer
///    into the parent; `cancel()` only discards. The parent controls the real
///    transaction lifecycle.
///
/// ## Lazy Acquisition and Parking
///
/// `PostgresClient` exposes only the scoped `withConnection`, so a lazily-acquired
/// connection is "parked": a background task enters `withConnection` and suspends
/// on a continuation until `releaseConnection()` resumes it. Concurrent first-touch
/// callers share a single cached acquisition `Task`, guaranteeing exactly one
/// connection is leased.
///
/// A transaction is single-logical-owner: calling `cancel()` while another task is
/// mid-read is misuse. The in-flight read may error, but connection release stays
/// exactly-once because `releaseConnection()` is idempotent.
public final class PostgreSQLStorageTransaction: Transaction, Sendable {

    public typealias RangeResult = PostgreSQLRangeResult

    private let isNested: Bool
    private let isInTransactionBlock: Bool
    private let tableName: String
    private let logger: Logger

    /// Parent transaction for nested transactions; nil otherwise.
    private let parent: PostgreSQLStorageTransaction?

    /// Client for lazy acquisition (set only for the top-level lazy path).
    private let client: PostgresClient?

    /// BEGIN statement issued after lazy acquisition (set only for the lazy path).
    private let beginStatement: String?

    private let state: Mutex<MutableState>

    /// Maximum rows bound per chunked statement. Each upsert row uses two
    /// parameters, so 1000 rows stays well under PostgreSQL's 65535 limit.
    static let maxBindRows = 1000

    private enum Lifecycle: Sendable {
        case open
        case committing
        case committed
        case cancelled
        case failed(StorageError)
    }

    private struct MutableState {
        var writeBuffer: [WriteOp] = []
        var lifecycle: Lifecycle = .open
        var connection: PostgresConnection? = nil
        var acquireTask: Task<PostgresConnection, any Error>? = nil
        var releaseContinuation: CheckedContinuation<Void, Never>? = nil

        init(connection: PostgresConnection? = nil) {
            self.connection = connection
        }
    }

    private enum WriteOp: Sendable {
        case set(key: Bytes, value: Bytes)
        case clear(key: Bytes)
        case clearRange(begin: Bytes, end: Bytes)
        case atomic(key: Bytes, param: Bytes, mutationType: MutationType)
    }

    private enum ConnectionOutcome {
        case existing(PostgresConnection)
        case pending(Task<PostgresConnection, any Error>)
    }

    private enum CommitStart {
        case alreadyCommitted
        case proceed
    }

    // MARK: - Initializers

    /// Eager-connection init (engine-managed lifecycle).
    init(
        connection: PostgresConnection,
        isInTransactionBlock: Bool,
        tableName: String,
        logger: Logger
    ) {
        self.isNested = false
        self.isInTransactionBlock = isInTransactionBlock
        self.tableName = tableName
        self.logger = logger
        self.parent = nil
        self.client = nil
        self.beginStatement = nil
        self.state = Mutex(MutableState(connection: connection))
    }

    /// Nested-transaction init (parent owns the connection).
    init(parent: PostgreSQLStorageTransaction, logger: Logger) {
        self.isNested = true
        self.isInTransactionBlock = parent.isInTransactionBlock
        self.tableName = parent.tableName
        self.logger = logger
        self.parent = parent
        self.client = nil
        self.beginStatement = nil
        self.state = Mutex(MutableState())
    }

    /// Lazy-connection init (this transaction owns the connection).
    init(
        client: PostgresClient,
        beginStatement: String,
        isInTransactionBlock: Bool,
        tableName: String,
        logger: Logger
    ) {
        self.isNested = false
        self.isInTransactionBlock = isInTransactionBlock
        self.tableName = tableName
        self.logger = logger
        self.parent = nil
        self.client = client
        self.beginStatement = beginStatement
        self.state = Mutex(MutableState())
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
            case .open, .committing:
                break
            default:
                throw Self.error(for: state.lifecycle, operation: .beginTransaction)
            }
        }

        if let parent {
            return try await parent.ensureConnection()
        }

        let outcome: ConnectionOutcome = try state.withLock { state in
            switch state.lifecycle {
            case .open, .committing:
                break
            default:
                throw Self.error(for: state.lifecycle, operation: .beginTransaction)
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
                try await self.acquireAndBegin(client: client, beginStatement: beginStatement)
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
        beginStatement: String?
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
        let parkingError = state.withLock { state -> StorageError? in
            switch state.lifecycle {
            case .open, .committing:
                return nil
            default:
                return Self.error(for: state.lifecycle, operation: .beginTransaction)
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
        let publishError = state.withLock { state -> StorageError? in
            switch state.lifecycle {
            case .open, .committing:
                break
            default:
                return Self.error(for: state.lifecycle, operation: .beginTransaction)
            }
            state.connection = connection
            return nil
        }
        if let publishError {
            if beginStatement != nil {
                await rollbackBestEffort(connection: connection, reason: "cancelled during acquire")
            }
            releaseConnection()
            throw publishError
        }
        return connection
    }

    /// Lease a connection and keep it parked until `releaseConnection()` runs.
    ///
    /// The holder task captures self strongly so the transaction cannot deinit
    /// while a connection is leased. If the caller never calls `commit()`/`cancel()`
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
    private func releaseConnection() {
        let continuation = state.withLock { state -> CheckedContinuation<Void, Never>? in
            let continuation = state.releaseContinuation
            state.releaseContinuation = nil
            state.connection = nil
            return continuation
        }
        continuation?.resume()
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

    private static func error(for lifecycle: Lifecycle, operation: StorageOperation) -> StorageError {
        switch lifecycle {
        case .open:
            return invalidOperation("Transaction is open", operation: operation)
        case .committing:
            return invalidOperation("Transaction is already committing", operation: operation)
        case .committed:
            return invalidOperation("Transaction is already committed", operation: operation)
        case .cancelled:
            return invalidOperation("Transaction is cancelled", operation: operation)
        case .failed(let error):
            return error
        }
    }

    /// Normalize an arbitrary error for throwing. Cancellation and existing
    /// `StorageError`s pass through unchanged; everything else is mapped.
    private func storageError(from error: any Error, operation: StorageOperation) -> any Error {
        if error is CancellationError {
            return error
        }
        if let storageError = error as? StorageError {
            return storageError
        }
        return PostgreSQLStorageEngine.mapError(error, operation: operation)
    }

    /// Mark this transaction terminal after an operation failure. This prevents
    /// a drained write buffer from making a later `commit()` appear successful.
    private func markFailed(_ error: any Error, operation: StorageOperation) -> any Error {
        if error is CancellationError {
            state.withLock { state in
                switch state.lifecycle {
                case .open, .committing:
                    state.lifecycle = .cancelled
                    state.writeBuffer.removeAll()
                case .committed, .cancelled, .failed:
                    break
                }
            }
            return error
        }

        let mapped = storageError(from: error, operation: operation)
        guard let storageError = mapped as? StorageError else {
            return mapped
        }

        state.withLock { state in
            switch state.lifecycle {
            case .open, .committing:
                state.lifecycle = .failed(storageError)
                state.writeBuffer.removeAll()
            case .committed, .cancelled, .failed:
                break
            }
        }
        return storageError
    }

    private func markRolledBackAfterFailure() {
        state.withLock { state in
            if case .failed = state.lifecycle {
                state.lifecycle = .cancelled
            }
        }
    }

    private func rollbackBestEffort(connection: PostgresConnection, reason: String) async {
        do {
            try await connection.query(PostgresQuery(unsafeSQL: "ROLLBACK"), logger: logger)
        } catch {
            logger.warning("PostgreSQL rollback failed", metadata: [
                "reason": "\(reason)",
                "error": "\(error)"
            ])
        }
    }

    // MARK: - Read

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
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
        var collectedAtomics: [(param: Bytes, mutationType: MutationType)] = []
        var base: Bytes?
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

        var value = baseDetermined ? base : try await fetchBaseValue(key: key, snapshot: snapshot)
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
        return value
    }

    private func fetchBaseValue(key: Bytes, snapshot: Bool) async throws -> Bytes? {
        if let parent {
            return try await parent.getValue(for: key, snapshot: snapshot)
        }
        return try await fetchValueFromDatabase(key: key)
    }

    private func fetchValueFromDatabase(key: Bytes) async throws -> Bytes? {
        do {
            let connection = try await ensureConnection()
            var bindings = PostgresBindings()
            bindings.append(ByteBuffer(bytes: key), context: .default)
            let sql = "SELECT value FROM \(tableName) WHERE key = $1"
            let rows = try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
            for try await (value) in rows.decode(ByteBuffer.self) {
                return Array(value.readableBytesView)
            }
            return nil
        } catch {
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
        let stateError = state.withLock { state -> StorageError? in
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
    /// Called by `PostgreSQLRangeResult.Iterator`. Always bounded to `batchSize`
    /// rows, so memory stays O(`batchSize`) however large the range is.
    func fetchRangeBatch(
        plan: RangeScanPlan,
        after lastKey: Bytes?,
        remaining: Int,
        flushFirst: Bool
    ) async throws -> [(Bytes, Bytes)] {
        do {
            let connection = try await ensureConnection()
            if flushFirst {
                try await flushWriteBuffer(connection: connection)
            }
            let batchLimit = plan.limit > 0 ? min(plan.batchSize, remaining) : plan.batchSize
            guard batchLimit > 0 else { return [] }

            var bindValues: [Bytes] = []
            func placeholder(for key: Bytes) -> String {
                bindValues.append(key)
                return "$\(bindValues.count)"
            }

            var clauses: [String] = []
            clauses.append(Self.boundaryClause(plan.begin, tableName: plan.tableName, placeholder: placeholder))
            clauses.append(Self.boundaryClause(plan.end, tableName: plan.tableName, placeholder: placeholder))
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
                bindings.append(ByteBuffer(bytes: value), context: .default)
            }
            let rows = try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
            var results: [(Bytes, Bytes)] = []
            for try await (keyBuffer, valueBuffer) in rows.decode((ByteBuffer, ByteBuffer).self) {
                results.append((Array(keyBuffer.readableBytesView), Array(valueBuffer.readableBytesView)))
            }
            return results
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
        placeholder: (Bytes) -> String
    ) -> String {
        switch boundary {
        case .direct(let op, let key):
            return "key \(op) \(placeholder(key))"
        case .resolvedSubquery(let op, let subqueryOp, let key):
            return "key \(op) COALESCE("
                + "(SELECT max(key) FROM \(tableName) WHERE key \(subqueryOp) \(placeholder(key))), "
                + "'\\x'::bytea)"
        }
    }

    // MARK: - Write (buffered, non-throwing per protocol)

    public func setValue(_ value: Bytes, for key: Bytes) {
        state.withLock { state in
            guard case .open = state.lifecycle else { return }
            state.writeBuffer.append(.set(key: key, value: value))
        }
    }

    public func clear(key: Bytes) {
        state.withLock { state in
            guard case .open = state.lifecycle else { return }
            state.writeBuffer.append(.clear(key: key))
        }
    }

    public func clearRange(beginKey: Bytes, endKey: Bytes) {
        state.withLock { state in
            guard case .open = state.lifecycle else { return }
            state.writeBuffer.append(.clearRange(begin: beginKey, end: endKey))
        }
    }

    public func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) {
        state.withLock { state in
            guard case .open = state.lifecycle else { return }
            state.writeBuffer.append(.atomic(key: key, param: param, mutationType: mutationType))
        }
    }

    // MARK: - Transaction Control

    public func commit() async throws {
        let start = try state.withLock { state -> CommitStart in
            switch state.lifecycle {
            case .open:
                state.lifecycle = .committing
                return .proceed
            case .committed:
                return .alreadyCommitted
            default:
                throw Self.error(for: state.lifecycle, operation: .commit)
            }
        }
        guard start == .proceed else { return }

        if isNested {
            do {
                try await commitNested()
            } catch {
                throw markFailed(error, operation: .commit)
            }
            return
        }

        let ownsConnection = client != nil
        let (existingConnection, hasWrites) = state.withLock { state -> (PostgresConnection?, Bool) in
            (state.connection, !state.writeBuffer.isEmpty)
        }

        // Never touched the database and nothing to write — a no-op commit.
        if existingConnection == nil, !hasWrites {
            state.withLock { $0.lifecycle = .committed }
            return
        }

        let connection: PostgresConnection
        do {
            connection = try await ensureConnection()
        } catch {
            throw markFailed(error, operation: .beginTransaction)
        }

        do {
            try await flushWriteBuffer(connection: connection)
            if isInTransactionBlock {
                try await connection.query(PostgresQuery(unsafeSQL: "COMMIT"), logger: logger)
            }
            state.withLock { $0.lifecycle = .committed }
        } catch {
            if isInTransactionBlock {
                await rollbackBestEffort(connection: connection, reason: "commit")
            }
            if ownsConnection { releaseConnection() }
            throw markFailed(error, operation: .commit)
        }
        if ownsConnection { releaseConnection() }
    }

    /// Commit a nested child: merge its writes into the parent.
    private func commitNested() async throws {
        guard let parent else {
            state.withLock { $0.lifecycle = .committed }
            return
        }
        let writes = state.withLock { state -> [WriteOp] in
            let writes = state.writeBuffer
            state.writeBuffer.removeAll()
            return writes
        }
        if !writes.isEmpty {
            try parent.appendWrites(writes)
        }
        state.withLock { $0.lifecycle = .committed }
    }

    public func cancel() {
        enum Action {
            case none
            case rollbackAndRelease(PostgresConnection)
        }
        let action: Action = state.withLock { state in
            switch state.lifecycle {
            case .open, .failed:
                state.lifecycle = .cancelled
            case .committing, .committed, .cancelled:
                return .none
            }
            state.writeBuffer.removeAll()
            // Only a top-level lazy transaction inside a BEGIN owns rollback.
            // ROLLBACK must complete before the connection returns to the pool,
            // otherwise another transaction could pick it up mid-rollback.
            if !isNested, client != nil, isInTransactionBlock, let connection = state.connection {
                return .rollbackAndRelease(connection)
            }
            return .none
        }
        switch action {
        case .none:
            break
        case .rollbackAndRelease(let connection):
            Task { [self] in
                await rollbackBestEffort(connection: connection, reason: "cancel")
                releaseConnection()
            }
        }
    }

    // MARK: - Internal (engine-managed eager path)

    func commitInternal(
        connection: PostgresConnection,
        skipCommitStatement: Bool = false
    ) async throws {
        let start = try state.withLock { state -> CommitStart in
            switch state.lifecycle {
            case .open:
                state.lifecycle = .committing
                return .proceed
            case .committed:
                return .alreadyCommitted
            default:
                throw Self.error(for: state.lifecycle, operation: .commit)
            }
        }
        guard start == .proceed else { return }

        do {
            try await flushWriteBuffer(connection: connection)
            if !skipCommitStatement {
                try await connection.query(PostgresQuery(unsafeSQL: "COMMIT"), logger: logger)
            }
            state.withLock { $0.lifecycle = .committed }
        } catch {
            let mapped = markFailed(error, operation: .commit)
            if !skipCommitStatement {
                await rollbackBestEffort(connection: connection, reason: "commitInternal")
                markRolledBackAfterFailure()
            }
            throw mapped
        }
    }

    func rollbackInternal(connection: PostgresConnection) async {
        let shouldRollback = state.withLock { state -> Bool in
            switch state.lifecycle {
            case .committed, .cancelled:
                return false
            case .open, .committing, .failed:
                break
            }
            state.writeBuffer.removeAll()
            state.lifecycle = .cancelled
            return true
        }
        guard shouldRollback else { return }
        await rollbackBestEffort(connection: connection, reason: "rollbackInternal")
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

        do {
            var index = 0
            while index < ops.count {
                switch ops[index] {
                case .set:
                    var pairs: [(Bytes, Bytes)] = []
                    while index < ops.count, case .set(let key, let value) = ops[index] {
                        pairs.append((key, value))
                        index += 1
                    }
                    try await upsertBatch(connection: connection, pairs: pairs)

                case .clear:
                    var keys: [Bytes] = []
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
                        connection: connection, key: key, param: param, mutationType: mutationType
                    )
                    index += 1
                }
            }
        } catch {
            throw markFailed(error, operation: .write)
        }
    }

    private func upsertBatch(connection: PostgresConnection, pairs: [(Bytes, Bytes)]) async throws {
        guard !pairs.isEmpty else { return }

        // Deduplicate within the batch (last write wins, first-seen order). A
        // single INSERT ... VALUES that names the same key twice would fail with
        // "ON CONFLICT DO UPDATE command cannot affect row a second time".
        var indexByKey: [Bytes: Int] = [:]
        var deduped: [(key: Bytes, value: Bytes)] = []
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
                bindings.append(ByteBuffer(bytes: entry.key), context: .default)
                bindings.append(ByteBuffer(bytes: entry.value), context: .default)
            }
            let sql = "INSERT INTO \(tableName) (key, value) VALUES \(tuples.joined(separator: ", ")) "
                + "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
            try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
            start = end
        }
    }

    private func deleteBatch(connection: PostgresConnection, keys: [Bytes]) async throws {
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
                bindings.append(ByteBuffer(bytes: key), context: .default)
            }
            let sql = "DELETE FROM \(tableName) WHERE key IN (\(placeholders.joined(separator: ", ")))"
            try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
            start = end
        }
    }

    private func deleteRange(connection: PostgresConnection, begin: Bytes, end: Bytes) async throws {
        var bindings = PostgresBindings()
        bindings.append(ByteBuffer(bytes: begin), context: .default)
        bindings.append(ByteBuffer(bytes: end), context: .default)
        let sql = "DELETE FROM \(tableName) WHERE key >= $1 AND key < $2"
        try await connection.query(PostgresQuery(unsafeSQL: sql, binds: bindings), logger: logger)
    }

    /// Apply one atomic mutation via a transaction-scoped lock plus
    /// row-locked read-modify-write.
    ///
    /// `pg_advisory_xact_lock` covers missing rows that `FOR UPDATE` cannot
    /// lock; `FOR UPDATE` still protects existing rows. Atomics require an
    /// explicit transaction block; in auto-commit mode the read and the write
    /// would not be atomic, so the operation fails loudly instead of silently
    /// losing atomicity.
    private func executeAtomicOp(
        connection: PostgresConnection,
        key: Bytes, param: Bytes, mutationType: MutationType
    ) async throws {
        guard isInTransactionBlock else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .postgreSQL,
                message: "Atomic operation '\(mutationType)' requires an explicit transaction block; "
                    + "it is not supported in auto-commit mode"
            )
        }

        try await lockAtomicKey(connection: connection, key: key)

        var selectBindings = PostgresBindings()
        selectBindings.append(ByteBuffer(bytes: key), context: .default)
        let selectSQL = "SELECT value FROM \(tableName) WHERE key = $1 FOR UPDATE"
        let rows = try await connection.query(
            PostgresQuery(unsafeSQL: selectSQL, binds: selectBindings),
            logger: logger
        )
        var current: Bytes?
        for try await (value) in rows.decode(ByteBuffer.self) {
            current = Array(value.readableBytesView)
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

    private func lockAtomicKey(connection: PostgresConnection, key: Bytes) async throws {
        var bindings = PostgresBindings()
        bindings.append(Self.advisoryLockID(for: key), context: .default)
        try await connection.query(
            PostgresQuery(unsafeSQL: "SELECT pg_advisory_xact_lock($1)", binds: bindings),
            logger: logger
        )
    }

    static func advisoryLockID(for key: Bytes) -> Int64 {
        var hash: UInt64 = 0xcbf29ce484222325
        for byte in key {
            hash ^= UInt64(byte)
            hash = hash &* 0x100000001b3
        }
        return Int64(bitPattern: hash)
    }
}
