import StorageKit
import PostgresNIO
import NIOCore
import Logging
import Synchronization

/// StorageKit.Transaction implementation for PostgreSQL.
///
/// Write operations (`setValue`/`clear`/`clearRange`) are non-throwing per the protocol,
/// so writes are buffered and flushed on commit or before range scans.
/// `getValue` checks the buffer in reverse order to provide read-your-writes semantics.
///
/// ## Connection Lifecycle
///
/// Transactions can be created in two modes:
///
/// 1. **Eager connection** (via `withTransaction`): Connection is provided at init.
///    The engine manages BEGIN/COMMIT/ROLLBACK.
///
/// 2. **Lazy connection** (via `createTransaction`): Connection is acquired on the
///    first async operation. The transaction manages its own BEGIN/COMMIT/ROLLBACK.
///    The caller MUST call `commit()` or `cancel()` to release the connection.
///
/// ## Nested Transaction Support
///
/// When `isNested` is true, this transaction reuses the parent's connection:
/// - `commit()` flushes the write buffer but does not issue COMMIT
/// - `cancel()` discards the write buffer but does not issue ROLLBACK
/// - The parent transaction controls the actual PostgreSQL transaction lifecycle
public final class PostgreSQLStorageTransaction: Transaction, @unchecked Sendable {

    public typealias RangeResult = PostgreSQLRangeResult

    nonisolated(unsafe) private(set) var currentConnection: PostgresConnection?
    private let isNested: Bool
    private let logger: Logger

    /// Parent transaction for nested transactions.
    /// When nested commit has no connection, writes are merged into parent's buffer.
    private let parent: PostgreSQLStorageTransaction?

    /// Client for lazy connection acquisition (only set for createTransaction path)
    private let client: PostgresClient?
    private let beginStatement: String?

    /// Continuation for releasing the scoped connection back to the pool.
    /// Set when a connection is lazily acquired via `ensureConnection()`.
    nonisolated(unsafe) private var connectionRelease: CheckedContinuation<Void, Never>?

    private struct MutableState: Sendable {
        var writeBuffer: [WriteOp] = []
        var committed = false
        var cancelled = false
        var connectionAcquired = false
    }
    private let _state: Mutex<MutableState>

    private enum WriteOp: Sendable {
        case set(key: Bytes, value: Bytes)
        case clear(key: Bytes)
        case clearRange(begin: Bytes, end: Bytes)
        case atomic(key: Bytes, param: Bytes, mutationType: MutationType)
    }

    /// Eager connection init (used by `withTransaction`).
    /// Connection lifecycle is managed by the engine.
    init(
        connection: PostgresConnection?,
        isNested: Bool,
        logger: Logger
    ) {
        self.currentConnection = connection
        self.parent = nil
        self.client = nil
        self.beginStatement = nil
        self.isNested = isNested
        self.logger = logger
        self._state = Mutex(MutableState(connectionAcquired: connection != nil))
    }

    /// Nested transaction init (used by `createTransaction` when ActiveTransactionScope is active).
    /// Delegates connection access to the parent rather than copying a snapshot.
    /// This ensures the nested transaction sees the parent's connection even if
    /// the parent acquires it lazily after the nested transaction is created.
    init(
        parent: PostgreSQLStorageTransaction,
        logger: Logger
    ) {
        self.currentConnection = nil
        self.parent = parent
        self.client = nil
        self.beginStatement = nil
        self.isNested = true
        self.logger = logger
        self._state = Mutex(MutableState())
    }

    /// Lazy connection init (used by `createTransaction`).
    /// Connection will be acquired on first async operation.
    init(
        client: PostgresClient,
        beginStatement: String,
        isNested: Bool,
        logger: Logger
    ) {
        self.currentConnection = nil
        self.parent = nil
        self.client = client
        self.beginStatement = beginStatement
        self.isNested = isNested
        self.logger = logger
        self._state = Mutex(MutableState())
    }

    // MARK: - Lazy Connection Acquisition

    /// Acquire a connection, delegating to parent for nested transactions.
    ///
    /// For nested transactions, the parent is responsible for connection lifecycle.
    /// For top-level lazy transactions, acquires from the pool on first call.
    ///
    /// The connection is "parked" inside `withConnection` via a checked continuation
    /// until `releaseConnection()` is called from `commit()` or `cancel()`.
    /// `connectionRelease` is set BEFORE the outer continuation resumes to prevent
    /// a race where `releaseConnection()` is called before `connectionRelease` is assigned.
    private func ensureConnection() async throws -> PostgresConnection {
        if let conn = currentConnection {
            return conn
        }

        // Nested: delegate to parent. The parent will acquire lazily if needed.
        if let parent {
            return try await parent.ensureConnection()
        }

        // Pre-check: already cancelled?
        try _state.withLock { state in
            guard !state.cancelled else {
                throw StorageError.invalidOperation("Transaction cancelled")
            }
        }

        guard let client else {
            throw StorageError.invalidOperation(
                "No connection available and no client for lazy acquisition"
            )
        }

        // Acquire a connection from the pool.
        // connectionRelease is assigned inside the withCheckedContinuation closure
        // (which runs synchronously) BEFORE resuming the outer continuation.
        // This guarantees connectionRelease is set before any code that might
        // call releaseConnection() has a chance to run.
        let conn: PostgresConnection = try await withCheckedThrowingContinuation { continuation in
            Task { [self] in
                do {
                    try await client.withConnection { conn -> Void in
                        await withCheckedContinuation { releaseContinuation in
                            self.connectionRelease = releaseContinuation
                            continuation.resume(returning: conn)
                        }
                    }
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }

        // Post-check: cancelled during connection acquisition?
        let wasCancelled = _state.withLock { state -> Bool in
            if state.cancelled { return true }
            state.connectionAcquired = true
            return false
        }

        if wasCancelled {
            releaseConnection()
            throw StorageError.invalidOperation("Transaction cancelled")
        }

        self.currentConnection = conn

        // Issue BEGIN
        if let beginStatement {
            do {
                try await conn.query(
                    PostgresQuery(unsafeSQL: beginStatement),
                    logger: logger
                )
            } catch {
                self.currentConnection = nil
                _state.withLock { $0.connectionAcquired = false }
                releaseConnection()
                throw error
            }
        }

        return conn
    }

    /// Release the connection back to the pool (for lazy-acquired connections).
    private func releaseConnection() {
        connectionRelease?.resume()
        connectionRelease = nil
    }

    // MARK: - Read

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        let writeBuffer = try _state.withLock { state in
            guard !state.cancelled else {
                throw StorageError.invalidOperation("Transaction cancelled")
            }
            return state.writeBuffer
        }

        // Check write buffer in reverse order (read-your-writes)
        for op in writeBuffer.reversed() {
            switch op {
            case .set(let k, let v) where k == key:
                return v
            case .clear(let k) where k == key:
                return nil
            case .clearRange(let b, let e)
                where compareBytes(key, b) >= 0 && compareBytes(key, e) < 0:
                return nil
            default:
                continue
            }
        }

        let conn = try await ensureConnection()

        let keyBuf = ByteBuffer(bytes: key)
        let rows = try await conn.query(
            "SELECT value FROM kv_store WHERE key = \(keyBuf)",
            logger: logger
        )
        for try await (value,) in rows.decode(ByteBuffer.self) {
            return Array(value.readableBytesView)
        }
        return nil
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> PostgreSQLRangeResult {
        let cancelled = _state.withLock { $0.cancelled }
        guard !cancelled else {
            return PostgreSQLRangeResult(
                error: StorageError.invalidOperation("Transaction cancelled")
            )
        }

        let (beginKey, beginOp) = Self.resolveBeginForSQL(begin)
        let (endKey, endOp) = Self.resolveEndForSQL(end)
        let logger = self.logger

        return PostgreSQLRangeResult { [self] in

            let conn = try await self.ensureConnection()

            // Flush write buffer before range scan
            try await self.flushWriteBuffer(connection: conn)

            let order = reverse ? "DESC" : "ASC"
            let beginBuf = ByteBuffer(bytes: beginKey)
            let endBuf = ByteBuffer(bytes: endKey)

            // Build SQL with safe parameter binding
            // Operators are controlled strings from resolveBeginForSQL/resolveEndForSQL
            // (always one of: >=, >, <, <=) — safe to embed in SQL
            let sql: String
            if limit > 0 {
                sql = "SELECT key, value FROM kv_store WHERE key \(beginOp) $1 AND key \(endOp) $2 ORDER BY key \(order) LIMIT \(limit)"
            } else {
                sql = "SELECT key, value FROM kv_store WHERE key \(beginOp) $1 AND key \(endOp) $2 ORDER BY key \(order)"
            }

            var bindings = PostgresBindings()
            bindings.append(beginBuf, context: .default)
            bindings.append(endBuf, context: .default)
            let query = PostgresQuery(unsafeSQL: sql, binds: bindings)

            let rows = try await conn.query(query, logger: logger)
            var results: [(Bytes, Bytes)] = []
            for try await (keyBuf, valueBuf) in rows.decode((ByteBuffer, ByteBuffer).self) {
                results.append((
                    Array(keyBuf.readableBytesView),
                    Array(valueBuf.readableBytesView)
                ))
            }
            return results
        }
    }

    // MARK: - Write (buffered, non-throwing per protocol)

    public func setValue(_ value: Bytes, for key: Bytes) {
        _state.withLock { state in
            guard !state.cancelled else { return }
            state.writeBuffer.append(.set(key: key, value: value))
        }
    }

    public func clear(key: Bytes) {
        _state.withLock { state in
            guard !state.cancelled else { return }
            state.writeBuffer.append(.clear(key: key))
        }
    }

    public func clearRange(beginKey: Bytes, endKey: Bytes) {
        _state.withLock { state in
            guard !state.cancelled else { return }
            state.writeBuffer.append(.clearRange(begin: beginKey, end: endKey))
        }
    }

    // MARK: - Atomic Operations

    public func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) {
        _state.withLock { state in
            guard !state.cancelled else { return }
            state.writeBuffer.append(.atomic(key: key, param: param, mutationType: mutationType))
        }
    }

    // MARK: - Transaction Control

    public func commit() async throws {
        let shouldProceed = try _state.withLock { state -> Bool in
            guard !state.cancelled else {
                throw StorageError.invalidOperation("Transaction cancelled")
            }
            return !state.committed
        }
        guard shouldProceed else { return }

        if isNested {
            // Nested: delegate connection access to parent.
            // If parent has a connection, flush directly to the shared connection.
            // If parent hasn't acquired a connection yet (lazy), transfer writes
            // to parent's buffer so they are flushed when parent commits.
            if let parent {
                if let conn = parent.currentConnection {
                    try await flushWriteBuffer(connection: conn)
                } else {
                    let writes = _state.withLock { state -> [WriteOp] in
                        let ops = state.writeBuffer
                        state.writeBuffer.removeAll()
                        return ops
                    }
                    if !writes.isEmpty {
                        parent._state.withLock { $0.writeBuffer.append(contentsOf: writes) }
                    }
                }
            }
            _state.withLock { $0.committed = true }
        } else {
            let hasWrites = _state.withLock { !$0.writeBuffer.isEmpty }
            let wasLazilyAcquired = client != nil

            if let conn = currentConnection {
                // Connection available — flush and commit
                do {
                    try await flushWriteBuffer(connection: conn)
                    try await conn.query("COMMIT", logger: logger)
                    _state.withLock { $0.committed = true }
                } catch {
                    _ = try? await conn.query("ROLLBACK", logger: logger)
                    if wasLazilyAcquired { releaseConnection() }
                    throw error
                }
                if wasLazilyAcquired { releaseConnection() }
            } else if hasWrites {
                // No connection but writes buffered — acquire, flush, commit
                let conn = try await ensureConnection()
                do {
                    try await flushWriteBuffer(connection: conn)
                    try await conn.query("COMMIT", logger: logger)
                    _state.withLock { $0.committed = true }
                } catch {
                    _ = try? await conn.query("ROLLBACK", logger: logger)
                    releaseConnection()
                    throw error
                }
                releaseConnection()
            } else {
                // No connection, no writes — no-op commit
                _state.withLock { $0.committed = true }
            }
        }
    }

    public func cancel() {
        let shouldCancel = _state.withLock { state -> Bool in
            guard !state.committed, !state.cancelled else { return false }
            state.cancelled = true
            state.writeBuffer.removeAll()
            return true
        }
        guard shouldCancel else { return }

        // Only top-level lazy transactions own the connection lifecycle.
        // Nested transactions delegate to parent. Eager transactions (from
        // withTransaction) are managed by the engine.
        // ROLLBACK must complete BEFORE releasing the connection back to
        // the pool. Otherwise another transaction could receive the
        // connection while ROLLBACK is still in-flight.
        if !isNested, let conn = currentConnection, client != nil {
            Task { [self] in
                _ = try? await conn.query("ROLLBACK", logger: self.logger)
                self.releaseConnection()
            }
        }
    }

    // MARK: - Internal (called by engine's withTransaction)

    func commitInternal(
        connection conn: PostgresConnection,
        skipCommitStatement: Bool = false
    ) async throws {
        do {
            try await flushWriteBuffer(connection: conn)
            if !skipCommitStatement {
                try await conn.query("COMMIT", logger: logger)
            }
            _state.withLock { $0.committed = true }
        } catch {
            _state.withLock { $0.cancelled = true }
            if !skipCommitStatement {
                _ = try? await conn.query("ROLLBACK", logger: logger)
            }
            throw error
        }
    }

    func rollbackInternal(connection conn: PostgresConnection) async throws {
        let shouldRollback = _state.withLock { state -> Bool in
            guard !state.committed, !state.cancelled else { return false }
            state.writeBuffer.removeAll()
            state.cancelled = true
            return true
        }
        guard shouldRollback else { return }
        _ = try? await conn.query("ROLLBACK", logger: logger)
    }

    // MARK: - Write Buffer Flush

    func flushWriteBuffer(connection conn: PostgresConnection) async throws {
        let ops = _state.withLock { state -> [WriteOp] in
            let ops = state.writeBuffer
            state.writeBuffer.removeAll()
            return ops
        }
        guard !ops.isEmpty else { return }

        for op in ops {
            switch op {
            case .set(let key, let value):
                let keyBuf = ByteBuffer(bytes: key)
                let valueBuf = ByteBuffer(bytes: value)
                try await conn.query(
                    """
                    INSERT INTO kv_store (key, value) VALUES (\(keyBuf), \(valueBuf))
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                    """,
                    logger: logger
                )

            case .clear(let key):
                let keyBuf = ByteBuffer(bytes: key)
                try await conn.query(
                    "DELETE FROM kv_store WHERE key = \(keyBuf)",
                    logger: logger
                )

            case .clearRange(let begin, let end):
                let beginBuf = ByteBuffer(bytes: begin)
                let endBuf = ByteBuffer(bytes: end)
                try await conn.query(
                    "DELETE FROM kv_store WHERE key >= \(beginBuf) AND key < \(endBuf)",
                    logger: logger
                )

            case .atomic(let key, let param, let mutationType):
                try await executeAtomicOp(
                    connection: conn, key: key, param: param,
                    mutationType: mutationType
                )
            }
        }
    }

    // MARK: - Atomic Operation Execution

    private func executeAtomicOp(
        connection conn: PostgresConnection,
        key: Bytes, param: Bytes, mutationType: MutationType
    ) async throws {
        switch mutationType {
        case .add:
            // Read-modify-write: interpret values as little-endian Int64
            // SELECT FOR UPDATE acquires row-level lock (safe within transaction)
            let keyBuf = ByteBuffer(bytes: key)
            let rows = try await conn.query(
                "SELECT value FROM kv_store WHERE key = \(keyBuf) FOR UPDATE",
                logger: logger
            )
            var currentValue: Int64 = 0
            for try await (buf,) in rows.decode(ByteBuffer.self) {
                if buf.readableBytes >= 8 {
                    var mutableBuf = buf
                    currentValue = mutableBuf.readInteger(
                        endianness: .little, as: Int64.self
                    ) ?? 0
                }
            }

            guard param.count >= 8 else {
                throw StorageError.invalidOperation(
                    "atomicOp(.add) requires param of at least 8 bytes, got \(param.count)"
                )
            }
            let addend = param.withUnsafeBytes { ptr in
                ptr.loadUnaligned(as: Int64.self)
            }

            let result = currentValue &+ addend
            var resultBuf = ByteBuffer()
            resultBuf.writeInteger(result, endianness: .little, as: Int64.self)

            try await conn.query(
                """
                INSERT INTO kv_store (key, value) VALUES (\(keyBuf), \(resultBuf))
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                logger: logger
            )

        case .setVersionstampedKey, .setVersionstampedValue:
            // Versionstamp is FDB-specific. PostgreSQL has no equivalent.
            // Throw to prevent silent data corruption in VersionIndex.
            throw StorageError.invalidOperation(
                "PostgreSQL does not support versionstamp operations. " +
                "VersionIndex requires FoundationDB backend."
            )

        case .max, .min, .bitOr, .bitAnd, .bitXor, .compareAndClear:
            // Not yet implemented for PostgreSQL backend.
            // Throw to prevent silent data corruption.
            throw StorageError.invalidOperation(
                "Atomic operation '\(mutationType)' is not implemented for PostgreSQL backend"
            )
        }
    }

    // MARK: - KeySelector Resolution (same logic as SQLiteStorageTransaction)

    /// Resolve a KeySelector used as the BEGIN of a range scan.
    ///
    /// The begin selector determines the first key included in the result.
    /// - firstGreaterOrEqual(k): key >= k
    /// - firstGreaterThan(k):    key > k
    static func resolveBeginForSQL(_ selector: KeySelector) -> (key: Bytes, op: String) {
        switch (selector.orEqual, selector.offset) {
        case (false, 1):
            return (selector.key, ">=")
        case (true, 1):
            return (selector.key, ">")
        default:
            return (selector.key, ">=")
        }
    }

    /// Resolve a KeySelector used as the END of a range scan.
    ///
    /// The end selector determines the first key PAST the result (exclusive boundary).
    /// - firstGreaterOrEqual(k): key < k
    /// - firstGreaterThan(k):    key <= k
    /// - lastLessOrEqual(k):     key <= k
    /// - lastLessThan(k):        key < k
    static func resolveEndForSQL(_ selector: KeySelector) -> (key: Bytes, op: String) {
        switch (selector.orEqual, selector.offset) {
        case (false, 1):
            return (selector.key, "<")
        case (true, 1):
            return (selector.key, "<=")
        case (true, 0):
            return (selector.key, "<=")
        case (false, 0):
            return (selector.key, "<")
        default:
            return (selector.key, "<")
        }
    }
}
