/// Abstract protocol for KV transactions.
///
/// Has API-compatible signatures with FDB's TransactionProtocol.
/// database-framework uses this protocol via `any Transaction`.
///
/// ## Zero-copy design
/// `getRange` returns an associated type `RangeResult`.
/// When using concrete types, the backend-specific AsyncSequence is returned directly without wrapping.
/// When accessed via `any Transaction`, only existential dispatch occurs (no data copying).
///
/// ## Backend implementation guide
/// Required methods: `getValue`, `getRange`, `setValue`, `clear`, `clearRange`,
/// `atomicOp`, `commit`, `cancel`.
/// Others have default implementations provided via extension (non-FDB backends are automatically covered).
/// Every production backend must own one `TransactionMutationByteMeter` per
/// transaction attempt and record accepted writes before buffering or
/// dispatching them.
///
/// `atomicOp` must be implemented by every backend. Backends that support
/// versionstamp mutations materialize them with the transaction's commit version.
public protocol Transaction: Sendable {

    // MARK: - Associated type (zero-copy getRange)

    /// Concrete type of the AsyncSequence returned by getRange.
    ///
    /// FDB: `FDB.AsyncKVSequence` (lazy batch fetching)
    /// SQLite: cursor-based AsyncSequence
    /// InMemory: array-based AsyncSequence
    associatedtype RangeResult: TransactionRangeResult

    /// Optional semantics implemented by this concrete backend.
    var capabilities: TransactionCapabilities { get }

    /// The configured portable logical mutation limit, if this transaction is
    /// bounded. A non-nil value must remain attached to the transaction when it
    /// crosses task boundaries.
    var mutationByteLimit: Int? { get }

    /// Configures mutation admission before the transaction accepts writes.
    /// Production backends must implement this with transaction-owned state.
    func configureMutationByteLimit(maximumBytes: Int?) throws

    // MARK: - Read

    /// Get the value for a key (returns nil if the key does not exist).
    ///
    /// - Parameters:
    ///   - key: The key to retrieve.
    ///   - snapshot: If true, performs a snapshot read (FDB: does not add to conflict range).
    func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes?

    /// Get the key at the position specified by a KeySelector.
    ///
    /// - Parameters:
    ///   - selector: The key selection criteria.
    ///   - snapshot: If true, performs a snapshot read.
    func getKey(selector: KeySelector, snapshot: Bool) async throws -> Bytes?

    /// Range scan (lazily evaluated).
    ///
    /// - Parameters:
    ///   - begin: The KeySelector for the start position.
    ///   - end: The KeySelector for the end position.
    ///   - limit: Maximum number of entries to fetch (0 means unlimited).
    ///   - reverse: If true, scans in reverse order.
    ///   - snapshot: If true, performs a snapshot read.
    ///   - streamingMode: Hint for batch size optimization.
    func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> RangeResult

    // MARK: - Write

    /// Set a value for a key (overwrites existing value).
    func setValue(_ value: Bytes, for key: Bytes) throws

    /// Delete a key.
    func clear(key: Bytes) throws

    /// Delete all keys within a range.
    ///
    /// - Parameters:
    ///   - beginKey: Start key (inclusive).
    ///   - endKey: End key (exclusive).
    func clearRange(beginKey: Bytes, endKey: Bytes) throws

    // MARK: - Atomic Operations

    /// Perform an atomic operation.
    ///
    /// - Parameters:
    ///   - key: The target key.
    ///   - param: Operation parameter (operation-dependent byte array).
    ///   - mutationType: The type of mutation operation.
    func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) throws

    // MARK: - Transaction Control

    /// Commit the transaction.
    func commit() async throws

    /// Cancel the transaction and wait until backend cleanup is authoritative.
    func cancel() async throws

    // MARK: - Version Management

    /// Set the read version (e.g. restoring from cache).
    func setReadVersion(_ version: Int64) throws

    /// Get the read version of this transaction.
    func getReadVersion() async throws -> Int64

    /// Get the committed version (only valid after commit).
    func getCommittedVersion() throws -> Int64

    // MARK: - Transaction Options

    /// Set a transaction option (no value).
    func setOption(forOption option: TransactionOption) throws

    /// Set a transaction option (byte value).
    func setOption(to value: Bytes?, forOption option: TransactionOption) throws

    /// Set a transaction option (integer value).
    func setOption(to value: Int, forOption option: TransactionOption) throws

    // MARK: - Conflict Range

    /// Add a conflict range.
    ///
    /// - Parameters:
    ///   - beginKey: Start key (inclusive).
    ///   - endKey: End key (exclusive).
    ///   - type: read or write.
    func addConflictRange(beginKey: Bytes, endKey: Bytes, type: ConflictRangeType) throws

    // MARK: - Statistics

    /// Get the byte-size metric for a key range in the transaction view.
    /// Pending mutations are included.
    func getEstimatedRangeSizeBytes(beginKey: Bytes, endKey: Bytes) async throws -> Int

    /// Get split points for the transaction view, including pending mutations.
    func getRangeSplitPoints(
        beginKey: Bytes,
        endKey: Bytes,
        chunkSize: Int
    ) async throws -> [Bytes]

    // MARK: - Versionstamp

    /// Get the versionstamp (only valid after commit).
    func getVersionstamp() async throws -> Bytes?
}

// MARK: - Convenience (default parameters)

extension Transaction {

    /// Backends expose no optional semantics unless they declare them.
    public var capabilities: TransactionCapabilities { .none }

    /// Custom backends fail closed when a bounded transaction is requested but
    /// they have not implemented transaction-owned mutation admission.
    public var mutationByteLimit: Int? { nil }

    public func configureMutationByteLimit(maximumBytes: Int?) throws {
        guard maximumBytes == nil else {
            throw StorageError.unsupportedOperation(
                "Transaction backend does not implement mutation byte admission",
                operation: .beginTransaction
            )
        }
    }

    /// Convenience with snapshot defaulting to false.
    ///
    /// Note: All Transaction conformers MUST implement `getValue(for:snapshot:)` directly.
    /// This extension only provides a default argument — it does NOT provide an implementation.
    /// Relying on this extension as a protocol witness would cause infinite recursion.
    public func getValue(for key: Bytes, snapshot: Bool = false) async throws -> Bytes? {
        try await getValue(for: key, snapshot: snapshot)
    }

    /// Provides default values for the KeySelector-based getRange.
    ///
    /// Adds default arguments to the protocol requirement getRange(from:to:limit:reverse:snapshot:streamingMode:).
    /// Parameters omitted at the call site are filled in here and delegated with full arguments
    /// to the actual protocol implementation (each backend).
    public func getRange(
        from begin: KeySelector, to end: KeySelector,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll
    ) -> RangeResult {
        getRange(
            from: begin, to: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        )
    }

    /// Bytes-based getRange convenience (converts to KeySelector internally).
    public func getRange(
        begin: Bytes, end: Bytes,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll
    ) -> RangeResult {
        getRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        )
    }

    // MARK: - Collecting (type-safe even via any Transaction)

    /// A collecting convenience that is type-safe even via `any Transaction`.
    ///
    /// The associated type RangeResult loses its Element type through protocol existential,
    /// but this method uses concrete self internally so the type is fully resolved.
    public func collectRange(
        from begin: KeySelector, to end: KeySelector,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll
    ) async throws -> [(Bytes, Bytes)] {
        var result: [(Bytes, Bytes)] = []
        try await forEachInRange(
            from: begin, to: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        ) { key, value in
            result.append((key, value))
        }
        return result
    }

    /// Bytes-based collectRange convenience (converts to KeySelector internally).
    public func collectRange(
        begin: Bytes, end: Bytes,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll
    ) async throws -> [(Bytes, Bytes)] {
        try await collectRange(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        )
    }

    // MARK: - ForEach (type-safe range iteration even via any Transaction)

    /// Performs type-safe range iteration even via `any Transaction`.
    ///
    /// Within a protocol extension, Self is a concrete type, so the associated type RangeResult's
    /// Element is resolved as (Bytes, Bytes).
    public func forEachInRange(
        from begin: KeySelector, to end: KeySelector,
        limit: Int = 0, reverse: Bool = false,
        snapshot: Bool = false, streamingMode: StreamingMode = .wantAll,
        body: (Bytes, Bytes) async throws -> Void
    ) async throws {
        let rows = getRange(
            from: begin, to: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        )
        try await rows.consumeRows(body)
    }

    // MARK: - setOption String compatible

    /// FDB compatible: set option with a string value.
    public func setOption(to value: String, forOption option: TransactionOption) throws {
        try setOption(to: Bytes(value.utf8), forOption: option)
    }
}

// MARK: - Default Transaction Behavior

/// Default implementations for non-FDB backends.
///
/// Basic methods (getValue, getRange, setValue, clear, clearRange, atomicOp,
/// commit, cancel) must be implemented by each backend. The rest work with defaults.
extension Transaction {

    /// Default: implements getKey with adjacent selectors, without assuming a
    /// maximum sentinel key for the backend keyspace.
    public func getKey(selector: KeySelector, snapshot: Bool = false) async throws -> Bytes? {
        let (nextOffset, overflow) = selector.offset.addingReportingOverflow(1)
        guard !overflow else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "KeySelector offset cannot be advanced"
            )
        }
        var result: Bytes?
        try await forEachInRange(
            from: selector,
            to: KeySelector(
                key: selector.key,
                orEqual: selector.orEqual,
                offset: nextOffset
            ),
            limit: 1,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .exact
        ) { key, _ in
            result = key
        }
        return result
    }

    /// Default: the backend does not expose historical read versions.
    public func setReadVersion(_ version: Int64) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support setting a read version",
            operation: .read
        )
    }

    /// Default: the backend does not expose a read version.
    public func getReadVersion() async throws -> Int64 {
        throw StorageError.unsupportedOperation(
            "This storage backend does not expose a read version",
            operation: .read
        )
    }

    /// Default: the backend does not expose a committed version.
    public func getCommittedVersion() throws -> Int64 {
        throw StorageError.unsupportedOperation(
            "This storage backend does not expose a committed version",
            operation: .read
        )
    }

    /// Default: options must be implemented explicitly by a backend.
    public func setOption(forOption option: TransactionOption) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support transaction options",
            operation: .execute
        )
    }

    /// Default: options must be implemented explicitly by a backend.
    public func setOption(to value: Bytes?, forOption option: TransactionOption) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support byte-valued transaction options",
            operation: .execute
        )
    }

    /// Default: options must be implemented explicitly by a backend.
    public func setOption(to value: Int, forOption option: TransactionOption) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support integer-valued transaction options",
            operation: .execute
        )
    }

    /// Default: conflict tracking must be implemented explicitly by a backend.
    public func addConflictRange(
        beginKey: Bytes,
        endKey: Bytes,
        type: ConflictRangeType
    ) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support explicit conflict ranges",
            operation: .write
        )
    }

    /// Default: computes the exact stored key and value byte count by scanning
    /// the transaction view. Backends may use a native implementation when no
    /// pending mutations can change the result.
    public func getEstimatedRangeSizeBytes(
        beginKey: Bytes,
        endKey: Bytes
    ) async throws -> Int {
        guard compareBytes(beginKey, endKey) <= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "Range size boundaries are not ordered"
            )
        }
        return try await StorageRangeMetrics.exactSize(
            getRange(
                begin: beginKey,
                end: endKey,
                snapshot: true,
                streamingMode: .wantAll
            )
        )
    }

    /// Default: returns ordered chunk boundaries, including `beginKey` and
    /// `endKey`, whose exact stored byte count is approximately `chunkSize`.
    public func getRangeSplitPoints(
        beginKey: Bytes,
        endKey: Bytes,
        chunkSize: Int
    ) async throws -> [Bytes] {
        guard compareBytes(beginKey, endKey) <= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "Split point boundaries are not ordered"
            )
        }
        guard chunkSize > 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "Split point chunk size must be positive"
            )
        }

        return try await StorageRangeMetrics.splitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: chunkSize,
            maximumPointCount: StorageRangeMetrics
                .defaultMaximumSplitPointCount,
            rows: getRange(
                begin: beginKey,
                end: endKey,
                snapshot: true,
                streamingMode: .wantAll
            )
        )
    }

    /// Default: versionstamps must be implemented explicitly by a backend.
    public func getVersionstamp() async throws -> Bytes? {
        throw StorageError.unsupportedOperation(
            "This storage backend does not expose a versionstamp",
            operation: .read
        )
    }
}
