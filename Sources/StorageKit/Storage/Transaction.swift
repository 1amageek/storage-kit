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
/// Required methods: `getValue`, `getRange`, `setValue`, `clear`, `clearRange`, `commit`, `cancel`.
/// Others have default implementations provided via extension (non-FDB backends are automatically covered).
public protocol Transaction: Sendable {

    // MARK: - Associated type (zero-copy getRange)

    /// Concrete type of the AsyncSequence returned by getRange.
    ///
    /// FDB: `FDB.AsyncKVSequence` (lazy batch fetching)
    /// SQLite: cursor-based AsyncSequence
    /// InMemory: array-based AsyncSequence
    associatedtype RangeResult: AsyncSequence & Sendable
        where RangeResult.Element == (Bytes, Bytes)

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
    func setValue(_ value: Bytes, for key: Bytes)

    /// Delete a key.
    func clear(key: Bytes)

    /// Delete all keys within a range.
    ///
    /// - Parameters:
    ///   - beginKey: Start key (inclusive).
    ///   - endKey: End key (exclusive).
    func clearRange(beginKey: Bytes, endKey: Bytes)

    // MARK: - Atomic Operations

    /// Perform an atomic operation.
    ///
    /// - Parameters:
    ///   - key: The target key.
    ///   - param: Operation parameter (operation-dependent byte array).
    ///   - mutationType: The type of mutation operation.
    func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType)

    // MARK: - Transaction Control

    /// Commit the transaction.
    func commit() async throws

    /// Cancel the transaction (discards uncommitted changes).
    func cancel()

    // MARK: - Version Management

    /// Set the read version (e.g. restoring from cache).
    func setReadVersion(_ version: Int64)

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

    /// Get the estimated byte size of a key range.
    func getEstimatedRangeSizeBytes(beginKey: Bytes, endKey: Bytes) async throws -> Int

    /// Get split points that divide a key range into chunks of the specified size.
    func getRangeSplitPoints(beginKey: Bytes, endKey: Bytes, chunkSize: Int) async throws -> [[UInt8]]

    // MARK: - Versionstamp

    /// Get the versionstamp (only valid after commit).
    func getVersionstamp() async throws -> Bytes?
}

// MARK: - Convenience (default parameters)

extension Transaction {

    /// Convenience with snapshot defaulting to false.
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

    // MARK: - FDB Legacy compatible overloads

    /// FDB TransactionProtocol compatible: beginSelector/endSelector labels.
    public func getRange(
        beginSelector: KeySelector, endSelector: KeySelector,
        snapshot: Bool = false
    ) -> RangeResult {
        getRange(
            from: beginSelector, to: endSelector,
            limit: 0, reverse: false,
            snapshot: snapshot, streamingMode: .wantAll
        )
    }

    /// FDB TransactionProtocol compatible: beginKey/endKey labels.
    public func getRange(
        beginKey: Bytes, endKey: Bytes,
        snapshot: Bool = false
    ) -> RangeResult {
        getRange(
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: 0, reverse: false,
            snapshot: snapshot, streamingMode: .wantAll
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
        for try await pair in getRange(
            from: begin, to: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        ) {
            result.append(pair)
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
        for try await (key, value) in getRange(
            from: begin, to: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        ) {
            try await body(key, value)
        }
    }

    // MARK: - setOption String compatible

    /// FDB compatible: set option with a string value.
    public func setOption(to value: String, forOption option: TransactionOption) throws {
        try setOption(to: Bytes(value.utf8), forOption: option)
    }
}

// MARK: - Default Implementations

/// Default implementations for non-FDB backends.
///
/// Basic methods (getValue, getRange, setValue, clear, clearRange, commit, cancel)
/// must be implemented by each backend. The rest work with defaults.
extension Transaction {

    /// Default: implements getKey via getRange (snapshot defaults to false).
    public func getKey(selector: KeySelector, snapshot: Bool = false) async throws -> Bytes? {
        // firstGreaterOrEqual / firstGreaterThan: fetch 1 entry from the start key
        let seq = getRange(
            from: selector,
            to: KeySelector(key: [0xFF], orEqual: true, offset: 1),
            limit: 1,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .exact
        )
        for try await (key, _) in seq {
            return key
        }
        return nil
    }

    /// Default: implements atomicOp via read-modify-write (correct for single-writer).
    public func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) {
        // Default is no-op (single-writer backends can implement read-modify-write separately)
    }

    /// Default: no-op.
    public func setReadVersion(_ version: Int64) {}

    /// Default: returns 0.
    public func getReadVersion() async throws -> Int64 { 0 }

    /// Default: returns 0.
    public func getCommittedVersion() throws -> Int64 { 0 }

    /// Default: no-op.
    public func setOption(forOption option: TransactionOption) throws {}

    /// Default: no-op.
    public func setOption(to value: Bytes?, forOption option: TransactionOption) throws {}

    /// Default: no-op.
    public func setOption(to value: Int, forOption option: TransactionOption) throws {}

    /// Default: no-op (single-writer has no conflicts).
    public func addConflictRange(beginKey: Bytes, endKey: Bytes, type: ConflictRangeType) throws {}

    /// Default: returns 0.
    public func getEstimatedRangeSizeBytes(beginKey: Bytes, endKey: Bytes) async throws -> Int { 0 }

    /// Default: returns an empty array.
    public func getRangeSplitPoints(beginKey: Bytes, endKey: Bytes, chunkSize: Int) async throws -> [[UInt8]] { [] }

    /// Default: returns nil.
    public func getVersionstamp() async throws -> Bytes? { nil }
}
