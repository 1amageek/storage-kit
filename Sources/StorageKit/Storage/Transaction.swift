import DatabaseTypes

/// Read and mutation access to one storage transaction.
///
/// Higher-level runtimes use this capability without acquiring authority to
/// commit or cancel the owning transaction.
///
/// ## Zero-copy design
/// `rangeCursor` is the existential transaction boundary. Concrete backends
/// may additionally expose a backend-specific `TransactionRangeResult` for
/// static dispatch, while this protocol transfers key/value payloads through
/// `ByteString` views without materializing intermediate arrays.
///
/// ## Backend implementation guide
/// Required methods: `getValue`, `rangeCursor`, `setValue`, `clear`,
/// `clearRange`, and `atomicOp`.
/// Others have default implementations provided via extension (non-FDB backends are automatically covered).
/// Every production backend must own one `TransactionMutationByteMeter` per
/// transaction attempt and record accepted writes before buffering or
/// dispatching them.
///
/// `atomicOp` must be implemented by every backend. Backends that support
/// versionstamp mutations materialize them with the transaction's commit version.
public protocol TransactionAccess: TransactionReadAccess {

    /// Optional semantics implemented by this concrete backend.
    var capabilities: TransactionCapabilities { get }

    /// Physical compaction access when the concrete backend supports it.
    var compaction: StorageCompactionAccess? { get }

    // MARK: - Write

    /// Set a value for a key (overwrites existing value).
    func setValue(_ value: ByteString, for key: ByteString) throws

    /// Delete a key.
    func clear(key: ByteString) throws

    /// Delete all keys within a range.
    ///
    /// - Parameters:
    ///   - beginKey: Start key (inclusive).
    ///   - endKey: End key (exclusive).
    func clearRange(beginKey: ByteString, endKey: ByteString) throws

    // MARK: - Atomic Operations

    /// Perform an atomic operation.
    ///
    /// - Parameters:
    ///   - key: The target key.
    ///   - param: Operation parameter (operation-dependent byte array).
    ///   - mutationType: The type of mutation operation.
    func atomicOp(key: ByteString, param: ByteString, mutationType: MutationType) throws

    // MARK: - Version Management

    /// Set the read version (e.g. restoring from cache).
    func setReadVersion(_ version: Int64) throws

    /// Get the read version of this transaction.
    func getReadVersion() async throws -> Int64

    // MARK: - Transaction Options

    /// Set a transaction option (no value).
    func setOption(forOption option: TransactionOption) throws

    /// Set a transaction option (byte value).
    func setOption(to value: ByteString?, forOption option: TransactionOption) throws

    /// Set a transaction option (integer value).
    func setOption(to value: Int, forOption option: TransactionOption) throws

    // MARK: - Conflict Range

    /// Add a conflict range.
    ///
    /// - Parameters:
    ///   - beginKey: Start key (inclusive).
    ///   - endKey: End key (exclusive).
    ///   - type: read or write.
    func addConflictRange(beginKey: ByteString, endKey: ByteString, type: ConflictRangeType) throws

    // MARK: - Statistics

    /// Get the byte-size metric for a key range in the transaction view.
    /// Pending mutations are included.
    func getEstimatedRangeSizeBytes(beginKey: ByteString, endKey: ByteString) async throws -> Int

    /// Get split points for the transaction view, including pending mutations.
    func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString]

    // MARK: - Versionstamp

    /// Requests the transaction versionstamp before commit.
    ///
    /// The returned value resolves after a successful commit.
    func requestVersionstamp() -> any PendingTransactionVersionstamp
}

/// Owns the commit and cancellation lifecycle of one storage transaction.
///
/// Only the component coordinating retries and transaction completion should
/// receive this capability. Database semantics and index implementations that
/// mutate storage should depend on `TransactionAccess`; read-only index
/// execution should depend on `TransactionReadAccess`.
public protocol Transaction: TransactionAccess {
    /// The storage engine instance that owns this transaction.
    var transactionDomain: StorageTransactionDomain { get }

    /// The authoritative storage failure recorded by this transaction.
    ///
    /// Higher layers use this state after an arbitrary operation error has
    /// crossed an untyped application boundary. Backends must record every
    /// `StorageError` before it escapes a transaction operation.
    var storageFailure: StorageError? { get }

    /// The configured portable logical mutation limit, if this transaction is
    /// bounded. A non-nil value remains attached to the owned transaction when
    /// it crosses task boundaries.
    var mutationByteLimit: Int? { get }

    /// Configures mutation admission before the transaction accepts writes.
    /// The lifecycle owner calls this before exposing transaction access.
    func configureMutationByteLimit(maximumBytes: Int?) throws

    /// Commit the transaction.
    func commit() async throws

    /// Cancel the transaction and wait until backend cleanup is authoritative.
    func cancel() async throws

    /// Get the committed version after a successful commit.
    func getCommittedVersion() throws -> Int64
}

// MARK: - Convenience (default parameters)

extension TransactionAccess {

    /// Backends expose no optional semantics unless they declare them.
    public var capabilities: TransactionCapabilities { .none }

    /// Backends expose no physical compaction operation by default.
    public var compaction: StorageCompactionAccess? { nil }

    // MARK: - setOption String compatible

    /// FDB compatible: set option with a string value.
    public func setOption(to value: String, forOption option: TransactionOption) throws {
        try setOption(to: ByteString(utf8: value), forOption: option)
    }
}

// MARK: - Default Transaction Behavior

/// Default implementations for non-FDB backends.
///
/// Basic access methods must be implemented by each backend. The rest work
/// with defaults.
extension TransactionAccess {

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

    /// Default: options must be implemented explicitly by a backend.
    public func setOption(forOption option: TransactionOption) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support transaction options",
            operation: .execute
        )
    }

    /// Default: options must be implemented explicitly by a backend.
    public func setOption(to value: ByteString?, forOption option: TransactionOption) throws {
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
        beginKey: ByteString,
        endKey: ByteString,
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
        beginKey: ByteString,
        endKey: ByteString
    ) async throws -> Int {
        guard compareBytes(beginKey, endKey) <= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "Range size boundaries are not ordered"
            )
        }
        return try await StorageRangeMetrics.exactSize(
            rangeCursor(
                from: .firstGreaterOrEqual(beginKey),
                to: .firstGreaterOrEqual(endKey),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
        )
    }

    /// Default: returns ordered chunk boundaries, including `beginKey` and
    /// `endKey`, whose exact stored byte count is approximately `chunkSize`.
    public func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString] {
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
            source: rangeCursor(
                from: .firstGreaterOrEqual(beginKey),
                to: .firstGreaterOrEqual(endKey),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
        )
    }

    /// Default: versionstamp resolution fails closed for unsupported backends.
    public func requestVersionstamp() -> any PendingTransactionVersionstamp {
        TransactionVersionstampRequest {
            throw StorageError.unsupportedOperation(
                "This storage backend does not expose a versionstamp",
                operation: .read
            )
        }
    }
}

extension Transaction {
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

    /// Default: the backend does not expose a committed version.
    public func getCommittedVersion() throws -> Int64 {
        throw StorageError.unsupportedOperation(
            "This storage backend does not expose a committed version",
            operation: .read
        )
    }
}
