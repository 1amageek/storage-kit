import StorageKit
import FoundationDB

/// StorageKit.Transaction adapter for FoundationDB transactions.
///
/// ## Range iteration
/// Returns a thin wrapper around `FDB.AsyncKVSequence` so iteration errors are
/// normalized to `StorageError`.
///
/// ## Accessing FDB-specific features
/// ```swift
/// if let fdbTx = transaction as? FDBStorageTransaction {
///     fdbTx.fdbTransaction.setReadVersion(cachedVersion)
/// }
/// ```
public final class FDBStorageTransaction: Transaction, Sendable {

    public typealias RangeResult = FDBStorageRangeResult

    /// Direct access to the underlying FDB transaction.
    public let fdbTransaction: any TransactionProtocol

    init(_ fdbTransaction: any TransactionProtocol) {
        self.fdbTransaction = fdbTransaction
    }

    // MARK: - Type Conversion (struct copy only, Bytes are CoW)

    private func toFDB(_ ks: KeySelector) -> FDB.KeySelector {
        FDB.KeySelector(key: ks.key, orEqual: ks.orEqual, offset: ks.offset)
    }

    private func toFDB(_ sm: StreamingMode) -> FDB.StreamingMode {
        FDB.StreamingMode(rawValue: sm.rawValue)!
    }

    private func toFDB(_ mt: MutationType) -> FDB.MutationType {
        switch mt {
        case .add: return .add
        case .bitAnd: return .bitAnd
        case .bitOr: return .bitOr
        case .bitXor: return .bitXor
        case .max: return .max
        case .min: return .min
        case .setVersionstampedKey: return .setVersionstampedKey
        case .setVersionstampedValue: return .setVersionstampedValue
        case .compareAndClear: return .compareAndClear
        }
    }

    private func toFDB(_ ct: ConflictRangeType) -> FDB.ConflictRangeType {
        switch ct {
        case .read: return .read
        case .write: return .write
        }
    }

    // MARK: - Read

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        do {
            return try await fdbTransaction.getValue(for: key, snapshot: snapshot)
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .read)
        } catch {
            throw Self.convertBackendError(error, operation: .read)
        }
    }

    public func getKey(selector: KeySelector, snapshot: Bool) async throws -> Bytes? {
        do {
            return try await fdbTransaction.getKey(selector: toFDB(selector), snapshot: snapshot)
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .read)
        } catch {
            throw Self.convertBackendError(error, operation: .read)
        }
    }

    /// Returns a thin sequence wrapper that normalizes FDB iteration errors.
    ///
    /// Uses the existential opening pattern to call extension methods on `any TransactionProtocol`.
    /// Swift 5.7+ implicit existential opening allows access to concrete type extension methods.
    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> FDBStorageRangeResult {
        let fdbBegin = toFDB(begin)
        let fdbEnd = toFDB(end)
        let fdbMode = toFDB(streamingMode)

        func impl<T: TransactionProtocol>(_ tx: T) -> FDB.AsyncKVSequence {
            tx.getRange(
                from: fdbBegin,
                to: fdbEnd,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: fdbMode
            )
        }
        return FDBStorageRangeResult(impl(fdbTransaction))
    }

    // MARK: - Write

    public func setValue(_ value: Bytes, for key: Bytes) {
        fdbTransaction.setValue(value, for: key)
    }

    public func clear(key: Bytes) {
        fdbTransaction.clear(key: key)
    }

    public func clearRange(beginKey: Bytes, endKey: Bytes) {
        fdbTransaction.clearRange(beginKey: beginKey, endKey: endKey)
    }

    // MARK: - Atomic Operations

    public func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) {
        fdbTransaction.atomicOp(key: key, param: param, mutationType: toFDB(mutationType))
    }

    // MARK: - Transaction Control

    public func commit() async throws {
        do {
            let committed = try await fdbTransaction.commit()
            if !committed {
                throw StorageError(
                    code: .transactionConflict,
                    operation: .commit,
                    backend: .foundationDB,
                    message: "FoundationDB transaction commit reported a conflict"
                )
            }
        } catch let error as StorageError {
            throw error
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .commit)
        } catch {
            throw Self.convertBackendError(error, operation: .commit)
        }
    }

    public func cancel() {
        fdbTransaction.cancel()
    }

    // MARK: - Version Management

    public func setReadVersion(_ version: Int64) {
        fdbTransaction.setReadVersion(version)
    }

    public func getReadVersion() async throws -> Int64 {
        do {
            return try await fdbTransaction.getReadVersion()
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .read)
        } catch {
            throw Self.convertBackendError(error, operation: .read)
        }
    }

    public func getCommittedVersion() throws -> Int64 {
        do {
            return try fdbTransaction.getCommittedVersion()
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .read)
        } catch {
            throw Self.convertBackendError(error, operation: .read)
        }
    }

    // MARK: - Transaction Options

    public func setOption(forOption option: TransactionOption) throws {
        do {
            switch option {
            case .timeout(let ms):
                // timeout requires an integer value; delegate to the Int overload
                try fdbTransaction.setOption(to: ms, forOption: .timeout)
            default:
                try fdbTransaction.setOption(forOption: toFDBOption(option))
            }
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .execute)
        } catch {
            throw Self.convertBackendError(error, operation: .execute)
        }
    }

    public func setOption(to value: Bytes?, forOption option: TransactionOption) throws {
        do {
            try fdbTransaction.setOption(to: value, forOption: toFDBOption(option))
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .execute)
        } catch {
            throw Self.convertBackendError(error, operation: .execute)
        }
    }

    public func setOption(to value: Int, forOption option: TransactionOption) throws {
        do {
            try fdbTransaction.setOption(to: value, forOption: toFDBOption(option))
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .execute)
        } catch {
            throw Self.convertBackendError(error, operation: .execute)
        }
    }

    private func toFDBOption(_ option: TransactionOption) -> FDB.TransactionOption {
        switch option {
        case .timeout: return .timeout
        case .priorityBatch: return .priorityBatch
        case .prioritySystemImmediate: return .prioritySystemImmediate
        case .readPriorityLow: return .readPriorityLow
        case .readPriorityHigh: return .readPriorityHigh
        case .accessSystemKeys: return .accessSystemKeys
        case .readServerSideCacheDisable: return .readServerSideCacheDisable
        }
    }

    // MARK: - Error Conversion

    /// Convert FDBError to the appropriate StorageError at the boundary.
    static func convertFDBError(_ error: FDBError, operation: StorageOperation = .unknown) -> StorageError {
        if error.isRetryable {
            return StorageError(
                code: .transactionConflict,
                operation: operation,
                backend: .foundationDB,
                message: "FoundationDB retryable transaction error",
                underlyingDescription: error.description
            )
        }
        return StorageError(
            code: .backendFailure,
            operation: operation,
            backend: .foundationDB,
            message: "FoundationDB backend error",
            underlyingDescription: error.description
        )
    }

    static func convertBackendError(_ error: any Error, operation: StorageOperation) -> StorageError {
        if let storageError = error as? StorageError {
            return storageError
        }
        return StorageError(
            code: .backendFailure,
            operation: operation,
            backend: .foundationDB,
            message: "FoundationDB backend error",
            underlyingDescription: String(describing: error)
        )
    }

    // MARK: - Conflict Range

    public func addConflictRange(beginKey: Bytes, endKey: Bytes, type: ConflictRangeType) throws {
        do {
            try fdbTransaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: toFDB(type))
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .execute)
        } catch {
            throw Self.convertBackendError(error, operation: .execute)
        }
    }

    // MARK: - Statistics

    public func getEstimatedRangeSizeBytes(beginKey: Bytes, endKey: Bytes) async throws -> Int {
        do {
            return try await fdbTransaction.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .rangeRead)
        } catch {
            throw Self.convertBackendError(error, operation: .rangeRead)
        }
    }

    public func getRangeSplitPoints(beginKey: Bytes, endKey: Bytes, chunkSize: Int) async throws -> [[UInt8]] {
        do {
            return try await fdbTransaction.getRangeSplitPoints(beginKey: beginKey, endKey: endKey, chunkSize: chunkSize)
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .rangeRead)
        } catch {
            throw Self.convertBackendError(error, operation: .rangeRead)
        }
    }

    // MARK: - Versionstamp

    public func getVersionstamp() async throws -> Bytes? {
        do {
            return try await fdbTransaction.getVersionstamp()
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .commit)
        } catch {
            throw Self.convertBackendError(error, operation: .commit)
        }
    }
}
