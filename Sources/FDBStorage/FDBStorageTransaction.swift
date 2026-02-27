import StorageKit
import FoundationDB

/// StorageKit.Transaction adapter for FoundationDB transactions.
///
/// ## ゼロコピー設計
/// `RangeResult = FDB.AsyncKVSequence` を直接返す。
/// 中間ラッパー・クロージャ・Task なし。
///
/// ## FDB 固有機能へのアクセス
/// ```swift
/// if let fdbTx = transaction as? FDBStorageTransaction {
///     fdbTx.fdbTransaction.setReadVersion(cachedVersion)
/// }
/// ```
public final class FDBStorageTransaction: Transaction, @unchecked Sendable {

    /// ゼロコピー: FDB.AsyncKVSequence をそのまま返す
    public typealias RangeResult = FDB.AsyncKVSequence

    /// Direct access to the underlying FDB transaction.
    public let fdbTransaction: any TransactionProtocol

    init(_ fdbTransaction: any TransactionProtocol) {
        self.fdbTransaction = fdbTransaction
    }

    // MARK: - Type Conversion（struct コピーのみ、Bytes は CoW）

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
        try await fdbTransaction.getValue(for: key, snapshot: snapshot)
    }

    public func getKey(selector: KeySelector, snapshot: Bool) async throws -> Bytes? {
        try await fdbTransaction.getKey(selector: toFDB(selector), snapshot: snapshot)
    }

    /// ゼロコピー: FDB.AsyncKVSequence を直接返す
    ///
    /// Existential opening パターンで `any TransactionProtocol` の extension メソッドを呼び出す。
    /// Swift 5.7+ の暗黙 existential opening により、具象型の extension メソッドにアクセス可能。
    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> FDB.AsyncKVSequence {
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
        return impl(fdbTransaction)
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
                throw StorageError.transactionConflict
            }
        } catch let error as StorageError {
            throw error
        } catch let error as FDBError {
            throw Self.convertFDBError(error)
        } catch {
            throw StorageError.backendError("\(error)")
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
        try await fdbTransaction.getReadVersion()
    }

    public func getCommittedVersion() throws -> Int64 {
        try fdbTransaction.getCommittedVersion()
    }

    // MARK: - Transaction Options

    public func setOption(forOption option: TransactionOption) throws {
        switch option {
        case .timeout(let ms):
            // timeout requires an integer value; delegate to the Int overload
            try fdbTransaction.setOption(to: ms, forOption: .timeout)
        default:
            try fdbTransaction.setOption(forOption: toFDBOption(option))
        }
    }

    public func setOption(to value: Bytes?, forOption option: TransactionOption) throws {
        try fdbTransaction.setOption(to: value, forOption: toFDBOption(option))
    }

    public func setOption(to value: Int, forOption option: TransactionOption) throws {
        try fdbTransaction.setOption(to: value, forOption: toFDBOption(option))
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
    static func convertFDBError(_ error: FDBError) -> StorageError {
        if error.isRetryable {
            return .transactionConflict
        }
        return .backendError(error.description)
    }

    // MARK: - Conflict Range

    public func addConflictRange(beginKey: Bytes, endKey: Bytes, type: ConflictRangeType) throws {
        try fdbTransaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: toFDB(type))
    }

    // MARK: - Statistics

    public func getEstimatedRangeSizeBytes(beginKey: Bytes, endKey: Bytes) async throws -> Int {
        try await fdbTransaction.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)
    }

    public func getRangeSplitPoints(beginKey: Bytes, endKey: Bytes, chunkSize: Int) async throws -> [[UInt8]] {
        try await fdbTransaction.getRangeSplitPoints(beginKey: beginKey, endKey: endKey, chunkSize: chunkSize)
    }

    // MARK: - Versionstamp

    public func getVersionstamp() async throws -> Bytes? {
        try await fdbTransaction.getVersionstamp()
    }
}
