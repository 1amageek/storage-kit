import StorageKit
import FoundationDB

/// FoundationDB トランザクションの StorageKit.Transaction アダプタ
///
/// FDB の TransactionProtocol をラップし、StorageKit の統一インターフェースを提供する。
/// `fdbTransaction` プロパティで FDB 固有機能（version 管理, atomic ops 等）に直接アクセス可能。
public final class FDBStorageTransaction: Transaction, @unchecked Sendable {

    /// FDB トランザクションへの直接アクセス
    ///
    /// database-framework 等が FDB 固有機能を使う場合に利用:
    /// ```swift
    /// if let fdbTx = transaction as? FDBStorageTransaction {
    ///     fdbTx.fdbTransaction.setReadVersion(cachedVersion)
    /// }
    /// ```
    public let fdbTransaction: any TransactionProtocol

    init(_ fdbTransaction: any TransactionProtocol) {
        self.fdbTransaction = fdbTransaction
    }

    // MARK: - Read

    public func getValue(for key: Bytes) async throws -> Bytes? {
        try await fdbTransaction.getValue(for: key, snapshot: false)
    }

    public func getRange(
        begin: Bytes,
        end: Bytes,
        limit: Int,
        reverse: Bool
    ) async throws -> KeyValueSequence {
        // AsyncKVSequence で全結果を取得（バッチング自動処理）
        var records: [(key: Bytes, value: Bytes)] = []
        let sequence = fdbTransaction.getRange(
            beginKey: begin, endKey: end, snapshot: false
        )
        for try await (key, value) in sequence {
            records.append((key: key, value: value))
        }

        if reverse {
            records.reverse()
        }

        if limit > 0 {
            records = Array(records.prefix(limit))
        }

        return KeyValueSequence(records)
    }

    // MARK: - Write

    public func setValue(_ value: Bytes, for key: Bytes) {
        fdbTransaction.setValue(value, for: key)
    }

    public func clear(key: Bytes) {
        fdbTransaction.clear(key: key)
    }

    public func clearRange(begin: Bytes, end: Bytes) {
        fdbTransaction.clearRange(beginKey: begin, endKey: end)
    }

    // MARK: - Transaction Management

    public func commit() async throws {
        let committed = try await fdbTransaction.commit()
        if !committed {
            throw StorageError.transactionConflict
        }
    }

    public func cancel() {
        fdbTransaction.cancel()
    }
}
