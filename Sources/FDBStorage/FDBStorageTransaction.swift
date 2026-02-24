import StorageKit
import FoundationDB

/// StorageKit.Transaction adapter for FoundationDB transactions.
///
/// Wraps FDB's `TransactionProtocol` and provides StorageKit's unified interface.
/// Use the `fdbTransaction` property to access FDB-specific features (version management, atomic ops, etc.).
public final class FDBStorageTransaction: Transaction, @unchecked Sendable {

    /// Direct access to the underlying FDB transaction.
    ///
    /// Use this when FDB-specific features are needed:
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
        // Collect all results via AsyncKVSequence (handles batching automatically)
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
