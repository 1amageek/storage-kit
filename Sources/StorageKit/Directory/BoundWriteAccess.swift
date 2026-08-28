import DatabaseTypes

/// Read and write access bound to one leased Partition for the duration of a
/// single `PartitionLease.withWriteAccess` closure.
///
/// Reads forward to the embedded `BoundReadAccess`; writes apply the same
/// lease, scope, and key containment checks before reaching the transaction.
public struct BoundWriteAccess: ~Copyable, Sendable {
    private let reads: BoundReadAccess
    private let transaction: any TransactionAccess

    init(reads: consuming BoundReadAccess, transaction: any TransactionAccess) {
        self.reads = reads
        self.transaction = transaction
    }

    public var partition: Partition {
        reads.partition
    }

    // MARK: - Reads

    public func getValue(
        for key: ByteString,
        snapshot: Bool = false
    ) async throws -> ByteString? {
        try await reads.getValue(for: key, snapshot: snapshot)
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool = false,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        try await reads.getValue(for: key, snapshot: snapshot, maximumByteCount: maximumByteCount)
    }

    public func getKey(
        selector: KeySelector,
        snapshot: Bool = false
    ) async throws -> ByteString? {
        try await reads.getKey(selector: selector, snapshot: snapshot)
    }

    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int = 0,
        reverse: Bool = false,
        snapshot: Bool = false,
        streamingMode: StreamingMode = .iterator
    ) throws -> KeyValueCursor {
        try reads.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }

    // MARK: - Writes

    public func setValue(_ value: ByteString, for key: ByteString) throws {
        try reads.requireLive(operation: .write)
        try reads.bounds.requireKey(key, operation: .write)
        try transaction.setValue(value, for: key)
    }

    public func clear(key: ByteString) throws {
        try reads.requireLive(operation: .delete)
        try reads.bounds.requireKey(key, operation: .delete)
        try transaction.clear(key: key)
    }

    public func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        try reads.requireLive(operation: .deleteRange)
        try reads.bounds.requireRange(begin: beginKey, end: endKey, operation: .deleteRange)
        try transaction.clearRange(beginKey: beginKey, endKey: endKey)
    }

    public func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        try reads.requireLive(operation: .write)
        try reads.bounds.requireKey(key, operation: .write)
        try transaction.atomicOp(key: key, param: param, mutationType: mutationType)
    }

    public func addConflictRange(
        beginKey: ByteString,
        endKey: ByteString,
        type: ConflictRangeType
    ) throws {
        try reads.requireLive(operation: .write)
        try reads.bounds.requireRange(begin: beginKey, end: endKey, operation: .write)
        try transaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: type)
    }
}
