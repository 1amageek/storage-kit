import DatabaseTypes

/// Persistent mutation access to one storage transaction.
public protocol TransactionMutationAccess: Sendable {
    var compaction: StorageCompactionAccess? { get }

    func setValue(_ value: ByteString, for key: ByteString) throws

    func clear(key: ByteString) throws

    func clearRange(beginKey: ByteString, endKey: ByteString) throws

    func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws
}

extension TransactionMutationAccess {
    /// Backends expose no physical compaction operation by default.
    public var compaction: StorageCompactionAccess? { nil }
}
