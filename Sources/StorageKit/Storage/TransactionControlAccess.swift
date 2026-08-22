import DatabaseTypes

/// Read-version, option, conflict, and versionstamp control for one storage
/// transaction attempt.
public protocol TransactionControlAccess: Sendable {
    func setReadVersion(_ version: Int64) throws

    func getReadVersion() async throws -> Int64

    func setOption(forOption option: TransactionOption) throws

    func setOption(
        to value: ByteString?,
        forOption option: TransactionOption
    ) throws

    func setOption(to value: Int, forOption option: TransactionOption) throws

    func addConflictRange(
        beginKey: ByteString,
        endKey: ByteString,
        type: ConflictRangeType
    ) throws

    func requestVersionstamp() -> any PendingTransactionVersionstamp
}

extension TransactionControlAccess {
    /// FoundationDB-compatible option convenience.
    public func setOption(
        to value: String,
        forOption option: TransactionOption
    ) throws {
        try setOption(to: ByteString(utf8: value), forOption: option)
    }

    public func setReadVersion(_ version: Int64) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support setting a read version",
            operation: .read
        )
    }

    public func getReadVersion() async throws -> Int64 {
        throw StorageError.unsupportedOperation(
            "This storage backend does not expose a read version",
            operation: .read
        )
    }

    public func setOption(forOption option: TransactionOption) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support transaction options",
            operation: .execute
        )
    }

    public func setOption(
        to value: ByteString?,
        forOption option: TransactionOption
    ) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support byte-valued transaction options",
            operation: .execute
        )
    }

    public func setOption(
        to value: Int,
        forOption option: TransactionOption
    ) throws {
        throw StorageError.unsupportedOperation(
            "This storage backend does not support integer-valued transaction options",
            operation: .execute
        )
    }

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

    public func requestVersionstamp() -> any PendingTransactionVersionstamp {
        TransactionVersionstampRequest {
            throw StorageError.unsupportedOperation(
                "This storage backend does not expose a versionstamp",
                operation: .read
            )
        }
    }
}
