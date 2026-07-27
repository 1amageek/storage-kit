import DatabaseTypes
/// The 10-byte version assigned to a committed transaction.
public struct TransactionVersionstamp: Sendable, Hashable {
    public static let byteCount = 10

    public let bytes: ByteString

    public init(bytes: ByteString) throws(StorageError) {
        guard bytes.count == Self.byteCount else {
            throw StorageError(
                code: .backendContractViolation,
                operation: .read,
                message: "Transaction versionstamp contained \(bytes.count) bytes; expected \(Self.byteCount)"
            )
        }
        self.bytes = bytes
    }

    package init(committedVersion: Int64) throws(StorageError) {
        guard committedVersion >= 0 else {
            throw StorageError(
                code: .backendContractViolation,
                operation: .read,
                message: "Committed transaction version must be non-negative"
            )
        }
        let value = UInt64(committedVersion)
        try self.init(bytes: [
            UInt8(truncatingIfNeeded: value >> 56),
            UInt8(truncatingIfNeeded: value >> 48),
            UInt8(truncatingIfNeeded: value >> 40),
            UInt8(truncatingIfNeeded: value >> 32),
            UInt8(truncatingIfNeeded: value >> 24),
            UInt8(truncatingIfNeeded: value >> 16),
            UInt8(truncatingIfNeeded: value >> 8),
            UInt8(truncatingIfNeeded: value),
            0,
            0,
        ])
    }
}
