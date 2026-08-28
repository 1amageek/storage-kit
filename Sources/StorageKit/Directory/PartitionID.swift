import DatabaseTypes

/// Nonempty opaque identifier of one Partition, compared by bytes.
public struct PartitionID: Sendable, Hashable, Comparable {
    public let bytes: ByteString

    public init(_ bytes: ByteString) throws(DirectoryAddressError) {
        guard !bytes.isEmpty else {
            throw .emptyPartitionID
        }
        guard bytes.count <= DirectoryLimits.maximumPartitionIDByteCount else {
            throw .partitionIDTooLong(byteCount: bytes.count)
        }
        self.bytes = bytes
    }

    public init(utf8 string: String) throws(DirectoryAddressError) {
        try self.init(ByteString(utf8: string))
    }

    public static func < (lhs: PartitionID, rhs: PartitionID) -> Bool {
        lhs.bytes < rhs.bytes
    }
}
