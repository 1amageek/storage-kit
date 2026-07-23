import StorageKitEmbeddedCore

/// Exact stored-byte count response for one committed key range.
public struct CloudflareDurableObjectEmbeddedRangeSizeResponse: Sendable, Hashable {
    public let byteCount: Int64
    public let currentCommitVersion: Int64

    public init(byteCount: Int64, currentCommitVersion: Int64) {
        self.byteCount = byteCount
        self.currentCommitVersion = currentCommitVersion
    }

    func encode(
        into writer: inout EmbeddedWireWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        guard byteCount >= 0 else {
            throw .wire(.invalidRangeByteCount(byteCount))
        }
        guard currentCommitVersion >= 0 else {
            throw .invalidVersion(currentCommitVersion)
        }
        writer.writeInt64(byteCount)
        writer.writeInt64(currentCommitVersion)
    }

    init(
        from reader: inout EmbeddedWireReader
    ) throws(CloudflareDurableObjectEmbeddedError) {
        let byteCount = try CloudflareDurableObjectEmbeddedError.readInt64(
            from: &reader
        )
        guard byteCount >= 0 else {
            throw .wire(.invalidRangeByteCount(byteCount))
        }
        let version = try CloudflareDurableObjectEmbeddedError.readInt64(
            from: &reader
        )
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.byteCount = byteCount
        self.currentCommitVersion = version
    }
}
