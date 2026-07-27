import DatabaseTypes

public struct StorageWireReadResponse: Sendable, Hashable {
    public let value: ByteString?
    public let currentCommitVersion: Int64

    public init(value: ByteString?, currentCommitVersion: Int64) {
        self.value = value
        self.currentCommitVersion = currentCommitVersion
    }

    func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        if let value {
            writer.writeBool(true)
            try StorageWireProtocolError.writeBytes(
                value,
                maximum: StorageWireLimits.cloudflareDurableObject.maxValueBytes,
                into: &writer
            )
        } else {
            writer.writeBool(false)
        }
        guard currentCommitVersion >= 0 else {
            throw .invalidVersion(currentCommitVersion)
        }
        writer.writeInt64(currentCommitVersion)
    }

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        let hasValue = try StorageWireProtocolError.readBool(from: &reader)
        if hasValue {
            self.value = try StorageWireProtocolError.readBytes(
                from: &reader,
                maximum: StorageWireLimits.cloudflareDurableObject.maxValueBytes
            )
        } else {
            self.value = nil
        }
        let version = try StorageWireProtocolError.readInt64(from: &reader)
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.currentCommitVersion = version
    }
}
