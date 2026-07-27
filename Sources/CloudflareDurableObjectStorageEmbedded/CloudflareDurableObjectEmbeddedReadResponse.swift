import DatabaseTypes
import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedReadResponse: Sendable, Hashable {
    public let value: ByteString?
    public let currentCommitVersion: Int64

    public init(value: ByteString?, currentCommitVersion: Int64) {
        self.value = value
        self.currentCommitVersion = currentCommitVersion
    }

    func encode(into writer: inout EmbeddedWireWriter) throws(CloudflareDurableObjectEmbeddedError) {
        if let value {
            writer.writeBool(true)
            try CloudflareDurableObjectEmbeddedError.writeBytes(
                value,
                maximum: EmbeddedLimits.cloudflareDurableObject.maxValueBytes,
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

    init(from reader: inout EmbeddedWireReader) throws(CloudflareDurableObjectEmbeddedError) {
        let hasValue = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        if hasValue {
            self.value = try CloudflareDurableObjectEmbeddedError.readBytes(
                from: &reader,
                maximum: EmbeddedLimits.cloudflareDurableObject.maxValueBytes
            )
        } else {
            self.value = nil
        }
        let version = try CloudflareDurableObjectEmbeddedError.readInt64(from: &reader)
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.currentCommitVersion = version
    }
}
