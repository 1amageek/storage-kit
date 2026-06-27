import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedReadResponse: Sendable, Hashable {
    public let value: [UInt8]?
    public let currentCommitVersion: Int64

    public init(value: [UInt8]?, currentCommitVersion: Int64) {
        self.value = value
        self.currentCommitVersion = currentCommitVersion
    }

    func encode(into writer: inout EmbeddedBinaryWriter) throws(CloudflareDurableObjectEmbeddedError) {
        if let value {
            writer.writeBool(true)
            try CloudflareDurableObjectEmbeddedError.writeBytes(value, into: &writer)
        } else {
            writer.writeBool(false)
        }
        writer.writeInt64(currentCommitVersion)
    }

    init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        let hasValue = try CloudflareDurableObjectEmbeddedError.readBool(from: &reader)
        if hasValue {
            self.value = try CloudflareDurableObjectEmbeddedError.readBytes(from: &reader)
        } else {
            self.value = nil
        }
        self.currentCommitVersion = try CloudflareDurableObjectEmbeddedError.readInt64(from: &reader)
    }
}
