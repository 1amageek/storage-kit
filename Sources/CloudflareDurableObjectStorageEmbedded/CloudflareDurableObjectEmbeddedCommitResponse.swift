import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedCommitResponse: Sendable, Hashable {
    public let committedVersion: Int64

    public init(committedVersion: Int64) {
        self.committedVersion = committedVersion
    }

    func encode(into writer: inout EmbeddedBinaryWriter) {
        writer.writeInt64(committedVersion)
    }

    init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        self.committedVersion = try CloudflareDurableObjectEmbeddedError.readInt64(from: &reader)
    }
}
