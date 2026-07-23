import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedCommitResponse: Sendable, Hashable {
    public let committedVersion: Int64

    public init(committedVersion: Int64) {
        self.committedVersion = committedVersion
    }

    func encode(into writer: inout EmbeddedWireWriter) throws(CloudflareDurableObjectEmbeddedError) {
        guard committedVersion >= 0 else {
            throw .invalidVersion(committedVersion)
        }
        writer.writeInt64(committedVersion)
    }

    init(from reader: inout EmbeddedWireReader) throws(CloudflareDurableObjectEmbeddedError) {
        let version = try CloudflareDurableObjectEmbeddedError.readInt64(from: &reader)
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.committedVersion = version
    }
}
