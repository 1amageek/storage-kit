
public struct StorageWireCommitResponse: Sendable, Hashable {
    public let committedVersion: Int64

    public init(committedVersion: Int64) {
        self.committedVersion = committedVersion
    }

    func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        guard committedVersion >= 0 else {
            throw .invalidVersion(committedVersion)
        }
        writer.writeInt64(committedVersion)
    }

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        let version = try StorageWireProtocolError.readInt64(from: &reader)
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.committedVersion = version
    }
}
