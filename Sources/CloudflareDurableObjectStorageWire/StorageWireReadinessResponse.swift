
public struct StorageWireReadinessResponse: Sendable, Hashable {
    public let schemaVersion: UInt32
    public let commitVersion: Int64
    public let metadataInitialized: Bool

    public init(schemaVersion: UInt32, commitVersion: Int64, metadataInitialized: Bool) {
        self.schemaVersion = schemaVersion
        self.commitVersion = commitVersion
        self.metadataInitialized = metadataInitialized
    }

    func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        guard commitVersion >= 0 else {
            throw .invalidVersion(commitVersion)
        }
        writer.writeUInt32(schemaVersion)
        writer.writeInt64(commitVersion)
        writer.writeBool(metadataInitialized)
    }

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        self.schemaVersion = try StorageWireProtocolError.readUInt32(from: &reader)
        let version = try StorageWireProtocolError.readInt64(from: &reader)
        guard version >= 0 else {
            throw .invalidVersion(version)
        }
        self.commitVersion = version
        self.metadataInitialized = try StorageWireProtocolError.readBool(from: &reader)
    }
}
