import DatabaseTypes
/// Key-value row carried by the storage protocol.
public struct StorageWireKeyValue: Sendable, Hashable {
    public let key: ByteString
    public let value: ByteString

    public init(key: ByteString, value: ByteString) {
        self.key = key
        self.value = value
    }

    public func encode(into writer: inout StorageWireWriter) throws(StorageWireError) {
        try writer.writeBytes(key)
        try writer.writeBytes(value)
    }

    public init(from reader: inout StorageWireReader) throws(StorageWireError) {
        self.key = try reader.readByteRegion()
        self.value = try reader.readByteRegion()
    }
}
