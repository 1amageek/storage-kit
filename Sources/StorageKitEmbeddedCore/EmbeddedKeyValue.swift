import DatabaseTypes
/// Embedded key-value row.
public struct EmbeddedKeyValue: Sendable, Hashable {
    public let key: ByteString
    public let value: ByteString

    public init(key: ByteString, value: ByteString) {
        self.key = key
        self.value = value
    }

    public func encode(into writer: inout EmbeddedWireWriter) throws(EmbeddedWireError) {
        try writer.writeBytes(key)
        try writer.writeBytes(value)
    }

    public init(from reader: inout EmbeddedWireReader) throws(EmbeddedWireError) {
        self.key = try reader.readByteRegion()
        self.value = try reader.readByteRegion()
    }
}
