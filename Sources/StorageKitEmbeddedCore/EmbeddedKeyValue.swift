/// Embedded key-value row.
public struct EmbeddedKeyValue: Sendable, Hashable {
    public let key: EmbeddedBytes
    public let value: EmbeddedBytes

    public init(key: EmbeddedBytes, value: EmbeddedBytes) {
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
