/// Embedded key-value row.
public struct EmbeddedKeyValue: Sendable, Hashable {
    public let key: [UInt8]
    public let value: [UInt8]

    public init(key: [UInt8], value: [UInt8]) {
        self.key = key
        self.value = value
    }

    public func encode(into writer: inout EmbeddedBinaryWriter) throws(EmbeddedWireError) {
        try writer.writeBytes(key)
        try writer.writeBytes(value)
    }

    public init(from reader: inout EmbeddedBinaryReader) throws(EmbeddedWireError) {
        self.key = try reader.readBytes()
        self.value = try reader.readBytes()
    }
}
