/// Length-prefixed byte container used by Embedded Swift codecs.
public struct EmbeddedBytes: Sendable, Hashable {
    public let rawValue: [UInt8]

    public init(_ rawValue: [UInt8]) {
        self.rawValue = rawValue
    }

    public func encode(into writer: inout EmbeddedBinaryWriter) throws(EmbeddedWireError) {
        try writer.writeBytes(rawValue)
    }

    public init(from reader: inout EmbeddedBinaryReader) throws(EmbeddedWireError) {
        self.rawValue = try reader.readBytes()
    }
}
