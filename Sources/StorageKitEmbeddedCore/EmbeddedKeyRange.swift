/// Half-open key range used for embedded read/write conflict tracking.
public struct EmbeddedKeyRange: Sendable, Hashable {
    public let begin: [UInt8]?
    public let end: [UInt8]?

    public init(begin: [UInt8]?, end: [UInt8]?) {
        self.begin = begin
        self.end = end
    }

    public static func singleKey(_ key: [UInt8]) -> EmbeddedKeyRange {
        EmbeddedKeyRange(begin: key, end: key + [0x00])
    }

    public func encode(into writer: inout EmbeddedBinaryWriter) throws(EmbeddedWireError) {
        if let begin {
            writer.writeBool(true)
            try writer.writeBytes(begin)
        } else {
            writer.writeBool(false)
        }
        if let end {
            writer.writeBool(true)
            try writer.writeBytes(end)
        } else {
            writer.writeBool(false)
        }
    }

    public init(from reader: inout EmbeddedBinaryReader) throws(EmbeddedWireError) {
        if try reader.readBool() {
            self.begin = try reader.readBytes()
        } else {
            self.begin = nil
        }
        if try reader.readBool() {
            self.end = try reader.readBytes()
        } else {
            self.end = nil
        }
    }
}
