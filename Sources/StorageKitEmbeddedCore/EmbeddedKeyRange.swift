/// Half-open key range used for embedded read/write conflict tracking.
public struct EmbeddedKeyRange: Sendable, Hashable {
    public let begin: EmbeddedBytes?
    public let end: EmbeddedBytes?

    public init(begin: EmbeddedBytes?, end: EmbeddedBytes?) {
        self.begin = begin
        self.end = end
    }

    public static func singleKey(_ key: EmbeddedBytes) -> EmbeddedKeyRange {
        EmbeddedKeyRange(begin: key, end: key.appending(0x00))
    }

    public func encode(into writer: inout EmbeddedWireWriter) throws(EmbeddedWireError) {
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

    public init(from reader: inout EmbeddedWireReader) throws(EmbeddedWireError) {
        if try reader.readBool() {
            self.begin = try reader.readByteRegion()
        } else {
            self.begin = nil
        }
        if try reader.readBool() {
            self.end = try reader.readByteRegion()
        } else {
            self.end = nil
        }
    }
}
