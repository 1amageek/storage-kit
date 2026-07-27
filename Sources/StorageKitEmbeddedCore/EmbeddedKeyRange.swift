import DatabaseTypes
/// Half-open key range used for embedded read/write conflict tracking.
public struct EmbeddedKeyRange: Sendable, Hashable {
    public let begin: ByteString?
    public let end: ByteString?

    public init(begin: ByteString?, end: ByteString?) {
        self.begin = begin
        self.end = end
    }

    public static func singleKey(_ key: ByteString) -> EmbeddedKeyRange {
        let end = ByteString.copying(count: key.count + 1) { destination in
            key.withUnsafeBytes { source in
                destination.copyMemory(from: source)
            }
            destination[key.count] = 0
        }
        return EmbeddedKeyRange(begin: key, end: end)
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
