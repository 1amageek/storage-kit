public enum EmbeddedRangeBoundary: Sendable, Hashable {
    case unbounded
    case selector(EmbeddedKeySelector)

    public func encode(
        into writer: inout EmbeddedWireWriter
    ) throws(EmbeddedWireError) {
        switch self {
        case .unbounded:
            writer.writeUInt8(0)
        case .selector(let selector):
            writer.writeUInt8(1)
            try selector.encode(into: &writer)
        }
    }

    public init(
        from reader: inout EmbeddedWireReader
    ) throws(EmbeddedWireError) {
        switch try reader.readUInt8() {
        case 0:
            self = .unbounded
        case 1:
            self = .selector(try EmbeddedKeySelector(from: &reader))
        case let tag:
            throw .unknownRangeBoundary(tag)
        }
    }
}
