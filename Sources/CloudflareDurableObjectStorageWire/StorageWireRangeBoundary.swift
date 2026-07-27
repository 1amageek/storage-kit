public enum StorageWireRangeBoundary: Sendable, Hashable {
    case unbounded
    case selector(StorageWireKeySelector)

    public func encode(
        into writer: inout StorageWireWriter
    ) throws(StorageWireError) {
        switch self {
        case .unbounded:
            writer.writeUInt8(0)
        case .selector(let selector):
            writer.writeUInt8(1)
            try selector.encode(into: &writer)
        }
    }

    public init(
        from reader: inout StorageWireReader
    ) throws(StorageWireError) {
        switch try reader.readUInt8() {
        case 0:
            self = .unbounded
        case 1:
            self = .selector(try StorageWireKeySelector(from: &reader))
        case let tag:
            throw .unknownRangeBoundary(tag)
        }
    }
}
