/// Embedded write operation using StorageKit atomic mutation semantics.
public enum EmbeddedWriteOperation: Sendable, Hashable {
    case set(key: [UInt8], value: [UInt8])
    case clear(key: [UInt8])
    case clearRange(begin: [UInt8], end: [UInt8])
    case atomic(key: [UInt8], param: [UInt8], mutationType: EmbeddedMutationType)

    public func encode(into writer: inout EmbeddedBinaryWriter) throws(EmbeddedWireError) {
        switch self {
        case .set(let key, let value):
            writer.writeUInt8(1)
            try writer.writeBytes(key)
            try writer.writeBytes(value)
        case .clear(let key):
            writer.writeUInt8(2)
            try writer.writeBytes(key)
        case .clearRange(let begin, let end):
            writer.writeUInt8(3)
            try writer.writeBytes(begin)
            try writer.writeBytes(end)
        case .atomic(let key, let param, let mutationType):
            writer.writeUInt8(4)
            try writer.writeBytes(key)
            try writer.writeBytes(param)
            mutationType.encode(into: &writer)
        }
    }

    public init(from reader: inout EmbeddedBinaryReader) throws(EmbeddedWireError) {
        let tag = try reader.readUInt8()
        switch tag {
        case 1:
            self = .set(key: try reader.readBytes(), value: try reader.readBytes())
        case 2:
            self = .clear(key: try reader.readBytes())
        case 3:
            self = .clearRange(begin: try reader.readBytes(), end: try reader.readBytes())
        case 4:
            let key = try reader.readBytes()
            let param = try reader.readBytes()
            let mutationType = try EmbeddedMutationType(from: &reader)
            self = .atomic(key: key, param: param, mutationType: mutationType)
        default:
            throw EmbeddedWireError.unknownWriteOperation(tag)
        }
    }
}
