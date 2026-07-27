import DatabaseTypes
/// Write operation carried by the Cloudflare storage protocol.
public enum StorageWireWriteOperation: Sendable, Hashable {
    case set(key: ByteString, value: ByteString)
    case clear(key: ByteString)
    case clearRange(begin: ByteString, end: ByteString)
    case atomic(
        key: ByteString,
        param: ByteString,
        mutationType: StorageWireMutationType
    )

    public func encode(into writer: inout StorageWireWriter) throws(StorageWireError) {
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

    public init(from reader: inout StorageWireReader) throws(StorageWireError) {
        let tag = try reader.readUInt8()
        switch tag {
        case 1:
            self = .set(
                key: try reader.readByteRegion(),
                value: try reader.readByteRegion()
            )
        case 2:
            self = .clear(key: try reader.readByteRegion())
        case 3:
            self = .clearRange(
                begin: try reader.readByteRegion(),
                end: try reader.readByteRegion()
            )
        case 4:
            let key = try reader.readByteRegion()
            let param = try reader.readByteRegion()
            let mutationType = try StorageWireMutationType(from: &reader)
            self = .atomic(key: key, param: param, mutationType: mutationType)
        default:
            throw StorageWireError.unknownWriteOperation(tag)
        }
    }
}
