/// Atomic mutation tags carried by the Cloudflare storage protocol.
public enum StorageWireMutationType: UInt8, Sendable, Hashable {
    case add = 1
    case setVersionstampedKey = 2
    case setVersionstampedValue = 3
    case bitOr = 4
    case bitAnd = 5
    case bitXor = 6
    case max = 7
    case min = 8
    case compareAndClear = 9
}

extension StorageWireMutationType {
    public func encode(into writer: inout StorageWireWriter) {
        writer.writeUInt8(rawValue)
    }

    public init(from reader: inout StorageWireReader) throws(StorageWireError) {
        let code = try reader.readUInt8()
        guard let value = StorageWireMutationType(rawValue: code) else {
            throw StorageWireError.unknownMutationType(code)
        }
        self = value
    }
}
