
/// Operations supported by the Cloudflare Durable Object storage protocol.
public enum StorageWireOperation: UInt8, Sendable, Hashable {
    case readiness = 1
    case read = 2
    case range = 3
    case commit = 4
    case rangeSize = 5
    case rangeSplitPoints = 6

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        let tag = try StorageWireProtocolError.readUInt8(from: &reader)
        guard let operation = StorageWireOperation(rawValue: tag) else {
            throw StorageWireProtocolError.unknownOperation(tag)
        }
        self = operation
    }

    func encode(into writer: inout StorageWireWriter) {
        writer.writeUInt8(rawValue)
    }
}
