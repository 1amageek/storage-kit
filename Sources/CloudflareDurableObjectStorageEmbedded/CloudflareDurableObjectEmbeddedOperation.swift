import StorageKitEmbeddedCore

/// Operations supported by the Cloudflare Durable Object Embedded protocol.
public enum CloudflareDurableObjectEmbeddedOperation: UInt8, Sendable, Hashable {
    case readiness = 1
    case read = 2
    case range = 3
    case commit = 4

    init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        let tag = try CloudflareDurableObjectEmbeddedError.readUInt8(from: &reader)
        guard let operation = CloudflareDurableObjectEmbeddedOperation(rawValue: tag) else {
            throw CloudflareDurableObjectEmbeddedError.unknownOperation(tag)
        }
        self = operation
    }

    func encode(into writer: inout EmbeddedBinaryWriter) {
        writer.writeUInt8(rawValue)
    }
}
