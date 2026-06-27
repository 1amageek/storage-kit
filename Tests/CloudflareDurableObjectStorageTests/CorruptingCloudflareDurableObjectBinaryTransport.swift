import CloudflareDurableObjectStorage

final class CorruptingCloudflareDurableObjectBinaryTransport: CloudflareDurableObjectBinaryTransport, Sendable {
    func send(_ requestBytes: [UInt8]) async throws -> [UInt8] {
        [0x01]
    }
}
