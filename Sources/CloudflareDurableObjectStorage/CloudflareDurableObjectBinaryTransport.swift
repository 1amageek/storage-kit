/// Binary request transport for a Durable Object storage endpoint.
public protocol CloudflareDurableObjectBinaryTransport: Sendable {
    func send(_ requestBytes: [UInt8]) async throws -> [UInt8]
}
