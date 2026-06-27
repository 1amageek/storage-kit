/// Converts a logical database scope into a Durable Object name.
public protocol CloudflareDurableObjectNameCodec: Sendable {
    var version: String { get }

    func name(for scope: CloudflareDurableObjectStorageScope) throws -> String
}
