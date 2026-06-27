import StorageKitEmbeddedCore

public struct CloudflareDurableObjectEmbeddedReadinessRequest: Sendable, Hashable {
    public let scope: CloudflareDurableObjectEmbeddedScope

    public init(scope: CloudflareDurableObjectEmbeddedScope) {
        self.scope = scope
    }

    func encode(into writer: inout EmbeddedBinaryWriter) throws(CloudflareDurableObjectEmbeddedError) {
        try scope.encode(into: &writer)
    }

    init(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        self.scope = try CloudflareDurableObjectEmbeddedScope(from: &reader)
    }
}
