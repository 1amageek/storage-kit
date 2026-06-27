/// Diagnostic value produced by a scope-to-name codec.
public struct CloudflareDurableObjectRoutedName: Sendable, Hashable, Codable {
    public let scope: CloudflareDurableObjectStorageScope
    public let name: String
    public let canonicalName: String
    public let codecVersion: String

    public init(
        scope: CloudflareDurableObjectStorageScope,
        name: String,
        canonicalName: String,
        codecVersion: String
    ) {
        self.scope = scope
        self.name = name
        self.canonicalName = canonicalName
        self.codecVersion = codecVersion
    }
}
