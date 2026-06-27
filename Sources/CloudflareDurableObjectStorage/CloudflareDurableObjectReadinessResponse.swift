/// Host readiness response.
public struct CloudflareDurableObjectReadinessResponse: Sendable, Hashable, Codable {
    public let schemaVersion: Int
    public let commitVersion: Int64
    public let metadataInitialized: Bool

    public init(schemaVersion: Int, commitVersion: Int64, metadataInitialized: Bool) {
        self.schemaVersion = schemaVersion
        self.commitVersion = commitVersion
        self.metadataInitialized = metadataInitialized
    }
}
