/// Structured metadata for a byte-limit violation at a storage boundary.
public struct StorageByteLimitViolation: Sendable, Hashable, Codable {
    public enum Resource: String, Sendable, Hashable, Codable {
        case commitRequest = "commit_request"
        case key
        case value
    }

    public enum Measurement: String, Sendable, Hashable, Codable {
        case exact
        case estimated
    }

    public let resource: Resource
    public let observedByteCount: UInt64
    public let maximumByteCount: UInt64
    public let measurement: Measurement

    public init(
        resource: Resource,
        observedByteCount: UInt64,
        maximumByteCount: UInt64,
        measurement: Measurement
    ) {
        self.resource = resource
        self.observedByteCount = observedByteCount
        self.maximumByteCount = maximumByteCount
        self.measurement = measurement
    }
}
