/// Configurable limits for Cloudflare Durable Object storage operations.
public struct CloudflareDurableObjectLimits: Sendable, Hashable, Codable {
    public let maxKeyBytes: Int
    public let maxValueBytes: Int
    public let maxMutationsPerCommit: Int
    public let maxRangeLimit: Int
    public let maxNameBytes: Int

    public init(
        maxKeyBytes: Int,
        maxValueBytes: Int,
        maxMutationsPerCommit: Int,
        maxRangeLimit: Int,
        maxNameBytes: Int
    ) {
        self.maxKeyBytes = maxKeyBytes
        self.maxValueBytes = maxValueBytes
        self.maxMutationsPerCommit = maxMutationsPerCommit
        self.maxRangeLimit = maxRangeLimit
        self.maxNameBytes = maxNameBytes
    }

    public static let `default` = CloudflareDurableObjectLimits(
        maxKeyBytes: 10_000,
        maxValueBytes: 1_000_000,
        maxMutationsPerCommit: 10_000,
        maxRangeLimit: 1_000,
        maxNameBytes: 512
    )
}
