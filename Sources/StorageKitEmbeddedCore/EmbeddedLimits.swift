/// Runtime limits used by Embedded StorageKit kernels.
public struct EmbeddedLimits: Sendable, Hashable {
    public let maxKeyBytes: Int
    public let maxValueBytes: Int
    public let maxMutationsPerCommit: Int
    public let maxRangeLimit: Int

    public init(
        maxKeyBytes: Int,
        maxValueBytes: Int,
        maxMutationsPerCommit: Int,
        maxRangeLimit: Int
    ) {
        self.maxKeyBytes = maxKeyBytes
        self.maxValueBytes = maxValueBytes
        self.maxMutationsPerCommit = maxMutationsPerCommit
        self.maxRangeLimit = maxRangeLimit
    }

    public static var cloudflareDefault: EmbeddedLimits {
        EmbeddedLimits(
            maxKeyBytes: 1_024,
            maxValueBytes: 1_048_576,
            maxMutationsPerCommit: 1_000,
            maxRangeLimit: 1_000
        )
    }
}
