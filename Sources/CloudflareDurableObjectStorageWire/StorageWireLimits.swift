/// Resource limits enforced by the Cloudflare storage protocol.
public struct StorageWireLimits: Sendable, Hashable {
    public let maxKeyBytes: Int
    public let maxVersionstampedKeyOperandBytes: Int
    public let maxBoundaryBytes: Int
    public let maxValueBytes: Int
    public let maxVersionstampedValueOperandBytes: Int
    public let maxMutationsPerCommit: Int
    public let maxConflictRangesPerCommit: Int
    public let maxRangeLimit: Int
    public let maxSplitPoints: Int
    public let maxFrameBytes: Int
    public let maxScopeComponentBytes: Int
    public let maxCanonicalScopeNameBytes: Int
    public let maxErrorMessageBytes: Int
    public let maxSelectorResolutionSteps: Int

    public init(
        maxKeyBytes: Int,
        maxVersionstampedKeyOperandBytes: Int,
        maxBoundaryBytes: Int,
        maxValueBytes: Int,
        maxVersionstampedValueOperandBytes: Int,
        maxMutationsPerCommit: Int,
        maxConflictRangesPerCommit: Int,
        maxRangeLimit: Int,
        maxSplitPoints: Int,
        maxFrameBytes: Int,
        maxScopeComponentBytes: Int,
        maxCanonicalScopeNameBytes: Int,
        maxErrorMessageBytes: Int,
        maxSelectorResolutionSteps: Int
    ) {
        self.maxKeyBytes = maxKeyBytes
        self.maxVersionstampedKeyOperandBytes =
            maxVersionstampedKeyOperandBytes
        self.maxBoundaryBytes = maxBoundaryBytes
        self.maxValueBytes = maxValueBytes
        self.maxVersionstampedValueOperandBytes =
            maxVersionstampedValueOperandBytes
        self.maxMutationsPerCommit = maxMutationsPerCommit
        self.maxConflictRangesPerCommit = maxConflictRangesPerCommit
        self.maxRangeLimit = maxRangeLimit
        self.maxSplitPoints = maxSplitPoints
        self.maxFrameBytes = maxFrameBytes
        self.maxScopeComponentBytes = maxScopeComponentBytes
        self.maxCanonicalScopeNameBytes = maxCanonicalScopeNameBytes
        self.maxErrorMessageBytes = maxErrorMessageBytes
        self.maxSelectorResolutionSteps = maxSelectorResolutionSteps
    }

    public static var cloudflareDurableObject: StorageWireLimits {
        StorageWireLimits(
            maxKeyBytes: 1_024,
            maxVersionstampedKeyOperandBytes: 1_028,
            maxBoundaryBytes: 1_025,
            maxValueBytes: 1_048_576,
            maxVersionstampedValueOperandBytes: 1_048_580,
            // Physical storage operations can outnumber logical database
            // mutations because one persisted model also maintains indexes.
            // The frame-size limit remains the aggregate byte bound.
            maxMutationsPerCommit: 10_000,
            maxConflictRangesPerCommit: 10_000,
            maxRangeLimit: 1_000,
            maxSplitPoints: 10_000,
            maxFrameBytes: 16 * 1_024 * 1_024,
            maxScopeComponentBytes: 512,
            maxCanonicalScopeNameBytes: 512,
            maxErrorMessageBytes: 4_096,
            maxSelectorResolutionSteps: 10_000
        )
    }
}
