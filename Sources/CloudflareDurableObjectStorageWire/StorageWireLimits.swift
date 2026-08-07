/// Resource limits enforced by the Cloudflare storage protocol.
public struct StorageWireLimits: Sendable, Hashable {
    public let maxKeyBytes: Int
    public let maxVersionstampedKeyOperandBytes: Int
    public let maxBoundaryBytes: Int
    public let maxValueBytes: Int
    public let maxStoredKeyValueBytes: Int
    public let maxVersionstampedValueOperandBytes: Int
    public let maxMutationsPerCommit: Int
    public let maxConflictRangesPerCommit: Int
    public let maxRangeLimit: Int
    public let maxSplitPoints: Int
    public let maxFrameBytes: Int
    public let maxPartitionIdentityComponentBytes: Int
    public let maxCanonicalPartitionIdentityNameBytes: Int
    public let maxErrorMessageBytes: Int
    public let maxSelectorResolutionSteps: Int

    public init(
        maxKeyBytes: Int,
        maxVersionstampedKeyOperandBytes: Int,
        maxBoundaryBytes: Int,
        maxValueBytes: Int,
        maxStoredKeyValueBytes: Int,
        maxVersionstampedValueOperandBytes: Int,
        maxMutationsPerCommit: Int,
        maxConflictRangesPerCommit: Int,
        maxRangeLimit: Int,
        maxSplitPoints: Int,
        maxFrameBytes: Int,
        maxPartitionIdentityComponentBytes: Int,
        maxCanonicalPartitionIdentityNameBytes: Int,
        maxErrorMessageBytes: Int,
        maxSelectorResolutionSteps: Int
    ) {
        self.maxKeyBytes = maxKeyBytes
        self.maxVersionstampedKeyOperandBytes =
            maxVersionstampedKeyOperandBytes
        self.maxBoundaryBytes = maxBoundaryBytes
        self.maxValueBytes = maxValueBytes
        self.maxStoredKeyValueBytes = maxStoredKeyValueBytes
        self.maxVersionstampedValueOperandBytes =
            maxVersionstampedValueOperandBytes
        self.maxMutationsPerCommit = maxMutationsPerCommit
        self.maxConflictRangesPerCommit = maxConflictRangesPerCommit
        self.maxRangeLimit = maxRangeLimit
        self.maxSplitPoints = maxSplitPoints
        self.maxFrameBytes = maxFrameBytes
        self.maxPartitionIdentityComponentBytes = maxPartitionIdentityComponentBytes
        self.maxCanonicalPartitionIdentityNameBytes = maxCanonicalPartitionIdentityNameBytes
        self.maxErrorMessageBytes = maxErrorMessageBytes
        self.maxSelectorResolutionSteps = maxSelectorResolutionSteps
    }

    public static var cloudflareDurableObject: StorageWireLimits {
        StorageWireLimits(
            // The database Worker is deployed with SQLite-backed Durable
            // Objects. Cloudflare documents a 2,000,000-byte combined key and
            // value limit. Either component may consume that budget alone;
            // stored pairs are validated against the combined limit.
            maxKeyBytes: 2_000_000,
            maxVersionstampedKeyOperandBytes: 2_000_004,
            maxBoundaryBytes: 2_000_001,
            maxValueBytes: 2_000_000,
            maxStoredKeyValueBytes: 2_000_000,
            maxVersionstampedValueOperandBytes: 2_000_004,
            // Physical storage operations can outnumber logical database
            // mutations because one persisted model also maintains indexes.
            // The frame-size limit remains the aggregate byte bound.
            maxMutationsPerCommit: 10_000,
            maxConflictRangesPerCommit: 10_000,
            maxRangeLimit: 1_000,
            maxSplitPoints: 10_000,
            maxFrameBytes: 16 * 1_024 * 1_024,
            maxPartitionIdentityComponentBytes: 512,
            maxCanonicalPartitionIdentityNameBytes: 512,
            maxErrorMessageBytes: 4_096,
            maxSelectorResolutionSteps: 10_000
        )
    }
}
