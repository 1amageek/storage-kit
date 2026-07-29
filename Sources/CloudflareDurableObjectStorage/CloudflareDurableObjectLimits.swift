import CloudflareDurableObjectStorageWire

/// Configurable limits for Cloudflare Durable Object storage operations.
public struct CloudflareDurableObjectLimits: Sendable, Hashable {
    public let maxKeyBytes: Int
    public let maxBoundaryBytes: Int
    public let maxValueBytes: Int
    public let maxMutationsPerCommit: Int
    public let maxConflictRangesPerCommit: Int
    public let maxRangeLimit: Int
    public let maxSplitPoints: Int
    public let maxSelectorResolutionSteps: Int

    public init(
        maxKeyBytes: Int,
        maxBoundaryBytes: Int,
        maxValueBytes: Int,
        maxMutationsPerCommit: Int,
        maxConflictRangesPerCommit: Int,
        maxRangeLimit: Int,
        maxSplitPoints: Int,
        maxSelectorResolutionSteps: Int = 10_000
    ) throws {
        try Self.validate(
            maxKeyBytes,
            field: "maxKeyBytes",
            maximum: 1_024
        )
        try Self.validate(
            maxBoundaryBytes,
            field: "maxBoundaryBytes",
            maximum: 1_025
        )
        try Self.validate(
            maxValueBytes,
            field: "maxValueBytes",
            maximum: 1_048_576
        )
        try Self.validate(
            maxMutationsPerCommit,
            field: "maxMutationsPerCommit",
            maximum: StorageWireLimits.cloudflareDurableObject
                .maxMutationsPerCommit
        )
        try Self.validate(
            maxConflictRangesPerCommit,
            field: "maxConflictRangesPerCommit",
            maximum: 1_000
        )
        try Self.validate(
            maxRangeLimit,
            field: "maxRangeLimit",
            maximum: 1_000
        )
        try Self.validate(
            maxSplitPoints,
            field: "maxSplitPoints",
            maximum: 10_000
        )
        try Self.validate(
            maxSelectorResolutionSteps,
            field: "maxSelectorResolutionSteps",
            maximum: 10_000
        )
        guard maxBoundaryBytes > maxKeyBytes else {
            throw
                CloudflareDurableObjectLimitsError
                .boundaryCannotRepresentKeySuccessor(
                    keyBytes: maxKeyBytes,
                    boundaryBytes: maxBoundaryBytes
                )
        }
        self.maxKeyBytes = maxKeyBytes
        self.maxBoundaryBytes = maxBoundaryBytes
        self.maxValueBytes = maxValueBytes
        self.maxMutationsPerCommit = maxMutationsPerCommit
        self.maxConflictRangesPerCommit = maxConflictRangesPerCommit
        self.maxRangeLimit = maxRangeLimit
        self.maxSplitPoints = maxSplitPoints
        self.maxSelectorResolutionSteps = maxSelectorResolutionSteps
    }

    public static let `default` = CloudflareDurableObjectLimits(
        maxKeyBytes: 1_024,
        maxBoundaryBytes: 1_025,
        maxValueBytes: 1_048_576,
        maxMutationsPerCommit: StorageWireLimits.cloudflareDurableObject
            .maxMutationsPerCommit,
        maxConflictRangesPerCommit: 1_000,
        maxRangeLimit: 1_000,
        maxSplitPoints: 10_000,
        maxSelectorResolutionSteps: 10_000,
        validated: ()
    )

    private init(
        maxKeyBytes: Int,
        maxBoundaryBytes: Int,
        maxValueBytes: Int,
        maxMutationsPerCommit: Int,
        maxConflictRangesPerCommit: Int,
        maxRangeLimit: Int,
        maxSplitPoints: Int,
        maxSelectorResolutionSteps: Int,
        validated: Void
    ) {
        _ = validated
        self.maxKeyBytes = maxKeyBytes
        self.maxBoundaryBytes = maxBoundaryBytes
        self.maxValueBytes = maxValueBytes
        self.maxMutationsPerCommit = maxMutationsPerCommit
        self.maxConflictRangesPerCommit = maxConflictRangesPerCommit
        self.maxRangeLimit = maxRangeLimit
        self.maxSplitPoints = maxSplitPoints
        self.maxSelectorResolutionSteps = maxSelectorResolutionSteps
    }

    private static func validate(
        _ value: Int,
        field: String,
        maximum: Int
    ) throws {
        guard value > 0 else {
            throw CloudflareDurableObjectLimitsError.nonPositive(
                field: field,
                value: value
            )
        }
        guard value <= maximum else {
            throw CloudflareDurableObjectLimitsError.exceedsProtocolMaximum(
                field: field,
                value: value,
                maximum: maximum
            )
        }
    }
}
