/// Declares optional behavior exposed by a concrete transaction backend.
///
/// Required `Transaction` operations are not represented here. A capability
/// must only be `true` when the corresponding operation has complete backend
/// semantics; accepting a call without enforcing it is not support.
public struct TransactionCapabilities: Sendable, Hashable {
    public let transactionTimeout: Bool
    public let schedulingPriority: Bool
    public let readPriority: Bool
    public let readCacheControl: Bool
    public let systemKeyAccess: Bool
    public let historicalReadVersion: Bool
    public let readVersion: Bool
    public let committedVersion: Bool
    public let explicitConflictRanges: Bool
    public let committedVersionstamp: Bool
    public let versionstampedMutations: Bool

    public init(
        transactionTimeout: Bool = false,
        schedulingPriority: Bool = false,
        readPriority: Bool = false,
        readCacheControl: Bool = false,
        systemKeyAccess: Bool = false,
        historicalReadVersion: Bool = false,
        readVersion: Bool = false,
        committedVersion: Bool = false,
        explicitConflictRanges: Bool = false,
        committedVersionstamp: Bool = false,
        versionstampedMutations: Bool = false
    ) {
        self.transactionTimeout = transactionTimeout
        self.schedulingPriority = schedulingPriority
        self.readPriority = readPriority
        self.readCacheControl = readCacheControl
        self.systemKeyAccess = systemKeyAccess
        self.historicalReadVersion = historicalReadVersion
        self.readVersion = readVersion
        self.committedVersion = committedVersion
        self.explicitConflictRanges = explicitConflictRanges
        self.committedVersionstamp = committedVersionstamp
        self.versionstampedMutations = versionstampedMutations
    }

    public static let none = TransactionCapabilities()
}
