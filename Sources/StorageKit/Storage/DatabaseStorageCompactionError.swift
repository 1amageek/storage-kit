/// Typed failures produced by the storage compaction capability.
public enum DatabaseStorageCompactionError: Error, Sendable, Hashable {
    case invalidMaximumWorkUnits(actual: UInt64, maximum: UInt64)
    case nestedTransaction
    case invalidContinuation
    case unsupportedContinuationVersion(actual: UInt8, supported: UInt8)
    case incompatibleContinuation
    case unsupportedConfiguration(feature: String, actualValue: Int64)
    case backendMadeNoProgress(remainingWorkUnits: UInt64)
    case backendFailure(description: String)
}
