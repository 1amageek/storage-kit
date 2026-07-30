/// Failures produced by a storage monotonic clock.
public enum StorageClockError: Error, Sendable, Equatable {
    case cancelled
    case unavailable
    case capacityExceeded(maximumWaitCount: Int)

    public var storageFailureDescription: String {
        switch self {
        case .cancelled:
            return "Storage clock wait was cancelled"
        case .unavailable:
            return "Storage clock is unavailable"
        case .capacityExceeded(let maximumWaitCount):
            return "Storage clock wait capacity exceeded: maximum=\(maximumWaitCount)"
        }
    }
}
