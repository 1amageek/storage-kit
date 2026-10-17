/// Failures of checkpoint admission, identity, and bounded journal work.
public enum TransactionCheckpointError: Error, Sendable, Equatable {
    public enum Resource: Sendable, Equatable {
        case entries
        case depth
        case retainedBytes
        case workUnits
    }

    case invalidLimits
    case configurationAfterAdmission
    case foreignToken
    case invalidToken
    case busy
    case closed
    case barrierInsideOperation
    case limitExceeded(resource: Resource, actual: Int, maximum: Int)
}
