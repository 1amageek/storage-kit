public enum CloudflareDurableObjectEmbeddedFailureStatus:
    UInt8,
    Sendable,
    Hashable {
    case transactionConflict = 1
    case invalidOperation = 2
    case backendFailure = 3
    case resourceUnavailable = 4
    case backendContractViolation = 5
}
