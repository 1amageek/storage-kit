/// Fixed status codes for Embedded host responses.
public enum CloudflareDurableObjectEmbeddedStatusCode: UInt8, Sendable, Hashable {
    case ok = 0
    case transactionConflict = 1
    case invalidOperation = 2
    case backendFailure = 3
    case resourceUnavailable = 4
}
