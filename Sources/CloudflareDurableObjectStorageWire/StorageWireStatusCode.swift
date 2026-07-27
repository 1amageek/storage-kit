/// Fixed status codes for storage host responses.
public enum StorageWireStatusCode: UInt8, Sendable, Hashable {
    case ok = 0
    case transactionConflict = 1
    case invalidOperation = 2
    case backendFailure = 3
    case resourceUnavailable = 4
    case backendContractViolation = 5
}
