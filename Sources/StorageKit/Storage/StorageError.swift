/// Error type for StorageEngine.
public enum StorageError: Error, Sendable {
    /// Transaction conflict (retryable).
    case transactionConflict

    /// Transaction too old (retryable).
    case transactionTooOld

    /// Key not found.
    case keyNotFound

    /// Invalid operation.
    case invalidOperation(String)

    /// Backend-specific error.
    case backendError(String)

    /// Whether this error is retryable.
    public var isRetryable: Bool {
        switch self {
        case .transactionConflict, .transactionTooOld:
            return true
        case .keyNotFound, .invalidOperation, .backendError:
            return false
        }
    }
}
