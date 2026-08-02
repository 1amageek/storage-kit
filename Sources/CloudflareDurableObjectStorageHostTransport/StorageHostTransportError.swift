import CloudflareDurableObjectStorage

public enum StorageHostTransportError:
    CloudflareDurableObjectStorageTransportFailure,
    Equatable {
    case unavailable
    case invalidLimit
    case limitExceedsProtocolMaximum(actual: Int, maximum: Int)
    case requestTooLarge(actual: Int, maximum: Int)
    case requestLengthOverflow
    case responseLengthOverflow
    case hostReturnedNoResponse
    case responseTooLarge(actual: Int, maximum: Int)

    public var failureStage: CloudflareDurableObjectStorageTransportFailureStage {
        switch self {
        case .invalidLimit,
             .limitExceedsProtocolMaximum,
             .requestTooLarge,
             .requestLengthOverflow:
            return .localValidation
        case .unavailable:
            return .unavailable
        case .hostReturnedNoResponse,
             .responseLengthOverflow,
             .responseTooLarge:
            return .afterDispatch
        }
    }
}
