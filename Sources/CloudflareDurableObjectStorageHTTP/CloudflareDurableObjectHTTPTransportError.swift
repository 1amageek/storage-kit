import CloudflareDurableObjectStorage

public enum CloudflareDurableObjectHTTPTransportError:
    CloudflareDurableObjectStorageTransportFailure,
    Equatable {
    case invalidLimit
    case limitExceedsProtocolMaximum(actual: Int, maximum: Int)
    case reservedHeader(name: String)
    case requestTooLarge(actual: Int, maximum: Int)
    case responseTooLarge(actual: Int64, maximum: Int)
    case unexpectedResponseMediaType(actual: String?)
    case missingResponse

    public var failureStage: CloudflareDurableObjectStorageTransportFailureStage {
        switch self {
        case .invalidLimit,
             .limitExceedsProtocolMaximum,
             .reservedHeader,
             .requestTooLarge:
            return .localValidation
        case .responseTooLarge,
             .unexpectedResponseMediaType,
             .missingResponse:
            return .afterDispatch
        }
    }
}
