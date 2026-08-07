public enum CloudflareDurableObjectLimitsError:
    Error,
    Sendable,
    Equatable {
    case nonPositive(field: String, value: Int)
    case exceedsProtocolMaximum(
        field: String,
        value: Int,
        maximum: Int
    )
    case boundaryCannotRepresentKeySuccessor(
        keyBytes: Int,
        boundaryBytes: Int
    )
    case componentExceedsStoredPairLimit(
        field: String,
        value: Int,
        maximum: Int
    )
}
