public enum CloudflareDurableObjectNameError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    case nameTooLong(limit: Int, actual: Int)

    public var description: String {
        switch self {
        case .nameTooLong(let limit, let actual):
            return "Durable Object name is \(actual) bytes, exceeding limit \(limit)"
        }
    }
}
