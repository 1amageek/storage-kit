/// Name codec failure.
public enum CloudflareDurableObjectNameCodecError: Error, Sendable, Equatable, CustomStringConvertible {
    case nameTooLong(limit: Int, actual: Int)
    case invalidBase64URL

    public var description: String {
        switch self {
        case .nameTooLong(let limit, let actual):
            return "Durable Object name is \(actual) bytes, exceeding limit \(limit)"
        case .invalidBase64URL:
            return "Invalid base64url value"
        }
    }
}
