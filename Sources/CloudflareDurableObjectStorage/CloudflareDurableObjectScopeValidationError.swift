/// Validation failure for a Cloudflare Durable Object storage scope.
public enum CloudflareDurableObjectScopeValidationError: Error, Sendable, Equatable, CustomStringConvertible {
    case blankComponent(String)
    case controlCharacter(component: String)
    case componentTooLong(
        component: String,
        actual: Int,
        maximum: Int
    )
    case canonicalNameTooLong(actual: Int, maximum: Int)

    public var description: String {
        switch self {
        case .blankComponent(let component):
            return "Scope component '\(component)' must not be blank"
        case .controlCharacter(let component):
            return "Scope component '\(component)' must not contain control characters"
        case .componentTooLong(let component, let actual, let maximum):
            return "Scope component '\(component)' is \(actual) bytes, exceeding \(maximum)"
        case .canonicalNameTooLong(let actual, let maximum):
            return "Canonical scope name is \(actual) bytes, exceeding \(maximum)"
        }
    }
}
