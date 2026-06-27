/// Validation failure for a Cloudflare Durable Object storage scope.
public enum CloudflareDurableObjectScopeValidationError: Error, Sendable, Equatable, CustomStringConvertible {
    case blankComponent(String)
    case controlCharacter(component: String)

    public var description: String {
        switch self {
        case .blankComponent(let component):
            return "Scope component '\(component)' must not be blank"
        case .controlCharacter(let component):
            return "Scope component '\(component)' must not contain control characters"
        }
    }
}
