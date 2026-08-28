/// Typed failure raised by `DirectoryConformanceCase` when an adapter breaks
/// the shared `DirectoryAccess` contract.
public struct DirectoryConformanceFailure: Error, Sendable, CustomStringConvertible {
    public let step: String
    public let message: String

    public init(step: String, message: String) {
        self.step = step
        self.message = message
    }

    public var description: String {
        "\(step): \(message)"
    }
}
