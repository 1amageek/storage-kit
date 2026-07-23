public enum CommitRequestLimitError: Error, Sendable, Equatable, CustomStringConvertible {
    case outsideAllowedRange(value: Int, allowed: ClosedRange<Int>)

    public var description: String {
        switch self {
        case .outsideAllowedRange(let value, let allowed):
            return "FoundationDB commit request limit \(value) is outside \(allowed.lowerBound)...\(allowed.upperBound) bytes"
        }
    }
}
