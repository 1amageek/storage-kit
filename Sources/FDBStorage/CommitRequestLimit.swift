/// The configured FoundationDB commit-request footprint limit.
///
/// FoundationDB counts mutations, cleared ranges, and read/write conflict
/// ranges in this value. The same value is installed as the native transaction
/// `sizeLimit` and used by the pre-dispatch estimated-footprint gate.
public struct CommitRequestLimit: Sendable, Hashable {
    public static let allowedByteCountRange = 32...10_000_000
    public static let `default` = CommitRequestLimit(validatedMaximumByteCount: 10_000_000)

    public let maximumByteCount: Int

    public init(
        maximumByteCount: Int
    ) throws(CommitRequestLimitError) {
        guard Self.allowedByteCountRange.contains(maximumByteCount) else {
            throw .outsideAllowedRange(
                value: maximumByteCount,
                allowed: Self.allowedByteCountRange
            )
        }
        self.maximumByteCount = maximumByteCount
    }

    private init(validatedMaximumByteCount: Int) {
        self.maximumByteCount = validatedMaximumByteCount
    }
}
