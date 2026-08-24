/// Reports range cleanup failure after a consumer reached or requested a
/// terminal state without an iteration failure.
public struct StorageRangeTerminalCleanupError:
    Error,
    CustomStringConvertible {
    public let cleanupError: any Error

    public init(cleanupError: any Error) {
        self.cleanupError = cleanupError
    }

    public var description: String {
        "Range iteration completed but iterator cleanup failed"
    }
}
