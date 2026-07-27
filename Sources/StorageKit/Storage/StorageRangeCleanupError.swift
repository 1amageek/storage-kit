/// Reports both an iteration failure and a subsequent range cleanup failure.
///
/// Successful cleanup always preserves and rethrows the original iteration
/// error unchanged.
public struct StorageRangeCleanupError: Error, CustomStringConvertible {
    public let iterationError: any Error
    public let cleanupError: any Error

    public init(
        iterationError: any Error,
        cleanupError: any Error
    ) {
        self.iterationError = iterationError
        self.cleanupError = cleanupError
    }

    public var description: String {
        "Range iteration failed and iterator cleanup also failed"
    }
}
