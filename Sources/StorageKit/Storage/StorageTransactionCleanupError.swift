/// Reports both an operation failure and the failure that occurred while
/// cancelling its transaction.
///
/// Wrapping happens only when cancellation itself fails. Successful cleanup
/// preserves and rethrows the original operation error unchanged.
public struct StorageTransactionCleanupError:
    Error,
    Sendable,
    CustomStringConvertible
{
    public let operationError: any Error
    public let cancellationErrors: [any Error]

    public init(
        operationError: any Error,
        cancellationError: any Error
    ) {
        self.operationError = operationError
        self.cancellationErrors = [cancellationError]
    }

    private init(
        operationError: any Error,
        cancellationErrors: [any Error]
    ) {
        self.operationError = operationError
        self.cancellationErrors = cancellationErrors
    }

    public func addingCancellationError(
        _ error: any Error
    ) -> StorageTransactionCleanupError {
        StorageTransactionCleanupError(
            operationError: operationError,
            cancellationErrors: cancellationErrors + [error]
        )
    }

    public var description: String {
        "Transaction operation failed and cancellation also failed: "
            + "cancellationFailureCount=\(cancellationErrors.count)"
    }
}
