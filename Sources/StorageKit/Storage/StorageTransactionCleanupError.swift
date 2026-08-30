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

    /// Restates the same cleanup failure with the operation error a backend
    /// recovered from its own transport wrapper.
    ///
    /// A backend that wraps a non-`StorageError` operation failure so its
    /// connection scope cannot remap it must be able to unwrap that failure
    /// again after cleanup also failed, without discarding the cancellation
    /// failures the owner recorded.
    public func replacingOperationError(
        _ error: any Error
    ) -> StorageTransactionCleanupError {
        StorageTransactionCleanupError(
            operationError: error,
            cancellationErrors: cancellationErrors
        )
    }

    public var description: String {
        "Transaction operation failed and cancellation also failed: "
            + "cancellationFailureCount=\(cancellationErrors.count)"
    }
}
