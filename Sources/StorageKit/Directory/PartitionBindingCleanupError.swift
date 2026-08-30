/// Reports the failures that closing a Partition binding produced, together
/// with the operation failure when the bound closure also failed.
///
/// Closing a binding is authoritative: it completes the backend cleanup of
/// every cursor the binding issued. That cleanup can fail on its own, so a
/// binding whose closure succeeded still fails when cleanup did, and the
/// closure's value is not returned. Only failures the close itself caused
/// appear here; a cursor the closure drove to a terminal state already
/// reported its failure to that caller.
public struct PartitionBindingCleanupError:
    Error,
    Sendable,
    CustomStringConvertible
{
    /// The failure of the bound closure, when it failed.
    public let operationError: (any Error)?

    /// The cleanup failures raised while closing the binding, in close order.
    public let cursorCleanupErrors: [any Error]

    package init(
        operationError: (any Error)?,
        cursorCleanupErrors: [any Error]
    ) {
        self.operationError = operationError
        self.cursorCleanupErrors = cursorCleanupErrors
    }

    public var description: String {
        let subject = operationError == nil
            ? "Partition binding cleanup failed"
            : "Partition binding operation failed and cleanup also failed"
        return subject + ": cursorCleanupFailureCount=\(cursorCleanupErrors.count)"
    }
}
