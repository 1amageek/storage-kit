/// Describes whether one storage client call can suspend independently of its caller.
public enum CloudflareDurableObjectCallExecution: Sendable {
    /// The call is completed by a synchronous host dispatch before returning.
    case synchronous

    /// The call may remain suspended while external I/O is in progress.
    case suspending
}
