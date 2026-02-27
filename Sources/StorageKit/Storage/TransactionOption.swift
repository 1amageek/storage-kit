/// Transaction options.
///
/// An abstraction of FDB's transaction options.
/// Ignored by default in non-FDB backends.
public enum TransactionOption: Sendable {
    /// Transaction timeout (in milliseconds).
    case timeout(milliseconds: Int)
    /// Batch priority (for background processing).
    case priorityBatch
    /// System immediate priority (for metadata operations).
    case prioritySystemImmediate
    /// Low read priority.
    case readPriorityLow
    /// High read priority.
    case readPriorityHigh
    /// Allow access to system keys.
    case accessSystemKeys
    /// Disable server-side cache.
    case readServerSideCacheDisable
}
