/// Transaction options.
///
/// An abstraction of FDB's transaction options.
/// Backends must either implement an option completely or throw a typed
/// `StorageError.unsupportedOperation`; options are never accepted as no-ops.
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
