/// Streaming mode for range scans.
///
/// Identical semantics to FDB's StreamingMode.
/// Used by backends as a hint for batch size optimization.
public enum StreamingMode: Int32, Sendable {
    /// Transfer all results at once (for small ranges).
    case wantAll = -2
    /// Default: balanced streaming.
    case iterator = -1
    /// Fetch only the specified number of rows (used with limit).
    case exact = 0
    /// Small batch.
    case small = 1
    /// Medium batch.
    case medium = 2
    /// Large batch.
    case large = 3
    /// Extra-large batch (for high throughput).
    case serial = 4
}
