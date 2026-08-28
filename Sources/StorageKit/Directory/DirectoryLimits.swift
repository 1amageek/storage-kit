/// Operational bounds owned by the Directory component.
///
/// The values are a correctness contract rather than tuning knobs: they keep
/// catalog node keys below every backend's key bound and bound the cost of
/// address walks and lease validation. Changing a value requires re-running
/// the shared conformance fixture on every adapter (see `DESIGN.md`).
public enum DirectoryLimits {
    /// Maximum UTF-8 byte count of one Directory name component.
    public static let maximumComponentByteCount = 255

    /// Maximum number of address steps below the root.
    public static let maximumDepth = 64

    /// Maximum byte count of one `PartitionID`.
    public static let maximumPartitionIDByteCount = 1024

    /// Maximum page size accepted by the listing operations.
    public static let maximumListLimit = 1000
}
