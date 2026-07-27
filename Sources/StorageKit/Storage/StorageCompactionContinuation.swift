import DatabaseTypes
/// Opaque, backend-owned binary state for continuing a storage compaction operation.
///
/// The producing backend owns the wire format and must validate it before doing work.
/// Continuations are intentionally bytes rather than a backend-specific public model so
/// callers can persist and transport them without learning backend semantics.
public struct StorageCompactionContinuation: Sendable, Hashable {
    public let bytes: ByteString

    public init(bytes: ByteString) {
        self.bytes = bytes
    }
}
