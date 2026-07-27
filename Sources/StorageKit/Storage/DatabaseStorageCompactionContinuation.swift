import DatabaseTypes
/// Opaque, backend-owned binary state for continuing a storage compaction operation.
///
/// The producing backend owns the wire format and must validate it before doing work.
/// Continuations are intentionally bytes rather than a backend-specific public model so
/// they can pass through DatabaseWire without giving higher layers storage semantics.
public struct DatabaseStorageCompactionContinuation: Sendable, Hashable {
    public let bytes: ByteString

    public init(bytes: ByteString) {
        self.bytes = bytes
    }
}
