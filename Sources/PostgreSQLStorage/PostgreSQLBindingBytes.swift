import DatabaseTypes
import NIOCore

/// Establishes the ownership boundary between immutable storage values and
/// asynchronous PostgreSQL bindings.
enum PostgreSQLBindingBytes {
    /// Copies the source bytes once into the final NIO-owned buffer.
    ///
    /// `PostgresQuery` can retain bind values after the synchronous
    /// `ByteString` borrow ends, so sharing the source pointer would violate its
    /// lifetime. `ByteBuffer(bytes:)` uses the contiguous collection fast path:
    /// one source borrow fills the independently retained destination buffer.
    static func copyToOwnedBuffer(_ bytes: ByteString) -> ByteBuffer {
        ByteBuffer(bytes: bytes)
    }
}
