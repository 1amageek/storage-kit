import DatabaseTypes
import NIOCore
import StorageKit

/// Retains PostgreSQL result bytes and lends their readable region without copying.
struct PostgreSQLResultBytesOwner: ByteStringOwner {
    let buffer: ByteBuffer

    var count: Int {
        buffer.readableBytes
    }

    /// A readable region can share a larger PostgreSQL/NIO result allocation.
    var retainedByteCount: Int? { nil }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try buffer.withUnsafeReadableBytes(body)
    }
}
