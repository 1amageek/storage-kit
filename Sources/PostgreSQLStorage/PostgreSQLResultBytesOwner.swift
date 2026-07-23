import NIOCore
import StorageKit

/// Retains PostgreSQL result bytes and lends their readable region without copying.
struct PostgreSQLResultBytesOwner: BytesOwner {
    let buffer: ByteBuffer

    var count: Int {
        buffer.readableBytes
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try buffer.withUnsafeReadableBytes(body)
    }
}
