import FoundationDB
import StorageKit

/// Keeps a FoundationDB result future alive while StorageKit borrows its bytes.
struct FoundationDBResultBytesOwner: BytesOwner {
    let buffer: FDB.ByteBuffer

    init(_ buffer: FDB.ByteBuffer) {
        self.buffer = buffer
    }

    var count: Int { buffer.count }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try buffer.withUnsafeBytes(body)
    }
}
