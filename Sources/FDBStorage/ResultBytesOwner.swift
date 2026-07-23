import FoundationDB
import StorageKit

/// Keeps a FoundationDB result future alive while StorageKit borrows its bytes.
struct ResultBytesOwner: BytesOwner {
    let bytes: FDB.ByteString

    init(_ bytes: FDB.ByteString) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}
