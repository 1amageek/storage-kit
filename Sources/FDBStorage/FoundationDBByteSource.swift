import FoundationDB
import StorageKit

/// Retains a StorageKit byte value and lends its existing contiguous storage
/// directly to FoundationDB.
struct FoundationDBByteSource: FDB.ByteSource {
    let bytes: Bytes

    init(_ bytes: Bytes) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        try bytes.withUnsafeBytes(body)
    }
}
