import DatabaseTypes
#if !os(WASI)
import Foundation
import CloudflareDurableObjectStorageWire

struct CloudflareDurableObjectHTTPResponseBytesOwner: ByteStringOwner {
    let data: Data

    var count: Int {
        data.count
    }

    /// `Data` does not expose the size of the allocation retained by a view.
    var retainedByteCount: Int? { nil }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try data.withUnsafeBytes(body)
    }
}
#endif
