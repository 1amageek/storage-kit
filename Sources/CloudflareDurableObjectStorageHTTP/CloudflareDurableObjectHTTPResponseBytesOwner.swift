import DatabaseTypes
#if !os(WASI)
import Foundation
import CloudflareDurableObjectStorageWire

struct CloudflareDurableObjectHTTPResponseBytesOwner: ByteStringOwner {
    let data: Data

    var count: Int {
        data.count
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try data.withUnsafeBytes(body)
    }
}
#endif
