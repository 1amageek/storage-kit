#if !os(WASI)
import Foundation
import StorageKitEmbeddedCore

struct CloudflareDurableObjectHTTPResponseBytesOwner: EmbeddedByteOwner {
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
