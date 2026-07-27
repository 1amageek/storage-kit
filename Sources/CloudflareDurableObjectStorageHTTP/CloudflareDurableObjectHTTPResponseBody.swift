import DatabaseTypes
#if !os(WASI)
import Foundation
import CloudflareDurableObjectStorageWire

struct CloudflareDurableObjectHTTPResponseBody: Sendable {
    private(set) var chunks: [Data] = []
    private(set) var byteCount = 0

    mutating func append(
        _ data: Data,
        maximumBytes: Int
    ) -> Bool {
        guard data.count <= maximumBytes,
              byteCount <= maximumBytes - data.count else {
            return false
        }
        chunks.append(data)
        byteCount += data.count
        return true
    }

    func bytes() -> ByteString {
        switch chunks.count {
        case 0:
            return []
        case 1:
            return ByteString(
                retaining: CloudflareDurableObjectHTTPResponseBytesOwner(
                    data: chunks[0]
                )
            )
        default:
            return ByteString.copying(count: byteCount) { destination in
                var offset = 0
                for chunk in chunks {
                    chunk.withUnsafeBytes { source in
                        guard source.count > 0 else {
                            return
                        }
                        destination.baseAddress!
                            .advanced(by: offset)
                            .copyMemory(
                                from: source.baseAddress!,
                                byteCount: source.count
                            )
                        offset += source.count
                    }
                }
            }
        }
    }
}
#endif
