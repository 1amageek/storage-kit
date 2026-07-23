#if !os(WASI)
import Foundation
import StorageKitEmbeddedCore
@testable import CloudflareDurableObjectStorageHTTP
import Testing

@Suite("Cloudflare Durable Object HTTP response ownership")
struct CloudflareDurableObjectHTTPResponseBodyTests {
    @Test("a single URLSession data chunk is retained without copying")
    func singleChunkRetainsFoundationStorage() throws {
        // Use heap-backed Data: Foundation may store tiny values inline in the
        // value itself, where copying the Data value necessarily relocates bytes.
        let data = Data(repeating: 0xa5, count: 4_096)
        var body = CloudflareDurableObjectHTTPResponseBody()

        let accepted = body.append(data, maximumBytes: data.count)
        #expect(accepted)
        let bytes = body.bytes()

        guard case .owner = bytes.sharedStorage else {
            Issue.record("Expected owner-backed response bytes")
            return
        }
        #expect(try address(of: bytes) == address(of: data))
        #expect(bytes.count == data.count)
        #expect(bytes.first == 0xa5)
    }

    @Test("multiple chunks consolidate directly into one final allocation")
    func multipleChunksUseOneFinalArray() {
        var body = CloudflareDurableObjectHTTPResponseBody()

        let acceptedFirst = body.append(Data([1, 2]), maximumBytes: 5)
        let acceptedSecond = body.append(Data([3, 4, 5]), maximumBytes: 5)
        #expect(acceptedFirst)
        #expect(acceptedSecond)
        let bytes = body.bytes()

        guard case .array(let storage, let range) = bytes.sharedStorage else {
            Issue.record("Expected one consolidated array allocation")
            return
        }
        #expect(range == storage.indices)
        #expect(bytes == [1, 2, 3, 4, 5])
    }

    @Test("the byte limit is checked before an oversized chunk is retained")
    func oversizedChunkIsNotRetained() {
        var body = CloudflareDurableObjectHTTPResponseBody()

        let accepted = body.append(Data([1, 2]), maximumBytes: 3)
        let rejected = body.append(Data([3, 4]), maximumBytes: 3)
        #expect(accepted)
        #expect(!rejected)
        #expect(body.byteCount == 2)
        #expect(body.chunks.count == 1)
        #expect(body.bytes() == [1, 2])
    }

    private func address(of bytes: EmbeddedBytes) throws -> UInt {
        try #require(bytes.withUnsafeBytes { buffer in
            buffer.baseAddress.map { UInt(bitPattern: $0) }
        })
    }

    private func address(of data: Data) throws -> UInt {
        try #require(data.withUnsafeBytes { buffer in
            buffer.baseAddress.map { UInt(bitPattern: $0) }
        })
    }
}
#endif
