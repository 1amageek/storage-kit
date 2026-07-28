import DatabaseTypes
@testable import CloudflareDurableObjectStorageHostTransport
import Testing

@Suite("Storage host response tests")
struct StorageHostResponseTests {
    @Test("response is copied directly into final byte storage")
    func receivesResponse() throws {
        var copyCount = 0
        var discardCount = 0

        let response = try StorageHostResponse.receive(
            byteCount: 3,
            maximumResponseBytes: 3,
            discard: { discardCount += 1 },
            copyInto: { destination in
                copyCount += 1
                destination[0] = 0x10
                destination[1] = 0x20
                destination[2] = 0x30
            }
        )

        #expect(response == ByteString([0x10, 0x20, 0x30]))
        #expect(copyCount == 1)
        #expect(discardCount == 0)
    }

    @Test("oversized response is discarded before allocation")
    func discardsOversizedResponse() {
        var copyCount = 0
        var discardCount = 0

        #expect(
            throws: StorageHostTransportError.responseTooLarge(
                actual: 3,
                maximum: 2
            )
        ) {
            _ = try StorageHostResponse.receive(
                byteCount: 3,
                maximumResponseBytes: 2,
                discard: { discardCount += 1 },
                copyInto: { _ in copyCount += 1 }
            )
        }
        #expect(copyCount == 0)
        #expect(discardCount == 1)
    }

    @Test("missing response fails without claiming host ownership")
    func rejectsMissingResponse() {
        var copyCount = 0
        var discardCount = 0

        #expect(throws: StorageHostTransportError.hostReturnedNoResponse) {
            _ = try StorageHostResponse.receive(
                byteCount: 0,
                maximumResponseBytes: 1,
                discard: { discardCount += 1 },
                copyInto: { _ in copyCount += 1 }
            )
        }
        #expect(copyCount == 0)
        #expect(discardCount == 0)
    }
}
