import DatabaseTypes
import StorageKit
import Testing
@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object byte ordering")
struct CloudflareDurableObjectByteOrderingTests {
    @Test("long owner-backed keys are each borrowed once")
    func ownerBackedKeysBorrowOnce() {
        var left = [UInt8](repeating: 0x41, count: 16_384)
        var right = left
        left[left.count - 1] = 0x40
        right[right.count - 1] = 0x42
        let leftOwner = BorrowCountingBytesOwner(left)
        let rightOwner = BorrowCountingBytesOwner(right)

        let comparison = CloudflareDurableObjectByteOrdering.compare(
            ByteString(retaining: leftOwner),
            ByteString(retaining: rightOwner)
        )

        #expect(comparison == -1)
        #expect(leftOwner.borrowCount == 1)
        #expect(rightOwner.borrowCount == 1)
    }
}
