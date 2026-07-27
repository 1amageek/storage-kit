import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit
import Testing

@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object Storage Value Semantics")
struct CloudflareDurableObjectStorageValueSemanticsTests {
    @Test func mutationTypeCodeRoundTripsStorageKitMutationTypes() {
        let mutationTypes: [MutationType] = [
            .add,
            .bitOr,
            .bitAnd,
            .bitXor,
            .max,
            .min,
            .compareAndClear,
            .setVersionstampedKey,
            .setVersionstampedValue,
        ]

        for mutationType in mutationTypes {
            let code = StorageWireMutationType(mutationType)
            #expect(code.mutationType == mutationType)
        }
    }

    @Test func singleKeyConflictRangeUsesHalfOpenFDBStyleBounds() {
        let range = StorageWireKeyRange.singleKey(
            ByteString([0x01, 0x02])
        )

        #expect(range.begin == ByteString([0x01, 0x02]))
        #expect(range.end == ByteString([0x01, 0x02, 0x00]))
    }
}
