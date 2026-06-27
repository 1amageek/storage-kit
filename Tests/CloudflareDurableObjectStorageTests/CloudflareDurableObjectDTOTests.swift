import Testing
import StorageKit
@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object DTO Tests")
struct CloudflareDurableObjectDTOTests {
    @Test func bytesUseBase64URLWithoutPadding() throws {
        let bytes = CloudflareDurableObjectBytes([0xFF, 0x00, 0x7F])

        #expect(bytes.base64url == "_wB_")
        #expect(try CloudflareDurableObjectBytes(base64url: bytes.base64url).rawValue == [0xFF, 0x00, 0x7F])
    }

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
            let code = CloudflareDurableObjectMutationTypeCode(mutationType)
            #expect(code.storageKitMutationType == mutationType)
        }
    }

    @Test func singleKeyConflictRangeUsesHalfOpenFDBStyleBounds() {
        let range = CloudflareDurableObjectConflictRange.singleKey(
            CloudflareDurableObjectBytes([0x01, 0x02])
        )

        #expect(range.begin?.rawValue == [0x01, 0x02])
        #expect(range.end?.rawValue == [0x01, 0x02, 0x00])
    }
}
