import CloudflareDurableObjectStorageHostTransport
import Testing

@Suite("Cloudflare Durable Object Storage Host Transport Tests")
struct CloudflareDurableObjectStorageHostTransportTests {
    @Test func dispatchesBytesThroughInjectedSynchronousHost() async throws {
        let transport = try CloudflareDurableObjectStorageHostTransport(
            dispatcher: ReversingStorageHostDispatcher(),
            maximumRequestBytes: 4,
            maximumResponseBytes: 3
        )

        let response = try await transport.send([1, 2, 3, 4])

        #expect(response == [4, 3, 2])
    }

    @Test func rejectsOversizedRequestBeforeHostDispatch() async throws {
        let transport = try CloudflareDurableObjectStorageHostTransport(
            dispatcher: ReversingStorageHostDispatcher(),
            maximumRequestBytes: 2,
            maximumResponseBytes: 2
        )

        await #expect(throws: StorageHostTransportError.self) {
            _ = try await transport.send([1, 2, 3])
        }
    }

    @Test func rejectsInvalidFrameLimitsDuringInitialization() {
        #expect(throws: StorageHostTransportError.invalidLimit) {
            _ = try CloudflareDurableObjectStorageHostTransport(
                dispatcher: ReversingStorageHostDispatcher(),
                maximumRequestBytes: 0,
                maximumResponseBytes: 1
            )
        }
        let oversized = CloudflareDurableObjectStorageHostTransport
            .defaultMaximumFrameBytes + 1
        #expect(
            throws: StorageHostTransportError
                .limitExceedsProtocolMaximum(
                    actual: oversized,
                    maximum: CloudflareDurableObjectStorageHostTransport
                        .defaultMaximumFrameBytes
                )
        ) {
            _ = try CloudflareDurableObjectStorageHostTransport(
                dispatcher: ReversingStorageHostDispatcher(),
                maximumRequestBytes: oversized,
                maximumResponseBytes: 1
            )
        }
    }
}
