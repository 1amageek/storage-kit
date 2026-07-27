import CloudflareDurableObjectStorageWire
import DatabaseTypes
import Synchronization
import Testing

@Suite("Storage Wire Byte View Tests")
struct StorageWireByteOwnershipTests {
    @Test func decodedFieldRetainsItsSourceFrame() throws {
        let encoded = try StorageWire.encode(
            .read(
                StorageWireReadResponse(
                    value: [0xAA, 0xBB, 0xCC],
                    currentCommitVersion: 9
                )
            )
        )
        let releaseRecorder = WireByteReleaseRecorder()
        var frame: ByteString? = makeOwnedBytes(
            encoded.copyBytes(),
            releaseRecorder: releaseRecorder
        )
        var decoded: StorageWireResponse? = try decode(
            frame
        )

        frame = nil
        #expect(releaseRecorder.releaseCount == 0)
        do {
            guard case .read(let response) = decoded else {
                Issue.record("Expected a read response")
                return
            }
            #expect(response.value == [0xAA, 0xBB, 0xCC])
            #expect(response.currentCommitVersion == 9)
        }

        decoded = nil
        #expect(releaseRecorder.releaseCount == 1)
    }

    @Test func decoderAcceptsANonzeroIndexFrameView() throws {
        let encoded = try StorageWire.encode(
            .readiness(
                StorageWireReadinessResponse(
                    schemaVersion: 1,
                    commitVersion: 2,
                    metadataInitialized: true
                )
            )
        )
        let owner = ByteString([0xFF] + encoded.copyBytes() + [0xEE])
        let view = owner[1..<(owner.endIndex - 1)]
        let response = try StorageWire
            .decodeResponse(view)

        guard case .readiness(let readiness) = response else {
            Issue.record("Expected a readiness response")
            return
        }
        #expect(readiness.schemaVersion == 1)
        #expect(readiness.commitVersion == 2)
        #expect(readiness.metadataInitialized)
    }

    @Test func encodedFrameIsExactOwnedAndSlicesDetach() throws {
        let encoded = try StorageWire.encode(
            .readiness(
                StorageWireReadinessResponse(
                    schemaVersion: 1,
                    commitVersion: 2,
                    metadataInitialized: true
                )
            )
        )
        let first = encoded.detached()
        let second = first.detached()
        let encodedAddress = try #require(encoded.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })
        let firstAddress = try #require(first.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })
        let secondAddress = try #require(second.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })
        let slice = encoded[
            (encoded.startIndex + 1)..<encoded.endIndex
        ]
        let detachedSlice = slice.detached()
        let sliceAddress = try #require(slice.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })
        let detachedSliceAddress = try #require(detachedSlice.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })

        #expect(encodedAddress == firstAddress)
        #expect(firstAddress == secondAddress)
        #expect(sliceAddress != detachedSliceAddress)
    }

    private func decode(
        _ frame: ByteString?
    ) throws -> StorageWireResponse {
        guard let frame else {
            throw WireResponseExpectationError.missingFrame
        }
        return try StorageWire
            .decodeResponse(frame)
    }

    private func makeOwnedBytes(
        _ bytes: [UInt8],
        releaseRecorder: WireByteReleaseRecorder
    ) -> ByteString {
        ByteString(
            retaining: WireTestByteAllocation(
                bytes: bytes,
                releaseRecorder: releaseRecorder
            )
        )
    }
}

private enum WireResponseExpectationError: Error {
    case missingFrame
}

private final class WireTestByteAllocation: ByteStringOwner {
    let count: Int

    private let address: UInt
    private let releaseRecorder: WireByteReleaseRecorder

    init(
        bytes: [UInt8],
        releaseRecorder: WireByteReleaseRecorder
    ) {
        let pointer = UnsafeMutableRawPointer.allocate(
            byteCount: bytes.count,
            alignment: MemoryLayout<UInt8>.alignment
        )
        bytes.withUnsafeBytes { source in
            if let sourceAddress = source.baseAddress {
                pointer.copyMemory(
                    from: sourceAddress,
                    byteCount: source.count
                )
            }
        }
        self.address = UInt(bitPattern: pointer)
        self.count = bytes.count
        self.releaseRecorder = releaseRecorder
    }

    deinit {
        UnsafeMutableRawPointer(bitPattern: address)?.deallocate()
        releaseRecorder.recordRelease()
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try body(
            UnsafeRawBufferPointer(
                start: UnsafeRawPointer(bitPattern: address),
                count: count
            )
        )
    }
}

private final class WireByteReleaseRecorder: Sendable {
    private let state = Mutex(0)

    var releaseCount: Int {
        state.withLock { $0 }
    }

    func recordRelease() {
        state.withLock { $0 += 1 }
    }
}
