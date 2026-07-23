import CloudflareDurableObjectStorageEmbedded
import StorageKitEmbeddedCore
import Synchronization
import Testing

@Suite("Embedded Bytes Ownership Tests")
struct EmbeddedBytesOwnershipTests {
    @Test func externalOwnerIsBorrowedWithoutMaterializing() throws {
        let owner = ArrayBackedEmbeddedByteOwner(
            bytes: [0x10, 0x20, 0x30, 0x40]
        )
        let bytes = EmbeddedBytes(retaining: owner)
        let slice = bytes.slice(1..<3)
        let ownerAddress = try #require(
            EmbeddedBytes(retaining: owner).withUnsafeBytes { buffer in
                buffer.baseAddress.map { UInt(bitPattern: $0) }
            }
        )
        let bytesAddress = try #require(bytes.withUnsafeBytes { buffer in
            buffer.baseAddress.map { UInt(bitPattern: $0) }
        })
        let sliceAddress = try #require(slice.withUnsafeBytes { buffer in
            buffer.baseAddress.map { UInt(bitPattern: $0) }
        })
        let contiguousAddress = try #require(
            bytes.withContiguousStorageIfAvailable { buffer in
                buffer.baseAddress.map { UInt(bitPattern: $0) }
            } ?? nil
        )
        let contiguousSliceAddress = try #require(
            slice.withContiguousStorageIfAvailable { buffer in
                buffer.baseAddress.map { UInt(bitPattern: $0) }
            } ?? nil
        )

        #expect(bytesAddress == ownerAddress)
        #expect(sliceAddress == ownerAddress + 1)
        #expect(contiguousAddress == ownerAddress)
        #expect(contiguousSliceAddress == ownerAddress + 1)
        #expect(slice.contiguousArray() == [0x20, 0x30])
    }

    @Test func viewsFromTheSameOwnerPermitNestedComparisonBorrows() {
        let owner = NestedBorrowEmbeddedByteOwner(
            bytes: [0x10, 0x20, 0x30, 0x40]
        )
        let bytes = EmbeddedBytes(retaining: owner)
        let lhs = bytes.slice(1..<3)
        let rhs = bytes.slice(1..<3)

        #expect(lhs == rhs)
        #expect(owner.maximumActiveBorrowCount == 2)
    }

    @Test func slicesKeepAllocationAliveUntilLastOwnerReleases() {
        let releaseRecorder = EmbeddedBytesReleaseRecorder()
        var bytes: EmbeddedBytes? = makeOwnedBytes(
            [0x10, 0x20, 0x30, 0x40],
            releaseRecorder: releaseRecorder
        )
        var firstSlice = bytes?.slice(1..<3)
        var secondSlice = bytes?.slice(2..<4)

        bytes = nil
        #expect(releaseRecorder.releaseCount == 0)
        #expect(firstSlice?.contiguousArray() == [0x20, 0x30])
        #expect(secondSlice?.contiguousArray() == [0x30, 0x40])

        firstSlice = nil
        #expect(releaseRecorder.releaseCount == 0)
        secondSlice = nil
        #expect(releaseRecorder.releaseCount == 1)
    }

    @Test func decodedFieldOwnsBorrowedFrameAfterInputReleases() throws {
        let encoded = try CloudflareDurableObjectStorageWireCodec.encode(
            .read(
                CloudflareDurableObjectEmbeddedReadResponse(
                    value: [0xAA, 0xBB, 0xCC],
                    currentCommitVersion: 9
                )
            )
        )
        let releaseRecorder = EmbeddedBytesReleaseRecorder()
        var frame: EmbeddedBytes? = makeOwnedBytes(
            encoded.contiguousArray(),
            releaseRecorder: releaseRecorder
        )
        var decoded: CloudflareDurableObjectEmbeddedResponse? = try decode(
            frame
        )

        frame = nil
        #expect(releaseRecorder.releaseCount == 0)
        do {
            guard case .read(let response) = decoded else {
                Issue.record("Expected a read response")
                return
            }
            #expect(response.value?.contiguousArray() == [0xAA, 0xBB, 0xCC])
            #expect(response.currentCommitVersion == 9)
        }

        decoded = nil
        #expect(releaseRecorder.releaseCount == 1)
    }

    @Test func detachedFullRangeReleasesItsOriginalAllocation() throws {
        let releaseRecorder = EmbeddedBytesReleaseRecorder()
        var frame: EmbeddedBytes? = makeOwnedBytes(
            [0x10, 0x20, 0x30, 0x40],
            releaseRecorder: releaseRecorder
        )
        var detached = frame?.detached()
        let frameAddress = try #require(
            frame?.withUnsafeBytes { buffer in
                buffer.baseAddress.map { UInt(bitPattern: $0) }
            }
        )
        let detachedAddress = try #require(
            detached?.withUnsafeBytes { buffer in
                buffer.baseAddress.map { UInt(bitPattern: $0) }
            }
        )

        #expect(frameAddress != detachedAddress)
        frame = nil
        #expect(releaseRecorder.releaseCount == 1)
        #expect(detached?.contiguousArray() == [0x10, 0x20, 0x30, 0x40])

        detached = nil
        #expect(releaseRecorder.releaseCount == 1)
    }

    @Test func detachedEmptySliceReleasesItsOriginalAllocation() {
        let releaseRecorder = EmbeddedBytesReleaseRecorder()
        var frame: EmbeddedBytes? = makeOwnedBytes(
            [0x10, 0x20, 0x30, 0x40],
            releaseRecorder: releaseRecorder
        )
        var emptySlice = frame?.slice(2..<2)
        let detached = emptySlice?.detached()

        frame = nil
        #expect(releaseRecorder.releaseCount == 0)
        emptySlice = nil
        #expect(releaseRecorder.releaseCount == 1)
        #expect(detached?.isEmpty == true)
    }

    @Test func detachingExactOwnedStorageIsIdempotent() throws {
        let exact = EmbeddedBytes.copying(count: 4) { output in
            output.copyBytes(from: [0x10, 0x20, 0x30, 0x40])
        }
        let first = exact.detached()
        let second = first.detached()
        let exactAddress = try #require(exact.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })
        let firstAddress = try #require(first.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })
        let secondAddress = try #require(second.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })

        #expect(exactAddress == firstAddress)
        #expect(firstAddress == secondAddress)
    }

    @Test func takingExactArrayOwnershipDoesNotCopyOrRedetach() throws {
        let array: [UInt8] = [0x10, 0x20, 0x30, 0x40]
        let arrayAddress = try #require(array.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })
        let bytes = EmbeddedBytes(owningExact: array)
        let detached = bytes.detached()
        let bytesAddress = try #require(bytes.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })
        let detachedAddress = try #require(detached.withUnsafeBytes {
            $0.baseAddress.map { UInt(bitPattern: $0) }
        })

        #expect(arrayAddress == bytesAddress)
        #expect(bytesAddress == detachedAddress)
    }

    @Test func encodedFrameIsExactOwnedAndSlicesStillDetach() throws {
        let encoded = try CloudflareDurableObjectStorageWireCodec.encode(
            .readiness(
                CloudflareDurableObjectEmbeddedReadinessResponse(
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
        let slice = encoded.slice(1..<encoded.count)
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
        _ frame: EmbeddedBytes?
    ) throws -> CloudflareDurableObjectEmbeddedResponse {
        guard let frame else {
            throw EmbeddedResponseExpectationError.missingFrame
        }
        return try CloudflareDurableObjectStorageWireCodec.decodeResponse(frame)
    }

    private func makeOwnedBytes(
        _ bytes: [UInt8],
        releaseRecorder: EmbeddedBytesReleaseRecorder
    ) -> EmbeddedBytes {
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
        let allocation = EmbeddedByteAllocation(
            unsafeAddress: UInt(bitPattern: pointer),
            count: bytes.count,
            deallocator: { address, _ in
                if let pointer = UnsafeMutableRawPointer(bitPattern: address) {
                    pointer.deallocate()
                }
                releaseRecorder.recordRelease()
            }
        )
        return EmbeddedBytes(allocation: allocation)
    }

    private enum EmbeddedResponseExpectationError: Error {
        case missingFrame
    }

    private final class EmbeddedBytesReleaseRecorder: Sendable {
        private let state = Mutex(0)

        var releaseCount: Int {
            state.withLock { $0 }
        }

        func recordRelease() {
            state.withLock { $0 += 1 }
        }
    }
}

private struct ArrayBackedEmbeddedByteOwner: EmbeddedByteOwner {
    let bytes: [UInt8]

    var count: Int {
        bytes.count
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}

private final class NestedBorrowEmbeddedByteOwner: EmbeddedByteOwner {
    let bytes: [UInt8]
    private let state = Mutex((active: 0, maximum: 0))

    init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int {
        bytes.count
    }

    var maximumActiveBorrowCount: Int {
        state.withLock { $0.maximum }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        state.withLock { state in
            state.active += 1
            state.maximum = Swift.max(state.maximum, state.active)
        }
        defer {
            state.withLock { $0.active -= 1 }
        }
        try bytes.withUnsafeBytes(body)
    }
}
