import DatabaseTypes
@testable import CloudflareDurableObjectStorageHostTransport
import StorageKitEmbeddedCore
import Synchronization
import Testing

@Suite("Storage Host Response Frame Ownership Tests")
struct StorageHostResponseFrameOwnershipTests {
    @Test func successfulResponseRetainsOneSharedFrame() throws {
        let releaseRecorder = StorageFrameReleaseRecorder()
        let address = makeFrame(payload: [0x10, 0x20, 0x30])
        var response: ByteString? = try adopt(
            address: address,
            maximumResponseBytes: 3,
            releaseRecorder: releaseRecorder
        )
        let payloadAddress = try #require(
            response?.withUnsafeBytes { bytes in
                bytes.baseAddress.map { UInt(bitPattern: $0) }
            }
        )

        #expect(payloadAddress == address + 4)
        #expect(response?.copyBytes() == [0x10, 0x20, 0x30])
        #expect(releaseRecorder.releases == [])

        response = nil
        #expect(releaseRecorder.releases == [7])
    }

    @Test func oversizedResponseFailsAndReleasesItsExactFrame() {
        let releaseRecorder = StorageFrameReleaseRecorder()
        let address = makeFrame(payload: [0x10, 0x20, 0x30])

        #expect(
            throws: StorageHostTransportError.responseTooLarge(
                actual: 3,
                maximum: 2
            )
        ) {
            _ = try adopt(
                address: address,
                maximumResponseBytes: 2,
                releaseRecorder: releaseRecorder
            )
        }
        #expect(releaseRecorder.releases == [7])
    }

    @Test func emptyResponseRetainsAndReleasesItsHeaderFrame() throws {
        let releaseRecorder = StorageFrameReleaseRecorder()
        let address = makeFrame(payload: [])
        var response: ByteString? = try adopt(
            address: address,
            maximumResponseBytes: 1,
            releaseRecorder: releaseRecorder
        )

        #expect(response?.isEmpty == true)
        #expect(releaseRecorder.releases == [])

        response = nil
        #expect(releaseRecorder.releases == [4])
    }

    private func adopt(
        address: UInt,
        maximumResponseBytes: Int,
        releaseRecorder: StorageFrameReleaseRecorder
    ) throws -> ByteString {
        try StorageHostResponseFrame.adopt(
            unsafeAddress: address,
            maximumResponseBytes: maximumResponseBytes,
            deallocator: { releasedAddress, count in
                guard let pointer = UnsafeMutableRawPointer(
                    bitPattern: releasedAddress
                ) else {
                    preconditionFailure("Test frame has an invalid address")
                }
                pointer.deallocate()
                releaseRecorder.record(count: count)
            }
        )
    }

    private func makeFrame(payload: [UInt8]) -> UInt {
        let byteCount = payload.count + 4
        let frame = UnsafeMutableRawPointer.allocate(
            byteCount: byteCount,
            alignment: MemoryLayout<UInt8>.alignment
        )
        let bytes = frame.bindMemory(to: UInt8.self, capacity: byteCount)
        let payloadCount = UInt32(payload.count)
        bytes[0] = UInt8(truncatingIfNeeded: payloadCount)
        bytes[1] = UInt8(truncatingIfNeeded: payloadCount >> 8)
        bytes[2] = UInt8(truncatingIfNeeded: payloadCount >> 16)
        bytes[3] = UInt8(truncatingIfNeeded: payloadCount >> 24)
        payload.withUnsafeBytes { source in
            guard source.count > 0, let sourceAddress = source.baseAddress else {
                return
            }
            frame.advanced(by: 4).copyMemory(
                from: sourceAddress,
                byteCount: source.count
            )
        }
        return UInt(bitPattern: frame)
    }
}

private final class StorageFrameReleaseRecorder: Sendable {
    private let counts = Mutex<[Int]>([])

    var releases: [Int] {
        counts.withLock { $0 }
    }

    func record(count: Int) {
        counts.withLock { $0.append(count) }
    }
}
