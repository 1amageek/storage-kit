import StorageKit
import StorageKitEmbeddedCore
import Synchronization
import Testing

@Suite("Storage Bytes Ownership Tests")
struct BytesOwnershipTests {
    @Test func exactCopySharesArrayStorageAtArrayOnlyBoundaries() throws {
        let bytes = Bytes.copying(count: 4) { output in
            output[0] = 0x10
            output[1] = 0x20
            output[2] = 0x30
            output[3] = 0x40
        }
        let array = bytes.contiguousArray()

        let bytesAddress = try #require(bytes.withUnsafeBytes { buffer in
            buffer.baseAddress.map { UInt(bitPattern: $0) }
        })
        let arrayAddress = try #require(array.withUnsafeBytes { buffer in
            buffer.baseAddress.map { UInt(bitPattern: $0) }
        })

        #expect(bytesAddress == arrayAddress)
        #expect(array == [0x10, 0x20, 0x30, 0x40])
    }

    @Test func externalOwnerIsBorrowedWithoutMaterializing() throws {
        let owner = ArrayBackedBytesOwner(
            bytes: [0x10, 0x20, 0x30, 0x40]
        )
        let bytes = Bytes(retaining: owner)
        let slice = bytes[1..<3]
        let ownerAddress = try #require(
            Bytes(retaining: owner).withUnsafeBytes { buffer in
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
        #expect(slice == [0x20, 0x30])
    }

    @Test func tupleCursorReturnsCanonicalBytesInsideTheSourceKey() throws {
        let key = Tuple(Bytes([0x11, 0x22, 0x33, 0x44])).pack()
        var cursor = TupleCursor(bytes: key)
        let payload = try cursor.requireBytes()
        let keyRange = try #require(key.withUnsafeBytes { buffer in
            buffer.baseAddress.map { address in
                let start = UInt(bitPattern: address)
                return start..<(start + UInt(buffer.count))
            }
        })
        let payloadRange = try #require(payload.withUnsafeBytes { buffer in
            buffer.baseAddress.map { address in
                let start = UInt(bitPattern: address)
                return start..<(start + UInt(buffer.count))
            }
        })

        #expect(payload == [0x11, 0x22, 0x33, 0x44])
        #expect(payloadRange.lowerBound >= keyRange.lowerBound)
        #expect(payloadRange.upperBound <= keyRange.upperBound)
        #expect(cursor.isAtEnd)
    }

    @Test func packedTupleRetainsByteViewsIntoTheSourceOwner() throws {
        let packed = Tuple(Bytes([0x11, 0x22, 0x33, 0x44]), Int64(7)).pack()
        let tuple = try Tuple(packed: packed)
        let payload = try #require(
            try tuple.element(at: 0) as? Bytes
        )
        let packedRange = try #require(packed.withUnsafeBytes { buffer in
            buffer.baseAddress.map { address in
                let start = UInt(bitPattern: address)
                return start..<(start + UInt(buffer.count))
            }
        })
        let payloadRange = try #require(payload.withUnsafeBytes { buffer in
            buffer.baseAddress.map { address in
                let start = UInt(bitPattern: address)
                return start..<(start + UInt(buffer.count))
            }
        })

        #expect(payload == [0x11, 0x22, 0x33, 0x44])
        #expect(payloadRange.lowerBound >= packedRange.lowerBound)
        #expect(payloadRange.upperBound <= packedRange.upperBound)
        #expect(try tuple.element(at: 1) as? Int64 == 7)
    }

    @Test func viewsFromTheSameOwnerPermitNestedComparisonBorrows() {
        let owner = NestedBorrowBytesOwner(
            bytes: [0x10, 0x20, 0x30, 0x40]
        )
        let bytes = Bytes(retaining: owner)
        let lhs = bytes[1..<3]
        let rhs = bytes[1..<3]

        #expect(lhs == rhs)
        #expect(owner.maximumActiveBorrowCount == 2)
    }

    @Test func slicesRetainAllocationWithoutMaterializing() {
        let releaseRecorder = BytesReleaseRecorder()
        var bytes: Bytes? = Bytes(
            makeOwnedEmbeddedBytes(
                [0x10, 0x20, 0x30, 0x40],
                releaseRecorder: releaseRecorder
            )
        )
        var slice = bytes?[1..<3]

        bytes = nil
        #expect(releaseRecorder.releaseCount == 0)
        #expect(slice == [0x20, 0x30])

        slice = nil
        #expect(releaseRecorder.releaseCount == 1)
    }

    @Test func mutatingSliceDetachesWithoutChangingSharedStorage() {
        let releaseRecorder = BytesReleaseRecorder()
        var parent: Bytes? = Bytes(
            makeOwnedEmbeddedBytes(
                [0x10, 0x20, 0x30, 0x40],
                releaseRecorder: releaseRecorder
            )
        )
        var slice = parent?[1..<3]

        slice?[0] = 0xFF
        #expect(parent == [0x10, 0x20, 0x30, 0x40])
        #expect(slice == [0xFF, 0x30])
        #expect(releaseRecorder.releaseCount == 0)

        parent = nil
        #expect(releaseRecorder.releaseCount == 1)
        #expect(slice == [0xFF, 0x30])
    }

    @Test func detachedFullRangeReleasesItsOriginalAllocation() throws {
        let releaseRecorder = BytesReleaseRecorder()
        var frame: Bytes? = Bytes(
            makeOwnedEmbeddedBytes(
                [0x10, 0x20, 0x30, 0x40],
                releaseRecorder: releaseRecorder
            )
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
        #expect(detached == [0x10, 0x20, 0x30, 0x40])

        detached = nil
        #expect(releaseRecorder.releaseCount == 1)
    }

    @Test func detachedEmptySliceReleasesItsOriginalAllocation() {
        let releaseRecorder = BytesReleaseRecorder()
        var frame: Bytes? = Bytes(
            makeOwnedEmbeddedBytes(
                [0x10, 0x20, 0x30, 0x40],
                releaseRecorder: releaseRecorder
            )
        )
        var emptySlice = frame?[2..<2]
        let detached = emptySlice?.detached()

        frame = nil
        #expect(releaseRecorder.releaseCount == 0)
        emptySlice = nil
        #expect(releaseRecorder.releaseCount == 1)
        #expect(detached?.isEmpty == true)
    }

    @Test func splitPointsDoNotRetainSourcePageStorage() async throws {
        let releaseRecorder = BytesReleaseRecorder()
        var frame: Bytes? = Bytes(
            makeOwnedEmbeddedBytes(
                [0x10, 0xA1, 0xA2, 0xA3, 0x20, 0xB1, 0xB2, 0xB3],
                releaseRecorder: releaseRecorder
            )
        )
        var rows: OwnershipRows? = OwnershipRows(
            rows: [
                (frame![0..<1], frame![1..<4]),
                (frame![4..<5], frame![5..<8]),
            ]
        )

        let splitPoints = try await StorageRangeMetrics.splitPoints(
            beginKey: [0x00],
            endKey: [0x30],
            chunkSize: 4,
            maximumPointCount: 3,
            rows: rows!
        )

        frame = nil
        rows = nil
        #expect(releaseRecorder.releaseCount == 1)
        #expect(splitPoints == [[0x00], [0x20], [0x30]])
    }

    @Test func detachingExactOwnedStorageIsIdempotent() throws {
        let exact = Bytes.copying(count: 4) { output in
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
        let bytes = Bytes(owningExact: array)
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

    @Test func splitPointLimitFailsBeforeReadingRemainingRows() async {
        let nextInvocationCounter = IteratorNextCounter()
        let rows = NextCountingRows(
            rows: [
                ([0x10], [0xA0]),
                ([0x20], [0xB0]),
                ([0x30], [0xC0]),
            ],
            nextInvocationCounter: nextInvocationCounter
        )

        await #expect(throws: StorageError.self) {
            _ = try await StorageRangeMetrics.splitPoints(
                beginKey: [0x00],
                endKey: [0x40],
                chunkSize: 1,
                maximumPointCount: 2,
                rows: rows
            )
        }
        #expect(nextInvocationCounter.value == 2)
    }

    private func makeOwnedEmbeddedBytes(
        _ bytes: [UInt8],
        releaseRecorder: BytesReleaseRecorder
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

    private final class BytesReleaseRecorder: Sendable {
        private let state = Mutex(0)

        var releaseCount: Int {
            state.withLock { $0 }
        }

        func recordRelease() {
            state.withLock { $0 += 1 }
        }
    }
}

private struct OwnershipRows: TransactionRangeResult {
    typealias Element = (Bytes, Bytes)

    let rows: [Element]

    func makeAsyncIterator() -> Iterator {
        Iterator(rows: rows)
    }

    struct Iterator: TransactionRangeIterator {
        let rows: [Element]
        var index = 0

        mutating func next() async -> Element? {
            guard index < rows.count else {
                return nil
            }
            defer { index += 1 }
            return rows[index]
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {}
    }
}

private struct NextCountingRows: TransactionRangeResult {
    typealias Element = (Bytes, Bytes)

    let rows: [Element]
    let nextInvocationCounter: IteratorNextCounter

    func makeAsyncIterator() -> Iterator {
        Iterator(rows: rows, nextInvocationCounter: nextInvocationCounter)
    }

    struct Iterator: TransactionRangeIterator {
        let rows: [Element]
        let nextInvocationCounter: IteratorNextCounter
        var index = 0

        mutating func next() async -> Element? {
            nextInvocationCounter.increment()
            guard index < rows.count else {
                return nil
            }
            defer { index += 1 }
            return rows[index]
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {}
    }
}

private final class IteratorNextCounter: Sendable {
    private let state = Mutex(0)

    var value: Int {
        state.withLock { $0 }
    }

    func increment() {
        state.withLock { $0 += 1 }
    }
}

private struct ArrayBackedBytesOwner: BytesOwner {
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

private final class NestedBorrowBytesOwner: BytesOwner {
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
