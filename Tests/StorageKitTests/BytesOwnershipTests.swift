import DatabaseTypes
import StorageKit
import Synchronization
import Testing

@Suite("Storage Byte View Tests")
struct BytesOwnershipTests {
    @Test func tupleCursorReturnsPayloadViewInsideSourceKey() throws {
        let key = Tuple(ByteString([0x11, 0x22, 0x33, 0x44])).pack()
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

    @Test func tupleCursorDecodesANonzeroIndexSourceView() throws {
        let packed = Tuple(ByteString([0x11, 0x22, 0x33])).pack()
        let owner = ByteString([0xFF] + packed.copyBytes() + [0xEE])
        let view = owner[1..<(owner.endIndex - 1)]
        var cursor = TupleCursor(bytes: view)

        #expect(try cursor.requireBytes() == [0x11, 0x22, 0x33])
        #expect(cursor.isAtEnd)
        #expect(cursor.consumedByteCount == packed.count)
    }

    @Test func packedTupleRetainsPayloadViewsIntoSourceOwner() throws {
        let packed = Tuple(
            ByteString([0x11, 0x22, 0x33, 0x44]),
            Int64(7)
        ).pack()
        let tuple = try Tuple(packed: packed)
        let payload = try #require(
            try tuple.element(at: 0) as? ByteString
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
        #expect(tuple.retainedByteCount == packed.retainedByteCount)
    }

    @Test func packedTupleRetainsOnlyItsMeasurablePackedOwner() throws {
        let releaseRecorder = ByteReleaseRecorder()
        var packed: ByteString? = makeOwnedBytes(
            Tuple("identifier", Int64(42)).pack().copyBytes(),
            releaseRecorder: releaseRecorder
        )
        var tuple: Tuple? = try Tuple(packed: packed!)

        #expect(tuple?.retainedByteCount == packed?.retainedByteCount)
        packed = nil
        #expect(releaseRecorder.releaseCount == 0)
        #expect(try tuple?.element(at: 0) as? String == "identifier")

        tuple = nil
        #expect(releaseRecorder.releaseCount == 1)
    }

    @Test func sourceAllocationLivesUntilItsLastStorageViewReleases() {
        let releaseRecorder = ByteReleaseRecorder()
        var bytes: ByteString? = makeOwnedBytes(
            [0x10, 0x20, 0x30, 0x40],
            releaseRecorder: releaseRecorder
        )
        var slice = bytes?[1..<3]

        bytes = nil
        #expect(releaseRecorder.releaseCount == 0)
        #expect(slice == [0x20, 0x30])

        slice = nil
        #expect(releaseRecorder.releaseCount == 1)
    }

    @Test func detachedViewReleasesItsOriginalAllocation() throws {
        let releaseRecorder = ByteReleaseRecorder()
        var frame: ByteString? = makeOwnedBytes(
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
        #expect(detached == [0x10, 0x20, 0x30, 0x40])

        detached = nil
        #expect(releaseRecorder.releaseCount == 1)
    }

    @Test func detachedEmptyViewReleasesItsOriginalAllocation() {
        let releaseRecorder = ByteReleaseRecorder()
        var frame: ByteString? = makeOwnedBytes(
            [0x10, 0x20, 0x30, 0x40],
            releaseRecorder: releaseRecorder
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
        let releaseRecorder = ByteReleaseRecorder()
        var frame: ByteString? = makeOwnedBytes(
            [0x10, 0xA1, 0xA2, 0xA3, 0x20, 0xB1, 0xB2, 0xB3],
            releaseRecorder: releaseRecorder
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

    private func makeOwnedBytes(
        _ bytes: [UInt8],
        releaseRecorder: ByteReleaseRecorder
    ) -> ByteString {
        ByteString(
            retaining: TestByteAllocation(
                bytes: bytes,
                releaseRecorder: releaseRecorder
            )
        )
    }
}

private final class TestByteAllocation: ByteStringOwner {
    let count: Int

    private let address: UInt
    private let releaseRecorder: ByteReleaseRecorder

    init(
        bytes: [UInt8],
        releaseRecorder: ByteReleaseRecorder
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

private final class ByteReleaseRecorder: Sendable {
    private let state = Mutex(0)

    var releaseCount: Int {
        state.withLock { $0 }
    }

    func recordRelease() {
        state.withLock { $0 += 1 }
    }
}

private struct OwnershipRows: TransactionRangeResult {
    typealias Element = (ByteString, ByteString)

    let rows: [Element]

    func makeCursor() -> Cursor {
        Cursor(rows: rows)
    }

    struct Cursor: TransactionRangeCursor {
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
    typealias Element = (ByteString, ByteString)

    let rows: [Element]
    let nextInvocationCounter: IteratorNextCounter

    func makeCursor() -> Cursor {
        Cursor(
            rows: rows,
            nextInvocationCounter: nextInvocationCounter
        )
    }

    struct Cursor: TransactionRangeCursor {
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
