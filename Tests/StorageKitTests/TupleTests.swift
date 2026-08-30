import DatabaseTypes
import Foundation
import StorageKitFoundation
import Synchronization
import Testing

@testable import StorageKit

private enum TupleDecodeAdmissionTestError: Error {
    case rejected
}

private enum TuplePackAdmissionTestError: Error {
    case rejected
}

private final class TupleEncodingCounter: Sendable {
    private let countState = Mutex(0)

    var count: Int {
        countState.withLock { $0 }
    }

    func recordEncoding() {
        countState.withLock { $0 += 1 }
    }

    func reset() {
        countState.withLock { $0 = 0 }
    }
}

private struct CountingTupleElement: TupleElement {
    let counter: TupleEncodingCounter

    func encodeTuple(to sink: inout TupleEncodingSink) {
        counter.recordEncoding()
        sink.writeByte(TupleTypeCode.boolTrue.rawValue)
    }

    static func decodeTuple(
        from bytes: ByteString,
        at offset: inout Int
    ) throws -> CountingTupleElement {
        throw TupleError.invalidTypeCode(0xFF)
    }

    static func == (
        lhs: CountingTupleElement,
        rhs: CountingTupleElement
    ) -> Bool {
        lhs.counter === rhs.counter
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(counter))
    }
}

@Suite("Tuple Layer Tests")
struct TupleTests {

    @Test("Admission-aware packing measures and encodes exactly once")
    func admissionAwarePackingUsesOneMeasureAndOneEncode() throws {
        let counter = TupleEncodingCounter()
        let tuple = Tuple(CountingTupleElement(counter: counter))
        let expected = tuple.pack()
        counter.reset()
        var admittedByteCounts: [Int] = []

        let packed = try tuple.pack(admitting: { byteCount in
            admittedByteCounts.append(byteCount)
        })

        #expect(packed == expected)
        #expect(admittedByteCounts == [packed.count])
        #expect(counter.count == 2)
    }

    @Test("Admission-aware packing rejects before materialization")
    func admissionAwarePackingPreservesTypedFailure() {
        let counter = TupleEncodingCounter()
        let tuple = Tuple(CountingTupleElement(counter: counter))
        let expectedByteCount = Tuple(true).pack().count
        var admittedByteCounts: [Int] = []

        #expect(throws: TuplePackAdmissionTestError.rejected) {
            try tuple.pack(admitting: { byteCount in
                admittedByteCounts.append(byteCount)
                throw TuplePackAdmissionTestError.rejected
            })
        }

        #expect(admittedByteCounts == [expectedByteCount])
        #expect(counter.count == 1)
    }

    @Test("Tuple decoding admits storage before malformed input can allocate past a limit")
    func tupleDecodeAdmissionPrecedesAllocation() {
        let encoded = ByteString(
            [UInt8](repeating: TupleTypeCode.boolFalse.rawValue, count: 1_024)
                + [0xFF]
        )
        var admittedBytes = 0

        #expect(throws: TupleDecodeAdmissionTestError.rejected) {
            _ = try Tuple(packed: encoded) { requestedBytes in
                guard requestedBytes <= 4_096 - admittedBytes else {
                    throw TupleDecodeAdmissionTestError.rejected
                }
                admittedBytes += requestedBytes
            }
        }
        #expect(admittedBytes <= 4_096)
    }

    @Test("String allocation is rejected before materialization")
    func stringAllocationRequiresAdmission() {
        let encoded = Tuple(String(repeating: "x", count: 4_096)).pack()

        #expect(throws: TupleDecodeAdmissionTestError.rejected) {
            _ = try Tuple(packed: encoded) { _ in
                throw TupleDecodeAdmissionTestError.rejected
            }
        }
    }

    @Test("Escaped byte allocation is rejected before materialization")
    func escapedByteAllocationRequiresAdmission() {
        let encoded = Tuple(ByteString(repeating: 0x00, count: 4_096)).pack()

        #expect(throws: TupleDecodeAdmissionTestError.rejected) {
            _ = try Tuple(packed: encoded) { _ in
                throw TupleDecodeAdmissionTestError.rejected
            }
        }
    }

    @Test("Tuple cursor materializes remaining elements once with admission")
    func tupleCursorAdmitsRemainingTuple() throws {
        let encoded = Tuple(
            Int64(42),
            "tenant",
            ByteString([0x01, 0x00, 0x02])
        ).pack()
        var cursor = TupleCursor(bytes: encoded)
        var admittedBytes = 0

        #expect(try cursor.requireInt64() == 42)
        let remaining = try cursor.remainingTuple { requestedBytes in
            admittedBytes += requestedBytes
        }

        #expect(remaining == Tuple("tenant", ByteString([0x01, 0x00, 0x02])))
        #expect(admittedBytes > 0)
        #expect(cursor.isAtEnd)
    }

    @Test func validatedElementRangePreservesValues() throws {
        let tuple = Tuple("prefix", Int64(42), ByteString([0x01, 0x02]))
        let elements = try tuple.elements(in: 1..<3)

        #expect(elements.count == 2)
        #expect(elements[0] as? Int64 == 42)
        #expect(elements[1] as? ByteString == ByteString([0x01, 0x02]))
    }

    @Test func invalidElementRangeThrowsTypedError() {
        let tuple = Tuple("only")

        #expect(throws: TupleError.self) {
            _ = try tuple.elements(in: 0..<2)
        }
    }

    // MARK: - String

    @Test func stringRoundTrip() throws {
        let original = "hello world"
        let encoded = original.encodeTuple()
        var offset = 1 // skip type code
        let decoded = try String.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    @Test func stringWithNullBytes() throws {
        let original = "hello\0world"
        let encoded = original.encodeTuple()
        // A contained 0x00 byte must be escaped as 0x00 0xFF.
        #expect(encoded.contains(where: { $0 == 0xFF }))
        var offset = 1
        let decoded = try String.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    @Test func emptyString() throws {
        let original = ""
        let encoded = original.encodeTuple()
        #expect(encoded == [0x02, 0x00]) // type code + terminator
        var offset = 1
        let decoded = try String.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    @Test(
        "Invalid UTF-8 is rejected",
        arguments: [
            ByteString([0x02, 0xC0, 0x80, 0x00]),
            ByteString([0x02, 0xED, 0xA0, 0x80, 0x00]),
            ByteString([0x02, 0xF4, 0x90, 0x80, 0x80, 0x00]),
            ByteString([0x02, 0xE2, 0x82, 0x00]),
        ]
    )
    func invalidUTF8IsRejected(_ encoded: ByteString) {
        var offset = 1

        #expect(throws: TupleError.self) {
            _ = try String.decodeTuple(from: encoded, at: &offset)
        }
    }

    // MARK: - ByteString

    @Test func bytesRoundTrip() throws {
        let original: ByteString = [0x01, 0x02, 0x00, 0x03, 0xFF]
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try ByteString.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    // MARK: - Int64

    @Test func intZero() throws {
        let encoded = Int64(0).encodeTuple()
        #expect(encoded == [0x14])
        var offset = 1
        let decoded = try Int64.decodeTuple(from: [0x14], at: &offset)
        #expect(decoded == 0)
    }

    @Test func intPositiveSmall() throws {
        let original = Int64(42)
        let encoded = original.encodeTuple()
        #expect(encoded[0] == 0x15) // 1-byte positive
        #expect(encoded[1] == 42)
        var offset = 1
        let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    @Test func intPositiveLarge() throws {
        let original = Int64(100_000)
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    @Test func intPositiveMax() throws {
        let original = Int64.max
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    @Test func intNegativeSmall() throws {
        let original = Int64(-42)
        let encoded = original.encodeTuple()
        #expect(encoded[0] == 0x13) // 1-byte negative
        var offset = 1
        let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    @Test func intNegativeLarge() throws {
        let original = Int64(-100_000)
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    @Test func intNegativeMin() throws {
        let original = Int64.min
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    @Test func intVariousValues() throws {
        let values: [Int64] = [
            -1, 1, -127, 127, -128, 128, -255, 255, -256, 256,
            -65535, 65535, -65536, 65536,
            -16_777_215, 16_777_215,
            -4_294_967_295, 4_294_967_295,
        ]
        for original in values {
            let encoded = original.encodeTuple()
            var offset = 1
            let decoded = try Int64.decodeTuple(from: encoded, at: &offset)
            #expect(decoded == original, "Failed for \(original)")
        }
    }

    @Test("Tuple cursor decodes signed integers without generic materialization")
    func tupleCursorRequiresInt64() throws {
        let encoded = Tuple(Int64.min, Int64(0), Int64.max).pack()
        var cursor = TupleCursor(bytes: encoded)

        #expect(try cursor.requireInt64() == .min)
        #expect(try cursor.requireInt64() == 0)
        #expect(try cursor.requireInt64() == .max)
        #expect(cursor.isAtEnd)
    }

    @Test("Tuple cursor rejects a non-integer before decoding its payload")
    func tupleCursorRejectsNonIntegerBeforePayloadDecode() {
        var bytes = [UInt8](
            repeating: 0x61,
            count: 1_024 * 1_024 + 2
        )
        bytes[0] = TupleTypeCode.string.rawValue
        bytes[bytes.count - 1] = 0x00
        let encoded = ByteString(bytes)
        var cursor = TupleCursor(bytes: encoded)

        #expect {
            _ = try cursor.requireInt64()
        } throws: { error in
            guard case TupleError.invalidTypeCode(let typeCode) = error else {
                return false
            }
            return typeCode == TupleTypeCode.string.rawValue
        }
        #expect(cursor.consumedByteCount == 0)
    }

    // MARK: - Int

    @Test func platformIntegerRoundTrip() throws {
        let original = 12345
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Int.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    // MARK: - UInt64

    @Test func uint64RoundTrip() throws {
        let values: [UInt64] = [
            0,
            1,
            255,
            65_535,
            UInt64(Int64.max),
            UInt64(Int64.max) + 1,
            .max,
        ]
        for original in values {
            let encoded = original.encodeTuple()
            var offset = 1
            let decoded = try UInt64.decodeTuple(from: encoded, at: &offset)
            #expect(decoded == original, "Failed for \(original)")
        }
    }

    @Test func integerBeyondUInt64RangeIsRejected() {
        let encoded = ByteString([
            0x1D,
            0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ])
        var offset = 1

        #expect(throws: TupleError.self) {
            _ = try UInt64.decodeTuple(from: encoded, at: &offset)
        }
    }

    // MARK: - Double

    @Test func doubleRoundTrip() throws {
        let values: [Double] = [0.0, 1.0, -1.0, 3.14159, -273.15, .infinity, -.infinity]
        for original in values {
            let encoded = original.encodeTuple()
            var offset = 1
            let decoded = try Double.decodeTuple(from: encoded, at: &offset)
            #expect(decoded == original, "Failed for \(original)")
        }
    }

    @Test func doubleNaN() throws {
        let original = Double.nan
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Double.decodeTuple(from: encoded, at: &offset)
        #expect(decoded.isNaN)
    }

    // MARK: - Float

    @Test func floatRoundTrip() throws {
        let values: [Float] = [0.0, 1.0, -1.0, 3.14, -273.15, .infinity, -.infinity]
        for original in values {
            let encoded = original.encodeTuple()
            var offset = 1
            let decoded = try Float.decodeTuple(from: encoded, at: &offset)
            #expect(decoded == original, "Failed for \(original)")
        }
    }

    // MARK: - Bool

    @Test func boolRoundTrip() throws {
        let trueEncoded = true.encodeTuple()
        #expect(trueEncoded == [0x27])
        let falseEncoded = false.encodeTuple()
        #expect(falseEncoded == [0x26])

        var offset = 1
        let decodedTrue = try Bool.decodeTuple(from: trueEncoded, at: &offset)
        #expect(decodedTrue == true)

        offset = 1
        let decodedFalse = try Bool.decodeTuple(from: falseEncoded, at: &offset)
        #expect(decodedFalse == false)
    }

    // MARK: - UUID

    @Test func uuidRoundTrip() throws {
        let original = DatabaseTypes.UUID(
            high: 0x0011_2233_4455_6677,
            low: 0x8899_AABB_CCDD_EEFF
        )
        let encoded = original.encodeTuple()
        #expect(encoded.count == 17) // 1 type code + 16 bytes
        #expect(encoded[0] == 0x30)
        var offset = 1
        let decoded = try DatabaseTypes.UUID.decodeTuple(
            from: encoded,
            at: &offset
        )
        #expect(decoded == original)
    }

    @Test func foundationUUIDAdapterRoundTrips() throws {
        let original = Foundation.UUID()
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Foundation.UUID.decodeTuple(
            from: encoded,
            at: &offset
        )

        #expect(decoded == original)
    }

    // MARK: - Date

    @Test func dateRoundTrip() throws {
        let original = Date(timeIntervalSince1970: 1_000_000.5)
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Date.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    // MARK: - TupleNil

    @Test func nilRoundTrip() throws {
        let original = TupleNil()
        let encoded = original.encodeTuple()
        #expect(encoded == [0x00])
    }

    // MARK: - Tuple pack/unpack

    @Test func tuplePackUnpack() throws {
        let tuple = Tuple("hello", Int64(42), true)
        let packed = tuple.pack()
        let elements = try Tuple.unpack(from: packed)
        #expect(elements.count == 3)
        #expect(elements[0] as? String == "hello")
        #expect(elements[1] as? Int64 == 42)
        #expect(elements[2] as? Bool == true)
    }

    @Test func tupleMultiType() throws {
        let uuid = Foundation.UUID()
        let tuple = Tuple(
            "test",
            Int64(-100),
            3.14,
            true,
            uuid,
            TupleNil()
        )
        let packed = tuple.pack()
        let elements = try Tuple.unpack(from: packed)
        #expect(elements.count == 6)
        #expect(elements[0] as? String == "test")
        #expect(elements[1] as? Int64 == -100)
        #expect(elements[2] as? Double == 3.14)
        #expect(elements[3] as? Bool == true)
        #expect(elements[4] as? DatabaseTypes.UUID == DatabaseTypes.UUID(uuid))
        #expect(elements[5] is TupleNil)
    }

    @Test func packedByteCountMatchesMaterializedEncoding() {
        let tuples = [
            Tuple(),
            Tuple("embedded\0string", Int64.min, UInt64.max),
            Tuple(
                ByteString([0x00, 0xff, 0x00]),
                Tuple("nested\0value", Double.pi)
            ),
        ]

        for tuple in tuples {
            #expect(tuple.packedByteCount == tuple.pack().count)
        }
    }

    @Test func emptyTuple() throws {
        let tuple = Tuple([any TupleElement]())
        let packed = tuple.pack()
        #expect(packed.isEmpty)
        let elements = try Tuple.unpack(from: packed)
        #expect(elements.isEmpty)
    }

    @Test func tupleSubscript() throws {
        let tuple = Tuple("a", Int64(1), true)
        #expect(tuple.count == 3)
        #expect(tuple[0] as? String == "a")
        #expect(tuple[1] as? Int64 == 1)
        #expect(tuple[2] as? Bool == true)
        #expect(tuple[3] == nil) // out of bounds
        #expect(tuple[-1] == nil)
    }

    @Test func tupleAppend() throws {
        let tuple = Tuple("a", Int64(1))
        let extended = tuple.appending("b")
        #expect(extended.count == 3)

        let elements = try Tuple.unpack(from: extended.pack())
        #expect(elements[0] as? String == "a")
        #expect(elements[1] as? Int64 == 1)
        #expect(elements[2] as? String == "b")
    }

    // MARK: - Nested Tuple

    @Test func nestedTupleRoundTrip() throws {
        let inner = Tuple("inner", Int64(99))
        let outer = Tuple("outer", inner)
        let packed = outer.pack()
        let elements = try Tuple.unpack(from: packed)
        #expect(elements.count == 2)
        #expect(elements[0] as? String == "outer")

        let decodedInner = elements[1] as? Tuple
        #expect(decodedInner != nil)
        let innerElements = try Tuple.unpack(from: decodedInner!.pack())
        #expect(innerElements[0] as? String == "inner")
        #expect(innerElements[1] as? Int64 == 99)
    }

    @Test func doublyNestedTupleRoundTrip() throws {
        // Tuple("prefix", Tuple(Tuple("a")), "suffix")
        // Detects a decoder that mistakes a nested 0x05 byte for a depth change.
        let innermost = Tuple("a")
        let middle = Tuple(innermost)
        let outer = Tuple("prefix", middle, "suffix")
        let packed = outer.pack()
        let elements = try Tuple.unpack(from: packed)
        #expect(elements.count == 3)
        #expect(elements[0] as? String == "prefix")
        #expect(elements[2] as? String == "suffix")

        // Validate the intermediate tuple.
        let decodedMiddle = elements[1] as? Tuple
        #expect(decodedMiddle != nil)
        let middleElements = try Tuple.unpack(from: decodedMiddle!.pack())
        #expect(middleElements.count == 1)

        let decodedInnermost = middleElements[0] as? Tuple
        #expect(decodedInnermost != nil)
        let innermostElements = try Tuple.unpack(from: decodedInnermost!.pack())
        #expect(innermostElements[0] as? String == "a")
    }

    @Test func triplyNestedTupleRoundTrip() throws {
        let t1 = Tuple("deep")
        let t2 = Tuple(t1, Int64(42))
        let t3 = Tuple(t2)
        let outer = Tuple("start", t3, "end")
        let packed = outer.pack()
        let elements = try Tuple.unpack(from: packed)
        #expect(elements.count == 3)
        #expect(elements[0] as? String == "start")
        #expect(elements[2] as? String == "end")
    }

    @Test func nestedTupleWithNullBytesInString() throws {
        // Cover a string containing a 0x00 byte.
        let inner = Tuple("hello\0world")
        let outer = Tuple(inner, "after")
        let packed = outer.pack()
        let elements = try Tuple.unpack(from: packed)
        #expect(elements.count == 2)
        #expect(elements[1] as? String == "after")

        let decodedInner = elements[0] as? Tuple
        #expect(decodedInner != nil)
        let innerElements = try Tuple.unpack(from: decodedInner!.pack())
        #expect(innerElements[0] as? String == "hello\0world")
    }

    // MARK: - FDB Byte Compatibility

    @Test func int64MinEncoding() throws {
        // A negative is stored as sizeLimits[n] - magnitude, which at n=8 is
        // UInt64.max - magnitude, not the raw two's complement pattern. For
        // Int64.min that is 0xFFFF_FFFF_FFFF_FFFF - 0x8000_0000_0000_0000.
        let encoded = Int64.min.encodeTuple()
        #expect(encoded == [0x0C, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])
        var offset = encoded.startIndex + 1
        #expect(try Int64.decodeTuple(from: encoded, at: &offset) == Int64.min)
    }

    @Test func negativeOneEncoding() throws {
        // -1: magnitude=1, n=1, limit=255, encoded=255-1=254=0xFE
        let encoded = Int64(-1).encodeTuple()
        #expect(encoded == [0x13, 0xFE])
    }

    @Test func integerFullRangeOrdering() throws {
        // Verify lexicographic ordering from Int64.min through Int64.max.
        let values: [Int64] = [
            .min, .min + 1,
            -72057594037927936, -72057594037927935, // n=8/n=7 boundary
            -256, -255, -1, 0, 1, 255, 256,
            72057594037927935, 72057594037927936,
            .max - 1, .max
        ]
        var previousPacked: ByteString?
        for value in values {
            let packed = Tuple(value).pack()
            if let prev = previousPacked {
                #expect(compareBytes(prev, packed) < 0, "Ordering failed at \(value)")
            }
            previousPacked = packed
        }
    }

    // MARK: - Lexicographic ordering

    @Test func integerOrdering() throws {
        let values: [Int64] = [-1000, -100, -1, 0, 1, 100, 1000]
        var previousPacked: ByteString?
        for value in values {
            let packed = Tuple(value).pack()
            if let prev = previousPacked {
                #expect(compareBytes(prev, packed) < 0, "Ordering failed: \(value)")
            }
            previousPacked = packed
        }
    }

    @Test func stringOrdering() throws {
        let values = ["a", "aa", "ab", "b", "ba"]
        var previousPacked: ByteString?
        for value in values {
            let packed = Tuple(value).pack()
            if let prev = previousPacked {
                #expect(compareBytes(prev, packed) < 0, "Ordering failed: \(value)")
            }
            previousPacked = packed
        }
    }

    @Test func doubleOrdering() throws {
        let values: [Double] = [-.infinity, -100.0, -1.0, 0.0, 1.0, 100.0, .infinity]
        var previousPacked: ByteString?
        for value in values {
            let packed = Tuple(value).pack()
            if let prev = previousPacked {
                #expect(compareBytes(prev, packed) < 0, "Ordering failed: \(value)")
            }
            previousPacked = packed
        }
    }

    // MARK: - Equality semantics

    @Test func positiveZeroNotEqualNegativeZero() throws {
        let posZero = Tuple(0.0)
        let negZero = Tuple(-0.0)
        #expect(posZero != negZero)
    }

    @Test func nanEquality() throws {
        let nan1 = Tuple(Double.nan)
        let nan2 = Tuple(Double.nan)
        #expect(nan1 == nan2)
    }
}
