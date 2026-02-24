import Testing
import Foundation
@testable import StorageKit

@Suite("Tuple Layer Tests")
struct TupleTests {

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
        // 0x00 は 0x00 0xFF にエスケープされるべき
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

    // MARK: - Bytes

    @Test func bytesRoundTrip() throws {
        let original: Bytes = [0x01, 0x02, 0x00, 0x03, 0xFF]
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Bytes.decodeTuple(from: encoded, at: &offset)
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

    // MARK: - Int

    @Test func intNativeRoundTrip() throws {
        let original = 12345
        let encoded = original.encodeTuple()
        var offset = 1
        let decoded = try Int.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
    }

    // MARK: - UInt64

    @Test func uint64RoundTrip() throws {
        let values: [UInt64] = [0, 1, 255, 65535, UInt64(Int64.max)]
        for original in values {
            let encoded = original.encodeTuple()
            var offset = 1
            let decoded = try UInt64.decodeTuple(from: encoded, at: &offset)
            #expect(decoded == original, "Failed for \(original)")
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
        let original = UUID()
        let encoded = original.encodeTuple()
        #expect(encoded.count == 17) // 1 type code + 16 bytes
        #expect(encoded[0] == 0x30)
        var offset = 1
        let decoded = try UUID.decodeTuple(from: encoded, at: &offset)
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
        let uuid = UUID()
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
        #expect(elements[4] as? UUID == uuid)
        #expect(elements[5] is TupleNil)
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
        // advanceOffset のバグ検出: depth 追跡で 0x05 を誤認する問題
        let innermost = Tuple("a")
        let middle = Tuple(innermost)
        let outer = Tuple("prefix", middle, "suffix")
        let packed = outer.pack()
        let elements = try Tuple.unpack(from: packed)
        #expect(elements.count == 3)
        #expect(elements[0] as? String == "prefix")
        #expect(elements[2] as? String == "suffix")

        // 中間 Tuple の検証
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
        // 文字列に 0x00 バイトを含むケース
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

    // MARK: - FDB バイト互換

    @Test func int64MinEncoding() throws {
        // FDB 公式仕様: n=8 は raw two's complement
        // Int64.min = -9223372036854775808 → [0x0C, 0x80, 0x00, ...]
        let encoded = Int64.min.encodeTuple()
        #expect(encoded[0] == 0x0C) // type code = 0x14 - 8
        #expect(encoded[1] == 0x80) // MSB of two's complement
        #expect(encoded.count == 9)
    }

    @Test func negativeOneEncoding() throws {
        // -1: magnitude=1, n=1, limit=255, encoded=255-1=254=0xFE
        let encoded = Int64(-1).encodeTuple()
        #expect(encoded == [0x13, 0xFE])
    }

    @Test func integerFullRangeOrdering() throws {
        // Int64.min から Int64.max まで辞書順が正しいことを確認
        let values: [Int64] = [
            .min, .min + 1,
            -72057594037927936, -72057594037927935, // n=8/n=7 境界
            -256, -255, -1, 0, 1, 255, 256,
            72057594037927935, 72057594037927936,
            .max - 1, .max
        ]
        var previousPacked: Bytes?
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
        var previousPacked: Bytes?
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
        var previousPacked: Bytes?
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
        var previousPacked: Bytes?
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
