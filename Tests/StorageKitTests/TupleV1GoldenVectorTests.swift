import DatabaseTypes
import Testing

@testable import StorageKit

@Suite("Tuple V1 Golden Vectors")
struct TupleV1GoldenVectorTests {
    @Test("Canonical type codes are frozen")
    func canonicalTypeCodesAreFrozen() {
        #expect(TupleTypeCode.null.rawValue == 0x00)
        #expect(TupleTypeCode.bytes.rawValue == 0x01)
        #expect(TupleTypeCode.string.rawValue == 0x02)
        #expect(TupleTypeCode.nested.rawValue == 0x05)
        #expect(TupleTypeCode.negativeInt8.rawValue == 0x0c)
        #expect(TupleTypeCode.intZero.rawValue == 0x14)
        #expect(TupleTypeCode.positiveInt8.rawValue == 0x1c)
        #expect(TupleTypeCode.float.rawValue == 0x20)
        #expect(TupleTypeCode.double.rawValue == 0x21)
        #expect(TupleTypeCode.boolFalse.rawValue == 0x26)
        #expect(TupleTypeCode.boolTrue.rawValue == 0x27)
        #expect(TupleTypeCode.uuid.rawValue == 0x30)
        #expect(TupleTypeCode.versionstamp.rawValue == 0x33)
    }

    @Test("Canonical encodings match frozen bytes and decode canonically")
    func canonicalEncodingsMatchFrozenBytes() throws {
        let vectors: [(name: String, tuple: Tuple, bytes: ByteString)] = [
            ("empty", Tuple(), []),
            ("null", Tuple(TupleNil()), [0x00]),
            (
                "escaped bytes",
                Tuple(ByteString([0x00, 0xff])),
                [0x01, 0x00, 0xff, 0xff, 0x00]
            ),
            (
                "escaped UTF-8 string",
                Tuple("A\0é"),
                [0x02, 0x41, 0x00, 0xff, 0xc3, 0xa9, 0x00]
            ),
            (
                "Int64 minimum",
                Tuple(Int64.min),
                [0x0c, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
            ),
            (
                "negative 2^56",
                Tuple(Int64(-72_057_594_037_927_936)),
                [0x0c, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
            ),
            ("negative 256", Tuple(Int64(-256)), [0x12, 0xfe, 0xff]),
            ("negative one", Tuple(Int64(-1)), [0x13, 0xfe]),
            ("integer zero", Tuple(Int64(0)), [0x14]),
            ("integer one", Tuple(Int64(1)), [0x15, 0x01]),
            ("integer 255", Tuple(Int64(255)), [0x15, 0xff]),
            ("integer 256", Tuple(Int64(256)), [0x16, 0x01, 0x00]),
            (
                "Int64 maximum",
                Tuple(Int64.max),
                [0x1c, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
            ),
            (
                "UInt64 maximum",
                Tuple(UInt64.max),
                [0x1c, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
            ),
            ("Float negative zero", Tuple(-Float.zero), [0x20, 0x7f, 0xff, 0xff, 0xff]),
            ("Float zero", Tuple(Float.zero), [0x20, 0x80, 0x00, 0x00, 0x00]),
            (
                "Double one",
                Tuple(Double(1)),
                [0x21, 0xbf, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
            ),
            ("false", Tuple(false), [0x26]),
            ("true", Tuple(true), [0x27]),
            (
                "UUID",
                Tuple(
                    DatabaseTypes.UUID(
                        high: 0x0011_2233_4455_6677,
                        low: 0x8899_aabb_ccdd_eeff
                    )
                ),
                [
                    0x30,
                    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
                ]
            ),
            (
                "complete versionstamp",
                Tuple(
                    Versionstamp(
                        transactionVersion: [
                            0x00, 0x01, 0x02, 0x03, 0x04,
                            0x05, 0x06, 0x07, 0x08, 0x09,
                        ],
                        userVersion: 0x1234
                    )
                ),
                [
                    0x33,
                    0x00, 0x01, 0x02, 0x03, 0x04,
                    0x05, 0x06, 0x07, 0x08, 0x09,
                    0x12, 0x34,
                ]
            ),
            (
                "nested null escape",
                Tuple(Tuple(TupleNil(), "x")),
                [0x05, 0x00, 0xff, 0x02, 0x78, 0x00, 0xff, 0x00]
            ),
            (
                "composite",
                Tuple("tenant", Int64(42), true),
                [
                    0x02, 0x74, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x00,
                    0x15, 0x2a,
                    0x27,
                ]
            ),
        ]

        for vector in vectors {
            #expect(vector.tuple.pack() == vector.bytes, "\(vector.name) encoding changed")

            let decoded = try Tuple(packed: vector.bytes)
            #expect(decoded.pack() == vector.bytes, "\(vector.name) is not canonical after decoding")
        }
    }

    @Test("Malformed V1 encodings fail explicitly")
    func malformedEncodingsFailExplicitly() {
        let malformed: [ByteString] = [
            [0xff],
            [0x02, 0x61],
            [0x02, 0xc0, 0x80, 0x00],
            [0x16, 0x01],
            [0x20, 0x00],
            [0x33, 0x00],
        ]

        for encoded in malformed {
            #expect(throws: TupleError.self) {
                _ = try Tuple(packed: encoded)
            }
        }
    }

    /// `0x1D` and `0x0B` carry their payload width in the byte that follows the
    /// type code. Reading either as a fixed-width payload returns a value that
    /// is not the encoded one and leaves the offset inside the next element.
    @Test("Extended integer forms are framed by their length byte")
    func extendedIntegerFormsAreFramed() throws {
        // 2^64 and -(2^64) as the reference implementation writes them.
        let positive: ByteString = [
            0x1d, 0x09, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]
        let negative: ByteString = [
            0x0b, 0xf6, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ]

        for encoded in [positive, negative] {
            var offset = encoded.startIndex + 1
            let frame = try decodeTupleIntegerFrame(from: encoded, at: &offset)
            #expect(frame.payload.count == 9, "the length byte fixes the payload width")
            #expect(offset == encoded.endIndex, "the element ends where the next one starts")

            // No Swift integer element holds a 9-byte value.
            #expect {
                _ = try decodeTupleIntegerMagnitude(from: encoded, frame: frame)
            } throws: { error in
                guard case TupleError.integerOverflow = error else { return false }
                return true
            }
            #expect(throws: TupleError.self) {
                _ = try Tuple(packed: encoded)
            }
        }

        // A width the fixed-width codes already express is not an extended form.
        let nonCanonicalWidth: ByteString = [
            0x1d, 0x08, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ]
        var nonCanonicalOffset = nonCanonicalWidth.startIndex + 1
        #expect(throws: TupleError.self) {
            _ = try decodeTupleIntegerFrame(from: nonCanonicalWidth, at: &nonCanonicalOffset)
        }

        let truncated: [ByteString] = [[0x1d], [0x0b], [0x1d, 0x09, 0x01]]
        for encoded in truncated {
            var offset = encoded.startIndex + 1
            #expect {
                _ = try decodeTupleIntegerFrame(from: encoded, at: &offset)
            } throws: { error in
                guard case TupleError.unexpectedEndOfData = error else { return false }
                return true
            }
        }
    }

    @Test("Every integer width round-trips and leaves the next element intact")
    func integerWidthsRoundTrip() throws {
        // 1 << 63 is Int64.min, whose negation traps, so the widest magnitudes
        // are listed directly instead of being derived inside the loop.
        var values: [Int64] = [Int64.min, Int64.min + 1, Int64.max, Int64.max - 1, 0]
        for bits in 1...62 {
            let magnitude = Int64(1) << bits
            values.append(contentsOf: [magnitude, -magnitude, magnitude - 1, 1 - magnitude])
        }

        for value in values {
            let packed = Tuple(value, "tail").pack()
            let decoded = try Tuple(packed: packed)
            #expect(decoded.count == 2, "\(value) must not disturb the following element")
            #expect(try decoded.element(at: 0) as? Int64 == value, "\(value) must round-trip")
            #expect(
                try decoded.element(at: 1) as? String == "tail",
                "\(value) must end where it says"
            )
        }

        for value in [UInt64(Int64.max) + 1, UInt64.max] {
            let packed = Tuple(value, "tail").pack()
            let decoded = try Tuple(packed: packed)
            #expect(try decoded.element(at: 0) as? UInt64 == value)
            #expect(try decoded.element(at: 1) as? String == "tail")
        }
    }
}
