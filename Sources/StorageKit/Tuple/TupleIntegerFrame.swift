import DatabaseTypes

/// Framing of one Tuple Layer integer element.
///
/// The type code fixes the sign and the payload width together. Codes `0x0C`
/// through `0x1C` carry the width as their distance from `0x14`. `0x1D` and
/// `0x0B` are the extended forms: the byte after the type code is the payload
/// width, stored raw after `0x1D` and one's complemented after `0x0B` so that
/// the encoding keeps sorting by value. Reading an extended form as a
/// fixed-width payload misreads the value and leaves the offset inside the
/// following element.
struct TupleIntegerFrame {
    let isNegative: Bool
    let payload: Range<Int>
}

/// Reads the integer type code at `offset - 1` and consumes its payload.
///
/// - Throws: `TupleError.invalidTypeCode` when the code is not an integer code
///   or an extended form declares a width the fixed-width codes already cover,
///   and `TupleError.unexpectedEndOfData` when the payload is truncated.
func decodeTupleIntegerFrame(
    from bytes: ByteString,
    at offset: inout Int
) throws -> TupleIntegerFrame {
    guard offset > bytes.startIndex else {
        throw TupleError.unexpectedEndOfData
    }
    let typeCode = bytes[offset - 1]
    let intZero = TupleTypeCode.intZero.rawValue
    guard typeCode >= negativeIntStartCode, typeCode <= positiveIntEndCode else {
        throw TupleError.invalidTypeCode(typeCode)
    }

    let isNegative: Bool
    let length: Int
    switch typeCode {
    case positiveIntEndCode, negativeIntStartCode:
        guard offset < bytes.endIndex else {
            throw TupleError.unexpectedEndOfData
        }
        isNegative = typeCode == negativeIntStartCode
        let lengthByte = bytes[offset]
        length = Int(isNegative ? lengthByte ^ 0xFF : lengthByte)
        offset += 1
        // A width the fixed-width codes already express is not this form.
        guard length > MemoryLayout<UInt64>.size else {
            throw TupleError.invalidTypeCode(typeCode)
        }
    default:
        isNegative = typeCode < intZero
        length = isNegative ? Int(intZero - typeCode) : Int(typeCode - intZero)
    }

    guard offset + length <= bytes.endIndex else {
        throw TupleError.unexpectedEndOfData
    }
    let payload = offset..<(offset + length)
    offset += length
    return TupleIntegerFrame(isNegative: isNegative, payload: payload)
}

/// Absolute value of a framed integer.
///
/// - Throws: `TupleError.integerOverflow` when the payload is wider than 64
///   bits, which no Swift integer element can represent.
func decodeTupleIntegerMagnitude(
    from bytes: ByteString,
    frame: TupleIntegerFrame
) throws -> UInt64 {
    let length = frame.payload.count
    guard length <= MemoryLayout<UInt64>.size else {
        throw TupleError.integerOverflow
    }
    var stored: UInt64 = 0
    for index in frame.payload {
        stored = (stored << 8) | UInt64(bytes[index])
    }
    // A negative payload stores (2^(8*length) - 1) + value.
    return frame.isNegative ? sizeLimits[length - 1] - stored : stored
}

let negativeIntStartCode: UInt8 = 0x0B
let positiveIntEndCode: UInt8 = 0x1D
