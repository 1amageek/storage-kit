import DatabaseTypes
// MARK: - Int64

extension Int64: TupleElement {
    public var tupleValue: TupleValue? { .signedInteger(self) }

    public func encodeTuple(to sink: inout TupleEncodingSink) {
        if self == 0 {
            sink.writeByte(TupleTypeCode.intZero.rawValue)
            return
        }
        if self > 0 {
            Self.encodePositive(UInt64(self), to: &sink)
        } else {
            Self.encodeNegative(self, to: &sink)
        }
    }

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> Int64 {
        guard offset > bytes.startIndex else {
            throw TupleError.unexpectedEndOfData
        }
        if bytes[offset - 1] == TupleTypeCode.intZero.rawValue {
            return 0
        }

        let frame = try decodeTupleIntegerFrame(from: bytes, at: &offset)
        let magnitude = try decodeTupleIntegerMagnitude(from: bytes, frame: frame)
        if frame.isNegative {
            guard magnitude <= UInt64(Int64.max) + 1 else {
                throw TupleError.integerOverflow
            }
            return Int64(bitPattern: 0 &- magnitude)
        }
        guard magnitude <= UInt64(Int64.max) else {
            throw TupleError.integerOverflow
        }
        return Int64(magnitude)
    }

    private static func encodePositive(
        _ value: UInt64,
        to sink: inout TupleEncodingSink
    ) {
        let n = byteCount(for: value)
        let typeCode = TupleTypeCode.intZero.rawValue + UInt8(n)
        sink.writeByte(typeCode)
        for shift in stride(from: (n - 1) * 8, through: 0, by: -8) {
            sink.writeByte(UInt8(truncatingIfNeeded: value >> UInt64(shift)))
        }
    }

    private static func encodeNegative(
        _ value: Int64,
        to sink: inout TupleEncodingSink
    ) {
        // Negate in UInt64 space (avoids overflow for Int64.min)
        let magnitude = 0 &- UInt64(bitPattern: value)
        let n = byteCount(for: magnitude)
        let typeCode = TupleTypeCode.intZero.rawValue - UInt8(n)

        // The payload is (2^(8*n) - 1) + value at every width, including n = 8
        // where sizeLimits[7] is UInt64.max. Writing the two's complement bit
        // pattern instead sorts the same but is one greater than the bytes the
        // reference implementation produces.
        let limit = sizeLimits[n - 1]
        let encoded = limit - magnitude
        sink.writeByte(typeCode)
        for shift in stride(from: (n - 1) * 8, through: 0, by: -8) {
            sink.writeByte(
                UInt8(truncatingIfNeeded: encoded >> UInt64(shift))
            )
        }
    }

    private static func byteCount(for value: UInt64) -> Int {
        var index = 0
        while index < sizeLimits.count {
            if value <= sizeLimits[index] {
                return index + 1
            }
            index += 1
        }
        return 8
    }
}

// MARK: - Int

extension Int: TupleElement {
    public var tupleValue: TupleValue? { .signedInteger(Int64(self)) }

    public func encodeTuple(to sink: inout TupleEncodingSink) {
        Int64(self).encodeTuple(to: &sink)
    }

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> Int {
        let value = try Int64.decodeTuple(from: bytes, at: &offset)
        guard let result = Int(exactly: value) else { throw TupleError.integerOverflow }
        return result
    }
}

// MARK: - Int32

extension Int32: TupleElement {
    public var tupleValue: TupleValue? { .signedInteger(Int64(self)) }

    public func encodeTuple(to sink: inout TupleEncodingSink) {
        Int64(self).encodeTuple(to: &sink)
    }

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> Int32 {
        let value = try Int64.decodeTuple(from: bytes, at: &offset)
        guard let result = Int32(exactly: value) else { throw TupleError.integerOverflow }
        return result
    }
}

// MARK: - UInt64

extension UInt64: TupleElement {
    public var tupleValue: TupleValue? { .unsignedInteger(self) }

    public func encodeTuple(to sink: inout TupleEncodingSink) {
        if self == 0 {
            sink.writeByte(TupleTypeCode.intZero.rawValue)
            return
        }
        let n = byteCount(for: self)
        let typeCode = TupleTypeCode.intZero.rawValue + UInt8(n)
        sink.writeByte(typeCode)
        for shift in stride(from: (n - 1) * 8, through: 0, by: -8) {
            sink.writeByte(UInt8(truncatingIfNeeded: self >> UInt64(shift)))
        }
    }

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> UInt64 {
        guard offset > bytes.startIndex else {
            throw TupleError.unexpectedEndOfData
        }
        if bytes[offset - 1] == TupleTypeCode.intZero.rawValue {
            return 0
        }

        let frame = try decodeTupleIntegerFrame(from: bytes, at: &offset)
        let magnitude = try decodeTupleIntegerMagnitude(from: bytes, frame: frame)
        guard !frame.isNegative else {
            throw TupleError.integerOverflow
        }
        return magnitude
    }

    private func byteCount(for value: UInt64) -> Int {
        var index = 0
        while index < sizeLimits.count {
            if value <= sizeLimits[index] {
                return index + 1
            }
            index += 1
        }
        return 8
    }
}
