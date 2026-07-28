import DatabaseTypes
// MARK: - Int64

extension Int64: TupleElement {
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
        let typeCode = bytes[offset - 1]
        let intZero = TupleTypeCode.intZero.rawValue

        if typeCode == intZero {
            return 0
        }

        // Positive integer: type code 0x15-0x1D
        if typeCode > intZero && typeCode <= 0x1D {
            let n = Int(typeCode - intZero)
            guard offset + n <= bytes.endIndex else { throw TupleError.unexpectedEndOfData }
            var value: UInt64 = 0
            for i in 0..<n {
                value = (value << 8) | UInt64(bytes[offset + i])
            }
            offset += n
            guard value <= UInt64(Int64.max) else { throw TupleError.integerOverflow }
            return Int64(value)
        }

        // Negative integer: type code 0x0B-0x13
        if typeCode >= 0x0B && typeCode < intZero {
            let n = Int(intZero - typeCode)
            guard offset + n <= bytes.endIndex else { throw TupleError.unexpectedEndOfData }

            // n=9 (type code 0x0B): extended range negative integer, exceeds Int64
            if n > 8 {
                offset += n
                throw TupleError.integerOverflow
            }

            if n == 8 {
                var bitPattern: UInt64 = 0
                for i in 0..<8 {
                    bitPattern = (bitPattern << 8) | UInt64(bytes[offset + i])
                }
                offset += 8
                return Int64(bitPattern: bitPattern)
            }

            // n < 8: sizeLimits conversion
            var raw: UInt64 = 0
            for i in 0..<n {
                raw = (raw << 8) | UInt64(bytes[offset + i])
            }
            offset += n
            let limit = sizeLimits[n - 1]
            let magnitude = limit - raw
            guard magnitude <= UInt64(Int64.max) else { throw TupleError.integerOverflow }
            return -Int64(magnitude)
        }

        throw TupleError.invalidTypeCode(typeCode)
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

        if n == 8 {
            // n=8: raw two's complement bit pattern (FDB official spec)
            // Python: struct.pack(">q", value)
            // Swift: big-endian representation of UInt64(bitPattern: value)
            let raw = UInt64(bitPattern: value)
            sink.writeByte(typeCode)
            for shift in stride(from: 56, through: 0, by: -8) {
                sink.writeByte(
                    UInt8(truncatingIfNeeded: raw >> UInt64(shift))
                )
            }
            return
        }

        // n < 8: sizeLimits conversion
        // Python: (size_limits[n] + value).to_bytes(n, 'big')
        // sizeLimits[n-1] (StorageKit) == size_limits[n] (Python), so equivalent to limit - magnitude
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
        let typeCode = bytes[offset - 1]
        let intZero = TupleTypeCode.intZero.rawValue

        if typeCode == intZero {
            return 0
        }

        guard typeCode > intZero && typeCode <= 0x1D else {
            throw TupleError.invalidTypeCode(typeCode)
        }

        let n = Int(typeCode - intZero)
        guard offset + n <= bytes.endIndex else { throw TupleError.unexpectedEndOfData }
        guard n <= MemoryLayout<UInt64>.size else {
            offset += n
            throw TupleError.integerOverflow
        }
        var value: UInt64 = 0
        for i in 0..<n {
            value = (value << 8) | UInt64(bytes[offset + i])
        }
        offset += n
        return value
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
