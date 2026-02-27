// MARK: - Int64

extension Int64: TupleElement {
    public func encodeTuple() -> Bytes {
        if self == 0 {
            return [TupleTypeCode.intZero.rawValue]
        }
        if self > 0 {
            return Self.encodePositive(UInt64(self))
        }
        return Self.encodeNegative(self)
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Int64 {
        guard offset > 0 else { throw TupleError.unexpectedEndOfData }
        let typeCode = bytes[offset - 1]
        let intZero = TupleTypeCode.intZero.rawValue

        if typeCode == intZero {
            return 0
        }

        // Positive integer: type code 0x15-0x1D
        if typeCode > intZero && typeCode <= 0x1D {
            let n = Int(typeCode - intZero)
            guard offset + n <= bytes.count else { throw TupleError.unexpectedEndOfData }
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
            guard offset + n <= bytes.count else { throw TupleError.unexpectedEndOfData }

            if n == 8 {
                // n=8: raw two's complement bit pattern (FDB official spec)
                var bp = Bytes(repeating: 0, count: 8)
                for i in 0..<8 {
                    bp[i] = bytes[offset + i]
                }
                offset += 8
                // big-endian → Int64
                var result: Int64 = 0
                for byte in bp {
                    result = (result << 8) | Int64(byte)
                }
                return result
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

    private static func encodePositive(_ value: UInt64) -> Bytes {
        let n = byteCount(for: value)
        let typeCode = TupleTypeCode.intZero.rawValue + UInt8(n)
        var result = Bytes(repeating: 0, count: n + 1)
        result[0] = typeCode
        var v = value
        for i in stride(from: n, through: 1, by: -1) {
            result[i] = UInt8(v & 0xFF)
            v >>= 8
        }
        return result
    }

    private static func encodeNegative(_ value: Int64) -> Bytes {
        // Negate in UInt64 space (avoids overflow for Int64.min)
        let magnitude = 0 &- UInt64(bitPattern: value)
        let n = byteCount(for: magnitude)
        let typeCode = TupleTypeCode.intZero.rawValue - UInt8(n)

        if n == 8 {
            // n=8: raw two's complement bit pattern (FDB official spec)
            // Python: struct.pack(">q", value)
            // Swift: big-endian representation of UInt64(bitPattern: value)
            let raw = UInt64(bitPattern: value)
            var result = Bytes(repeating: 0, count: 9)
            result[0] = typeCode
            var v = raw
            for i in stride(from: 8, through: 1, by: -1) {
                result[i] = UInt8(v & 0xFF)
                v >>= 8
            }
            return result
        }

        // n < 8: sizeLimits conversion
        // Python: (size_limits[n] + value).to_bytes(n, 'big')
        // sizeLimits[n-1] (StorageKit) == size_limits[n] (Python), so equivalent to limit - magnitude
        let limit = sizeLimits[n - 1]
        let encoded = limit - magnitude
        var result = Bytes(repeating: 0, count: n + 1)
        result[0] = typeCode
        var v = encoded
        for i in stride(from: n, through: 1, by: -1) {
            result[i] = UInt8(v & 0xFF)
            v >>= 8
        }
        return result
    }

    private static func byteCount(for value: UInt64) -> Int {
        for (i, limit) in sizeLimits.enumerated() {
            if value <= limit {
                return i + 1
            }
        }
        return 8
    }
}

// MARK: - Int

extension Int: TupleElement {
    public func encodeTuple() -> Bytes {
        Int64(self).encodeTuple()
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Int {
        let value = try Int64.decodeTuple(from: bytes, at: &offset)
        guard let result = Int(exactly: value) else { throw TupleError.integerOverflow }
        return result
    }
}

// MARK: - Int32

extension Int32: TupleElement {
    public func encodeTuple() -> Bytes {
        Int64(self).encodeTuple()
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Int32 {
        let value = try Int64.decodeTuple(from: bytes, at: &offset)
        guard let result = Int32(exactly: value) else { throw TupleError.integerOverflow }
        return result
    }
}

// MARK: - UInt64

extension UInt64: TupleElement {
    public func encodeTuple() -> Bytes {
        if self == 0 {
            return [TupleTypeCode.intZero.rawValue]
        }
        let n = byteCount(for: self)
        let typeCode = TupleTypeCode.intZero.rawValue + UInt8(n)
        var result = Bytes(repeating: 0, count: n + 1)
        result[0] = typeCode
        var v = self
        for i in stride(from: n, through: 1, by: -1) {
            result[i] = UInt8(v & 0xFF)
            v >>= 8
        }
        return result
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> UInt64 {
        guard offset > 0 else { throw TupleError.unexpectedEndOfData }
        let typeCode = bytes[offset - 1]
        let intZero = TupleTypeCode.intZero.rawValue

        if typeCode == intZero {
            return 0
        }

        guard typeCode > intZero && typeCode <= 0x1D else {
            throw TupleError.invalidTypeCode(typeCode)
        }

        let n = Int(typeCode - intZero)
        guard offset + n <= bytes.count else { throw TupleError.unexpectedEndOfData }
        var value: UInt64 = 0
        for i in 0..<n {
            value = (value << 8) | UInt64(bytes[offset + i])
        }
        offset += n
        return value
    }

    private func byteCount(for value: UInt64) -> Int {
        for (i, limit) in sizeLimits.enumerated() {
            if value <= limit {
                return i + 1
            }
        }
        return 8
    }
}
