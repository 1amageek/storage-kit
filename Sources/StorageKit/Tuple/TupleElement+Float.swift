import Foundation

// MARK: - Float

extension Float: TupleElement {
    /// IEEE 754 big-endian エンコード
    ///
    /// 正の値: 全ビット反転 → 辞書順が数値順と一致
    /// 負の値: 符号ビットのみ反転 → 負の辞書順が正しくなる
    ///
    /// FDB 仕様: 正の場合は符号ビット反転、負の場合は全ビット反転
    public func encodeTuple() -> Bytes {
        var bits = self.bitPattern.bigEndian
        var rawBytes = withUnsafeBytes(of: &bits) { Array($0) }
        if self.sign == .minus {
            // 負の値（-0.0 含む）: 全ビット反転
            for i in 0..<rawBytes.count {
                rawBytes[i] = ~rawBytes[i]
            }
        } else {
            // 正の値（+0.0, +Inf, NaN 含む）: 符号ビットのみ反転
            rawBytes[0] ^= 0x80
        }
        return [TupleTypeCode.float.rawValue] + rawBytes
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Float {
        guard offset + 4 <= bytes.count else { throw TupleError.unexpectedEndOfData }
        var rawBytes = Array(bytes[offset..<(offset + 4)])
        offset += 4

        if rawBytes[0] & 0x80 != 0 {
            // 正の値: 符号ビットのみ戻す
            rawBytes[0] ^= 0x80
        } else {
            // 負の値: 全ビット反転を戻す
            for i in 0..<rawBytes.count {
                rawBytes[i] = ~rawBytes[i]
            }
        }
        let bits = rawBytes.withUnsafeBytes { $0.load(as: UInt32.self) }
        return Float(bitPattern: UInt32(bigEndian: bits))
    }
}

// MARK: - Double

extension Double: TupleElement {
    /// IEEE 754 big-endian エンコード（Float と同じアルゴリズム、8 バイト）
    public func encodeTuple() -> Bytes {
        var bits = self.bitPattern.bigEndian
        var rawBytes = withUnsafeBytes(of: &bits) { Array($0) }
        if self.sign == .minus {
            for i in 0..<rawBytes.count {
                rawBytes[i] = ~rawBytes[i]
            }
        } else {
            rawBytes[0] ^= 0x80
        }
        return [TupleTypeCode.double.rawValue] + rawBytes
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Double {
        guard offset + 8 <= bytes.count else { throw TupleError.unexpectedEndOfData }
        var rawBytes = Array(bytes[offset..<(offset + 8)])
        offset += 8

        if rawBytes[0] & 0x80 != 0 {
            rawBytes[0] ^= 0x80
        } else {
            for i in 0..<rawBytes.count {
                rawBytes[i] = ~rawBytes[i]
            }
        }
        let bits = rawBytes.withUnsafeBytes { $0.load(as: UInt64.self) }
        return Double(bitPattern: UInt64(bigEndian: bits))
    }
}
