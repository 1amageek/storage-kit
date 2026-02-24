/// FDB Tuple Layer バイナリエンコーディング仕様に完全準拠した型コード
///
/// Reference: https://github.com/apple/foundationdb/blob/main/design/tuple.md
public enum TupleTypeCode: UInt8, Sendable {
    case null           = 0x00
    case bytes          = 0x01
    case string         = 0x02
    case nested         = 0x05
    // 0x0B - 0x13: negative integers (variable length)
    case negativeInt8   = 0x0C
    case negativeInt7   = 0x0D
    case negativeInt6   = 0x0E
    case negativeInt5   = 0x0F
    case negativeInt4   = 0x10
    case negativeInt3   = 0x11
    case negativeInt2   = 0x12
    case negativeInt1   = 0x13
    case intZero        = 0x14
    case positiveInt1   = 0x15
    case positiveInt2   = 0x16
    case positiveInt3   = 0x17
    case positiveInt4   = 0x18
    case positiveInt5   = 0x19
    case positiveInt6   = 0x1A
    case positiveInt7   = 0x1B
    case positiveInt8   = 0x1C
    // 0x1D is positiveInt9 (for full UInt64 range)
    case float          = 0x20
    case double         = 0x21
    case boolFalse      = 0x26
    case boolTrue       = 0x27
    case uuid           = 0x30
}

/// StorageKit のバイト列型（FDB.Bytes と同等）
public typealias Bytes = [UInt8]

/// strinc アルゴリズム: バイト列の辞書順で次の接頭辞を返す
///
/// 末尾の 0xFF バイトを除去し、最後のバイトをインクリメントする。
/// Range scan の終了キー生成に使用。
///
/// Reference: FoundationDB strinc specification
public func strinc(_ bytes: Bytes) throws -> Bytes {
    var result = bytes
    while result.last == 0xFF {
        result.removeLast()
    }
    guard !result.isEmpty else {
        throw TupleError.cannotIncrementKey
    }
    result[result.count - 1] &+= 1
    return result
}

/// Tuple Layer のエンコード/デコード用プロトコル
///
/// FDB Tuple Layer のバイナリフォーマットに従い、各型をバイト列に変換・復元する。
/// エンコード結果は辞書順 (lexicographic order) が値の論理順と一致する。
public protocol TupleElement: Sendable, Hashable {
    /// この値を FDB Tuple Layer 形式のバイト列にエンコード
    func encodeTuple() -> Bytes

    /// バイト列の指定位置からこの型の値をデコード
    ///
    /// - Parameters:
    ///   - bytes: エンコード済みバイト列
    ///   - offset: 読み取り開始位置（型コードの次のバイト）。デコード後に更新される。
    static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Self
}

/// Tuple Layer のエラー型
public enum TupleError: Error, Sendable {
    case unexpectedEndOfData
    case invalidTypeCode(UInt8)
    case integerOverflow
    case invalidUTF8
    case invalidNullEscape
    case cannotIncrementKey
    case prefixMismatch
}

/// 各型のバイト数上限テーブル（整数の可変長エンコードで使用）
///
/// sizeLimits[n] = 2^(8*(n+1)) - 1
/// n バイトで表現可能な最大値を返す
package let sizeLimits: [UInt64] = [
    0xFF,                       // 1 byte
    0xFFFF,                     // 2 bytes
    0xFFFF_FF,                  // 3 bytes
    0xFFFF_FFFF,                // 4 bytes
    0xFFFF_FFFF_FF,             // 5 bytes
    0xFFFF_FFFF_FFFF,           // 6 bytes
    0xFFFF_FFFF_FFFF_FF,        // 7 bytes
    0xFFFF_FFFF_FFFF_FFFF,      // 8 bytes
]
