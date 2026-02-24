import Foundation

/// FDB Tuple Layer 互換の複合キー構造体
///
/// 複数の型付き値をバイト列にエンコードし、辞書順 (lexicographic order) が
/// 各要素の論理順と一致するバイナリフォーマットを生成する。
///
/// ## 使用例
/// ```swift
/// let tuple = Tuple("users", Int64(42), "profile")
/// let packed = tuple.pack()
/// let unpacked = try Tuple.unpack(from: packed)
/// ```
///
/// ## 等値比較
/// エンコード済みバイト列で比較する（FDB セマンティクス準拠）:
/// - +0.0 ≠ -0.0 (異なるビットパターン)
/// - NaN == NaN (同じビットパターン)
public struct Tuple: Sendable, Hashable, Equatable {

    /// 型消去された要素を保持する内部ラッパー
    private struct AnyElement: Sendable, Hashable {
        let encoded: Bytes

        init(_ element: any TupleElement) {
            self.encoded = element.encodeTuple()
        }

        static func == (lhs: AnyElement, rhs: AnyElement) -> Bool {
            lhs.encoded == rhs.encoded
        }

        func hash(into hasher: inout Hasher) {
            hasher.combine(encoded)
        }
    }

    private let storage: [AnyElement]

    /// 要素数
    public var count: Int { storage.count }

    /// 空かどうか
    public var isEmpty: Bool { storage.isEmpty }

    // MARK: - Initializers

    public init(_ elements: any TupleElement...) {
        self.storage = elements.map { AnyElement($0) }
    }

    public init(_ elements: [any TupleElement]) {
        self.storage = elements.map { AnyElement($0) }
    }

    /// 内部用: AnyElement 配列から直接構築
    private init(storage: [AnyElement]) {
        self.storage = storage
    }

    // MARK: - Subscript

    /// インデックスアクセスで要素を取得（範囲外は nil）
    public subscript(index: Int) -> (any TupleElement)? {
        guard index >= 0 && index < storage.count else { return nil }
        let encoded = storage[index].encoded
        guard let first = encoded.first else { return nil }
        var offset = 1
        do {
            return try Self.decodeElement(typeCode: first, bytes: encoded, at: &offset)
        } catch {
            return nil
        }
    }

    // MARK: - Pack

    /// 全要素をバイト列にエンコード
    public func pack() -> Bytes {
        var result = Bytes()
        for element in storage {
            result.append(contentsOf: element.encoded)
        }
        return result
    }

    // MARK: - Unpack

    /// バイト列から要素配列をデコード
    ///
    /// FDB 実装と同じ単一パス方式: 各デコーダが inout offset を直接更新する。
    public static func unpack(from bytes: Bytes) throws -> [any TupleElement] {
        var elements: [any TupleElement] = []
        var offset = 0

        while offset < bytes.count {
            let typeCode = bytes[offset]
            offset += 1

            let element = try decodeElement(typeCode: typeCode, bytes: bytes, at: &offset)
            elements.append(element)
        }

        return elements
    }

    /// 型コードに基づいて 1 要素をデコードし、offset を更新する
    ///
    /// - Parameters:
    ///   - typeCode: 既に読み取り済みの型コードバイト
    ///   - bytes: 全バイト列
    ///   - offset: 型コードの次のバイト位置（デコード後に更新される）
    private static func decodeElement(typeCode: UInt8, bytes: Bytes, at offset: inout Int) throws -> any TupleElement {
        let intZero = TupleTypeCode.intZero.rawValue

        switch typeCode {
        case TupleTypeCode.null.rawValue:
            return TupleNil()

        case TupleTypeCode.bytes.rawValue:
            return try Bytes.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.string.rawValue:
            return try String.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.nested.rawValue:
            return try decodeNestedTuple(from: bytes, at: &offset)

        case intZero:
            return Int64(0)

        case 0x0B..<intZero, (intZero + 1)...0x1D:
            // Int64.decodeTuple は bytes[offset - 1] を型コードとして読む
            return try Int64.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.float.rawValue:
            return try Float.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.double.rawValue:
            return try Double.decodeTuple(from: bytes, at: &offset)

        case TupleTypeCode.boolFalse.rawValue:
            return false

        case TupleTypeCode.boolTrue.rawValue:
            return true

        case TupleTypeCode.uuid.rawValue:
            return try UUID.decodeTuple(from: bytes, at: &offset)

        default:
            throw TupleError.invalidTypeCode(typeCode)
        }
    }

    // MARK: - Nested Tuple

    /// Nested Tuple のエンコード（型コード 0x05）
    ///
    /// 内部要素をエンコードし、結果の 0x00 バイトを 0x00 0xFF にエスケープ、
    /// 最後に 0x00 終端を付加する。
    public func encodeNested() -> Bytes {
        var result: Bytes = [TupleTypeCode.nested.rawValue]
        for element in storage {
            let encoded = element.encoded
            for byte in encoded {
                if byte == 0x00 {
                    result.append(0x00)
                    result.append(0xFF)
                } else {
                    result.append(byte)
                }
            }
        }
        result.append(0x00) // terminator
        return result
    }

    /// Nested Tuple のデコード
    ///
    /// null-escape パターン (0x00 + 0xFF) を戻しながら内部バイトを収集し、
    /// 非エスケープの 0x00 で終端を検出する。depth 追跡は不要。
    private static func decodeNestedTuple(from bytes: Bytes, at offset: inout Int) throws -> Tuple {
        var innerBytes = Bytes()
        while offset < bytes.count {
            let byte = bytes[offset]
            offset += 1
            if byte == 0x00 {
                if offset < bytes.count && bytes[offset] == 0xFF {
                    innerBytes.append(0x00)
                    offset += 1
                } else {
                    // terminator found
                    break
                }
            } else {
                innerBytes.append(byte)
            }
        }
        let elements = try unpack(from: innerBytes)
        return Tuple(elements)
    }
}

// MARK: - TupleElement conformance for Tuple (nested)

extension Tuple: TupleElement {
    public func encodeTuple() -> Bytes {
        encodeNested()
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Tuple {
        try decodeNestedTuple(from: bytes, at: &offset)
    }
}

// MARK: - Append

extension Tuple {
    /// 要素を追加した新しい Tuple を返す
    public func appending(_ element: any TupleElement) -> Tuple {
        var newStorage = storage
        newStorage.append(AnyElement(element))
        return Tuple(storage: newStorage)
    }

    /// 別の Tuple の全要素を追加した新しい Tuple を返す
    public func appending(_ other: Tuple) -> Tuple {
        Tuple(storage: storage + other.storage)
    }
}
