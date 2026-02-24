import Foundation

/// Tuple ベースのキー空間プレフィックス管理
///
/// Subspace は共通プレフィックスを持つキー群を管理する。
/// FoundationDB の Subspace と同一セマンティクス。
///
/// ## 使用例
/// ```swift
/// let root = Subspace(prefix: [])
/// let users = root.subspace("users")
/// let user42 = users.subspace(Int64(42))
/// let key = user42.pack(Tuple("email"))
/// ```
public struct Subspace: Sendable, Hashable, Equatable {

    /// このサブスペースのプレフィックスバイト列
    public let prefix: Bytes

    // MARK: - Initializers

    public init(prefix: Bytes = []) {
        self.prefix = prefix
    }

    /// Tuple のエンコード結果をプレフィックスとして使用
    public init(_ tuple: Tuple) {
        self.prefix = tuple.pack()
    }

    /// 可変長引数で Tuple 要素からプレフィックスを構築
    public init(_ elements: any TupleElement...) {
        self.prefix = Tuple(elements).pack()
    }

    // MARK: - Subspace nesting

    /// 追加要素でネストしたサブスペースを作成
    public func subspace(_ elements: any TupleElement...) -> Subspace {
        Subspace(prefix: prefix + Tuple(elements).pack())
    }

    /// subscript でネスト（subspace のエイリアス）
    public subscript(_ elements: any TupleElement...) -> Subspace {
        Subspace(prefix: prefix + Tuple(elements).pack())
    }

    // MARK: - Pack / Unpack

    /// Tuple をこのサブスペースのプレフィックス付きでエンコード
    public func pack(_ tuple: Tuple) -> Bytes {
        prefix + tuple.pack()
    }

    /// プレフィックスを除去して Tuple をデコード
    public func unpack(_ key: Bytes) throws -> Tuple {
        guard contains(key) else {
            throw TupleError.prefixMismatch
        }
        let remaining = Array(key[prefix.count...])
        let elements = try Tuple.unpack(from: remaining)
        return Tuple(elements)
    }

    // MARK: - Contains

    /// キーがこのサブスペースに含まれるか判定
    public func contains(_ key: Bytes) -> Bool {
        guard key.count >= prefix.count else { return false }
        return key.prefix(prefix.count) == prefix[...]
    }

    // MARK: - Range

    /// このサブスペースの全キー範囲を返す [prefix + 0x00, strinc(prefix))
    ///
    /// prefix 自体は含まない。prefix の後に少なくとも 1 バイトの追加データを持つキーのみ。
    public func range() -> (begin: Bytes, end: Bytes) {
        let begin = prefix + [0x00]
        let end: Bytes
        if prefix.isEmpty {
            end = [0xFF]
        } else {
            end = (try? strinc(prefix)) ?? (prefix + [0xFF])
        }
        return (begin: begin, end: end)
    }

    /// Tuple 範囲からキー範囲を生成
    public func range(from start: Tuple, to end: Tuple) -> (begin: Bytes, end: Bytes) {
        let beginKey = prefix + start.pack()
        let endKey = prefix + end.pack()
        return (begin: beginKey, end: endKey)
    }

    /// プレフィックスベースの範囲 [prefix, strinc(prefix))
    ///
    /// prefix 自体も含む全てのキーを対象にする。
    public func prefixRange() throws -> (begin: Bytes, end: Bytes) {
        let end = try strinc(prefix)
        return (begin: prefix, end: end)
    }
}
