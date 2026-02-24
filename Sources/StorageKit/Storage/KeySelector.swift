/// FDB KeySelector 互換のキー選択構造体
///
/// キーの正確な位置ではなく、相対的な位置（以上、より大きい等）でキーを指定する。
/// Range scan の開始・終了位置の指定に使用。
///
/// Reference: https://apple.github.io/foundationdb/developer-guide.html#key-selectors
public struct KeySelector: Sendable, Hashable {

    /// 参照キー
    public let key: Bytes

    /// true の場合、key 自身を含む
    public let orEqual: Bool

    /// 結果位置からのオフセット（正: 後方、負: 前方）
    public let offset: Int

    public init(key: Bytes, orEqual: Bool, offset: Int) {
        self.key = key
        self.orEqual = orEqual
        self.offset = offset
    }

    /// key 以上の最初のキー
    public static func firstGreaterOrEqual(_ key: Bytes) -> KeySelector {
        KeySelector(key: key, orEqual: true, offset: 0)
    }

    /// key より大きい最初のキー
    public static func firstGreaterThan(_ key: Bytes) -> KeySelector {
        KeySelector(key: key, orEqual: true, offset: 1)
    }

    /// key 以下の最後のキー
    public static func lastLessOrEqual(_ key: Bytes) -> KeySelector {
        KeySelector(key: key, orEqual: true, offset: 0)
    }

    /// key より小さい最後のキー
    public static func lastLessThan(_ key: Bytes) -> KeySelector {
        KeySelector(key: key, orEqual: false, offset: 0)
    }
}
