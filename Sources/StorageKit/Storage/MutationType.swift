/// アトミック操作の種別
///
/// FDB のアトミックミューテーション型を抽象化。
public enum MutationType: Sendable {
    /// 加算（リトルエンディアンの整数バイト列を加算）
    case add
    /// バージョンスタンプ付きキーを設定
    case setVersionstampedKey
    /// バージョンスタンプ付き値を設定
    case setVersionstampedValue
    /// ビット OR
    case bitOr
    /// ビット AND
    case bitAnd
    /// ビット XOR
    case bitXor
    /// 最大値を設定（バイト列の辞書順比較）
    case max
    /// 最小値を設定（バイト列の辞書順比較）
    case min
    /// 比較して書き込み
    case compareAndClear
}
