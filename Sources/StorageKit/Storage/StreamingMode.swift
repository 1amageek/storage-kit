/// Range scan のストリーミングモード
///
/// FDB の StreamingMode と同一セマンティクス。
/// バックエンドがバッチサイズの最適化ヒントとして使用する。
public enum StreamingMode: Int32, Sendable {
    /// 全結果を一括転送（小規模範囲向け）
    case wantAll = -2
    /// デフォルト: バランスの取れたストリーミング
    case iterator = -1
    /// 指定行数のみ取得（limit と併用）
    case exact = 0
    /// 小バッチ
    case small = 1
    /// 中バッチ
    case medium = 2
    /// 大バッチ
    case large = 3
    /// 超大バッチ（高スループット向け）
    case serial = 4
}
