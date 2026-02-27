/// トランザクションオプション
///
/// FDB のトランザクションオプションを抽象化した型。
/// 非 FDB バックエンドではデフォルトで無視される。
public enum TransactionOption: Sendable {
    /// トランザクションのタイムアウト（ミリ秒）
    case timeout(milliseconds: Int)
    /// バッチ優先度（バックグラウンド処理用）
    case priorityBatch
    /// システム即時優先度（メタデータ操作用）
    case prioritySystemImmediate
    /// 読み取り低優先度
    case readPriorityLow
    /// 読み取り高優先度
    case readPriorityHigh
    /// システムキーへのアクセスを許可
    case accessSystemKeys
    /// サーバーサイドキャッシュを無効化
    case readServerSideCacheDisable
}
