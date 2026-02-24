/// StorageEngine のエラー型
public enum StorageError: Error, Sendable {
    /// トランザクションコンフリクト（リトライ可能）
    case transactionConflict

    /// トランザクションが古い（リトライ可能）
    case transactionTooOld

    /// キーが見つからない
    case keyNotFound

    /// 無効な操作
    case invalidOperation(String)

    /// バックエンド固有のエラー
    case backendError(String)
}
