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

    /// このエラーがリトライ可能かどうか
    public var isRetryable: Bool {
        switch self {
        case .transactionConflict, .transactionTooOld:
            return true
        case .keyNotFound, .invalidOperation, .backendError:
            return false
        }
    }
}
