/// KV ストレージバックエンドの抽象プロトコル
///
/// 各バックエンド（FoundationDB, SQLite, InMemory）はこのプロトコルに準拠する。
/// トランザクション作成とリトライロジック付き実行を提供する。
public protocol StorageEngine: Sendable {
    associatedtype TransactionType: Transaction

    /// 新しいトランザクションを作成
    func createTransaction() throws -> TransactionType

    /// リトライロジック付きトランザクション実行
    ///
    /// トランザクションコンフリクト時は自動リトライする。
    /// クロージャが正常完了した場合、自動的にコミットされる。
    func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T

    /// 階層的名前空間管理サービス
    ///
    /// FDB: DirectoryLayer（動的プレフィックス割り当て）
    /// 非 FDB: StaticDirectoryService（パスを Tuple エンコードで直接変換）
    var directoryService: any DirectoryService { get }
}

extension StorageEngine {
    /// デフォルト: StaticDirectoryService（パスを Tuple エンコードで直接 Subspace に変換）
    public var directoryService: any DirectoryService { StaticDirectoryService() }
}
