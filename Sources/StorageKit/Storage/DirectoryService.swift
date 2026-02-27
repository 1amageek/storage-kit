/// 階層的名前空間管理サービス
///
/// FDB の DirectoryLayer に相当する機能を抽象化。
/// パスベースの名前空間を Subspace に変換する。
public protocol DirectoryService: Sendable {
    /// パスに対応する Subspace を作成または開く
    ///
    /// - Parameter path: 階層パス（例: ["User", "email_index"]）
    /// - Returns: パスに対応する Subspace
    func createOrOpen(path: [String]) async throws -> Subspace

    /// パス配下のサブディレクトリ名を列挙する
    func list(path: [String]) async throws -> [String]

    /// パスに対応するディレクトリを削除する
    func remove(path: [String]) async throws

    /// パスに対応するディレクトリが存在するか
    func exists(path: [String]) async throws -> Bool
}

extension DirectoryService {
    public func list(path: [String]) async throws -> [String] { [] }
    public func remove(path: [String]) async throws {}
    public func exists(path: [String]) async throws -> Bool { false }
}

/// 静的ディレクトリサービス（非 FDB バックエンド用）
///
/// パスを Tuple エンコードして直接 Subspace に変換する。
/// FDB DirectoryLayer のような動的プレフィックス割り当ては行わない。
public struct StaticDirectoryService: DirectoryService, Sendable {
    public init() {}

    public func createOrOpen(path: [String]) async throws -> Subspace {
        Subspace(Tuple(path.map { $0 as any TupleElement }))
    }
}
