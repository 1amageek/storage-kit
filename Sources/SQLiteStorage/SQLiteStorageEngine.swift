import StorageKit
import Foundation

/// SQLite バックエンドの StorageEngine 実装
///
/// `WITHOUT ROWID` テーブルで BLOB 主キーの B-tree 直接格納を実現。
/// SQLite は single-writer のため、`NSLock` でトランザクションを直列化する。
///
/// ## 使用例
/// ```swift
/// // ファイルベース
/// let engine = try SQLiteStorageEngine(path: "/path/to/db.sqlite")
///
/// // インメモリ（テスト用）
/// let engine = try SQLiteStorageEngine()
/// ```
public final class SQLiteStorageEngine: StorageEngine, @unchecked Sendable {
    public typealias TransactionType = SQLiteStorageTransaction

    private let lock = NSLock()
    private var connection: SQLiteConnection?

    /// ファイルベースのデータベース
    public init(path: String) throws {
        let conn = try SQLiteConnection(path: path)
        try conn.initialize()
        self.connection = conn
    }

    /// インメモリデータベース（テスト用）
    public init() throws {
        let conn = try SQLiteConnection(path: ":memory:")
        try conn.initialize()
        self.connection = conn
    }

    public func createTransaction() throws -> SQLiteStorageTransaction {
        lock.lock()
        guard let conn = connection else {
            lock.unlock()
            throw StorageError.invalidOperation("Database closed")
        }
        do {
            try conn.execute("BEGIN IMMEDIATE")
        } catch {
            lock.unlock()
            throw error
        }
        return SQLiteStorageTransaction(connection: conn, lock: lock)
    }

    public func withTransaction<T: Sendable>(
        _ operation: (any Transaction) async throws -> T
    ) async throws -> T {
        let tx = try createTransaction()
        do {
            let result = try await operation(tx)
            try await tx.commit()
            return result
        } catch {
            tx.cancel()
            throw error
        }
    }

    /// データベースを閉じる
    public func close() {
        lock.lock()
        defer { lock.unlock() }
        connection?.close()
        connection = nil
    }
}
