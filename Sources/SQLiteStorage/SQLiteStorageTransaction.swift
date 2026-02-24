import StorageKit
import Foundation

/// SQLite トランザクションの StorageKit.Transaction 実装
///
/// `setValue`/`clear`/`clearRange` は non-throwing のため、書き込みはバッファし commit 時に一括適用する。
/// `getValue` はバッファを逆順チェックして read-your-writes を実現。
/// `getRange` はバッファを flush してから SQL クエリを実行。
public final class SQLiteStorageTransaction: Transaction, @unchecked Sendable {

    private let connection: SQLiteConnection
    private let lock: NSLock
    private var writeBuffer: [WriteOp] = []
    private var committed = false
    private var cancelled = false

    private enum WriteOp {
        case set(key: Bytes, value: Bytes)
        case clear(key: Bytes)
        case clearRange(begin: Bytes, end: Bytes)
    }

    init(connection: SQLiteConnection, lock: NSLock) {
        self.connection = connection
        self.lock = lock
    }

    // MARK: - Read

    public func getValue(for key: Bytes) async throws -> Bytes? {
        guard !cancelled else {
            throw StorageError.invalidOperation("Transaction cancelled")
        }

        // バッファを逆順チェック（read-your-writes）
        for op in writeBuffer.reversed() {
            switch op {
            case .set(let k, let v) where k == key:
                return v
            case .clear(let k) where k == key:
                return nil
            case .clearRange(let b, let e)
                where compareBytes(key, b) >= 0 && compareBytes(key, e) < 0:
                return nil
            default:
                continue
            }
        }

        return try connection.get(key: key)
    }

    public func getRange(
        begin: Bytes,
        end: Bytes,
        limit: Int,
        reverse: Bool
    ) async throws -> KeyValueSequence {
        guard !cancelled else {
            throw StorageError.invalidOperation("Transaction cancelled")
        }

        // バッファ内の操作を SQLite に適用してから range query
        try flushWriteBuffer()
        let results = try connection.getRange(
            begin: begin, end: end, limit: limit, reverse: reverse
        )
        return KeyValueSequence(results)
    }

    // MARK: - Write

    public func setValue(_ value: Bytes, for key: Bytes) {
        guard !cancelled else { return }
        writeBuffer.append(.set(key: key, value: value))
    }

    public func clear(key: Bytes) {
        guard !cancelled else { return }
        writeBuffer.append(.clear(key: key))
    }

    public func clearRange(begin: Bytes, end: Bytes) {
        guard !cancelled else { return }
        writeBuffer.append(.clearRange(begin: begin, end: end))
    }

    // MARK: - Transaction Management

    public func commit() async throws {
        guard !cancelled else {
            throw StorageError.invalidOperation("Transaction cancelled")
        }
        guard !committed else { return }
        do {
            try flushWriteBuffer()
            try connection.execute("COMMIT")
            committed = true
            releaseLock()
        } catch {
            try? connection.execute("ROLLBACK")
            releaseLock()
            throw error
        }
    }

    public func cancel() {
        guard !committed, !cancelled else { return }
        cancelled = true
        writeBuffer.removeAll()
        try? connection.execute("ROLLBACK")
        releaseLock()
    }

    // MARK: - Internal

    /// NSLock.unlock() は async context から呼べないため、
    /// nonisolated な同期関数経由で呼び出す
    private nonisolated func releaseLock() {
        lock.unlock()
    }

    private func flushWriteBuffer() throws {
        for op in writeBuffer {
            switch op {
            case .set(let key, let value):
                try connection.insertOrReplace(key: key, value: value)
            case .clear(let key):
                try connection.delete(key: key)
            case .clearRange(let begin, let end):
                try connection.deleteRange(begin: begin, end: end)
            }
        }
        writeBuffer.removeAll()
    }
}

// MARK: - Byte Comparison

/// バイト列の辞書順比較
private func compareBytes(_ lhs: Bytes, _ rhs: Bytes) -> Int {
    let minLen = min(lhs.count, rhs.count)
    for i in 0..<minLen {
        if lhs[i] != rhs[i] {
            return Int(lhs[i]) - Int(rhs[i])
        }
    }
    return lhs.count - rhs.count
}
