import StorageKit
import Foundation

/// StorageKit.Transaction implementation for SQLite.
///
/// Write operations (`setValue`/`clear`/`clearRange`) are non-throwing per the protocol,
/// so writes are buffered and flushed on commit.
/// `getValue` checks the buffer in reverse order to provide read-your-writes semantics.
/// `getRange` flushes the buffer before executing the SQL query.
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

        // Check write buffer in reverse order (read-your-writes)
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

        // Flush buffered writes to SQLite before executing range query
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

    /// NSLock.unlock() cannot be called from async contexts,
    /// so we route through a nonisolated synchronous function.
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

/// Lexicographic comparison of byte arrays.
private func compareBytes(_ lhs: Bytes, _ rhs: Bytes) -> Int {
    let minLen = min(lhs.count, rhs.count)
    for i in 0..<minLen {
        if lhs[i] != rhs[i] {
            return Int(lhs[i]) - Int(rhs[i])
        }
    }
    return lhs.count - rhs.count
}
