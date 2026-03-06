import StorageKit
import Foundation
import Synchronization

/// StorageKit.Transaction implementation for SQLite.
///
/// Write operations (`setValue`/`clear`/`clearRange`) are non-throwing per the protocol,
/// so writes are buffered and flushed on commit.
/// `getValue` checks the buffer in reverse order to provide read-your-writes semantics.
/// `getRange` flushes the buffer before executing the SQL query.
///
/// ## Nested Transaction Support
///
/// When `lock` is nil, this transaction is a nested child created by
/// `TransactionContext` detection. In this mode:
/// - `commit()` flushes the write buffer but does not execute COMMIT or release a lock
/// - `cancel()` discards the write buffer but does not execute ROLLBACK or release a lock
/// - The parent transaction controls the actual SQLite transaction lifecycle
public final class SQLiteStorageTransaction: Transaction, Sendable {

    public typealias RangeResult = KeyValueRangeResult

    /// Externally synchronized by engine's transaction lock.
    nonisolated(unsafe) let connection: SQLiteConnection
    private let lock: NSLock?

    private struct MutableState: Sendable {
        var writeBuffer: [WriteOp] = []
        var committed = false
        var cancelled = false
    }
    private let _state: Mutex<MutableState>

    private enum WriteOp: Sendable {
        case set(key: Bytes, value: Bytes)
        case clear(key: Bytes)
        case clearRange(begin: Bytes, end: Bytes)
    }

    init(connection: SQLiteConnection, lock: NSLock?) {
        self.connection = connection
        self.lock = lock
        self._state = Mutex(MutableState())
    }

    // MARK: - Read

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        let writeBuffer = try _state.withLock { state in
            guard !state.cancelled else {
                throw StorageError.invalidOperation("Transaction cancelled")
            }
            return state.writeBuffer
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
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueRangeResult {
        let cancelled = _state.withLock { $0.cancelled }
        guard !cancelled else {
            return KeyValueRangeResult(error: StorageError.invalidOperation("Transaction cancelled"))
        }

        // Resolve KeySelectors to SQL boundary conditions.
        //
        // Begin and end selectors have different semantics:
        // - Begin: the resolved position is the first key IN the range.
        // - End: the resolved position is the first key PAST the range.
        //
        // For the common pattern getRange(from: .firstGreaterOrEqual(A), to: .firstGreaterOrEqual(B)):
        //   begin: key >= A (inclusive)
        //   end:   key < B  (exclusive — B is the first key past the range)
        let (beginKey, beginOp) = Self.resolveBeginForSQL(begin)
        let (endKey, endOp) = Self.resolveEndForSQL(end)

        do {
            try flushWriteBuffer()
            let results = try connection.getRangeWithOps(
                begin: beginKey, beginOp: beginOp,
                end: endKey, endOp: endOp,
                limit: limit, reverse: reverse
            )
            return KeyValueRangeResult(results)
        } catch {
            return KeyValueRangeResult(error: error)
        }
    }

    /// Resolve a KeySelector used as the BEGIN of a range scan.
    ///
    /// The begin selector determines the first key included in the result.
    /// - firstGreaterOrEqual(k): key >= k
    /// - firstGreaterThan(k):    key > k
    /// - lastLessOrEqual(k):     key >= k (approximation: include k as start)
    /// - lastLessThan(k):        key >= k (approximation: safe over-inclusion)
    private static func resolveBeginForSQL(_ selector: KeySelector) -> (key: Bytes, op: String) {
        switch (selector.orEqual, selector.offset) {
        case (false, 1):
            // firstGreaterOrEqual: key >= selector.key
            return (selector.key, ">=")
        case (true, 1):
            // firstGreaterThan: key > selector.key
            return (selector.key, ">")
        default:
            // Non-standard: approximate as >= (safe over-inclusion)
            return (selector.key, ">=")
        }
    }

    /// Resolve a KeySelector used as the END of a range scan.
    ///
    /// The end selector determines the first key PAST the result (exclusive boundary).
    /// - firstGreaterOrEqual(k): key < k  (the resolved key is past the range)
    /// - firstGreaterThan(k):    key <= k  (keys equal to k are still in range)
    /// - lastLessOrEqual(k):     key <= k  (include k itself)
    /// - lastLessThan(k):        key < k   (exclude k)
    private static func resolveEndForSQL(_ selector: KeySelector) -> (key: Bytes, op: String) {
        switch (selector.orEqual, selector.offset) {
        case (false, 1):
            // firstGreaterOrEqual: the resolved position is >= k, so range is < k
            return (selector.key, "<")
        case (true, 1):
            // firstGreaterThan: the resolved position is > k, so range includes k → <= k
            return (selector.key, "<=")
        case (true, 0):
            // lastLessOrEqual: the resolved position is <= k, which is the last key in range
            return (selector.key, "<=")
        case (false, 0):
            // lastLessThan: the resolved position is < k, so range is < k
            return (selector.key, "<")
        default:
            // Non-standard: approximate as < (safe under-inclusion for end)
            return (selector.key, "<")
        }
    }

    // MARK: - Write

    public func setValue(_ value: Bytes, for key: Bytes) {
        _state.withLock { state in
            guard !state.cancelled else { return }
            state.writeBuffer.append(.set(key: key, value: value))
        }
    }

    public func clear(key: Bytes) {
        _state.withLock { state in
            guard !state.cancelled else { return }
            state.writeBuffer.append(.clear(key: key))
        }
    }

    public func clearRange(beginKey: Bytes, endKey: Bytes) {
        _state.withLock { state in
            guard !state.cancelled else { return }
            state.writeBuffer.append(.clearRange(begin: beginKey, end: endKey))
        }
    }

    // MARK: - Transaction Management

    public func commit() async throws {
        let shouldProceed = try _state.withLock { state -> Bool in
            guard !state.cancelled else {
                throw StorageError.invalidOperation("Transaction cancelled")
            }
            return !state.committed
        }
        guard shouldProceed else { return }

        if lock != nil {
            // Top-level transaction: flush buffer, COMMIT, release lock
            defer { releaseLock() }
            do {
                try flushWriteBuffer()
                try connection.execute("COMMIT")
                _state.withLock { $0.committed = true }
            } catch {
                try? connection.execute("ROLLBACK")
                throw error
            }
        } else {
            // Nested transaction: flush buffer only (writes become part of parent TX)
            try flushWriteBuffer()
            _state.withLock { $0.committed = true }
        }
    }

    public func cancel() {
        let shouldCancel = _state.withLock { state -> Bool in
            guard !state.committed, !state.cancelled else { return false }
            state.cancelled = true
            state.writeBuffer.removeAll()
            return true
        }
        guard shouldCancel else { return }

        if lock != nil {
            // Top-level transaction: ROLLBACK and release lock
            try? connection.execute("ROLLBACK")
            releaseLock()
        }
        // Nested transaction: just discard buffer (parent controls ROLLBACK)
    }

    // MARK: - Internal

    private nonisolated func releaseLock() {
        lock?.unlock()
    }

    private func flushWriteBuffer() throws {
        let ops = _state.withLock { $0.writeBuffer }
        for op in ops {
            switch op {
            case .set(let key, let value):
                try connection.insertOrReplace(key: key, value: value)
            case .clear(let key):
                try connection.delete(key: key)
            case .clearRange(let begin, let end):
                try connection.deleteRange(begin: begin, end: end)
            }
        }
        _state.withLock { $0.writeBuffer.removeAll() }
    }
}
