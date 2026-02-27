import StorageKit
import Foundation

/// SQLite 用の getRange 結果型
///
/// 配列ベースの AsyncSequence。ゼロコピーで結果を返す。
public struct SQLiteRangeResult: AsyncSequence, Sendable {
    public typealias Element = (Bytes, Bytes)

    private let results: [(key: Bytes, value: Bytes)]
    private let error: (any Error)?

    init(_ results: [(key: Bytes, value: Bytes)]) {
        self.results = results
        self.error = nil
    }

    init(error: any Error) {
        self.results = []
        self.error = error
    }

    public func makeAsyncIterator() -> Iterator {
        Iterator(results: results, error: error)
    }

    public struct Iterator: AsyncIteratorProtocol {
        private let results: [(key: Bytes, value: Bytes)]
        private let error: (any Error)?
        private var index: Int = 0

        init(results: [(key: Bytes, value: Bytes)], error: (any Error)?) {
            self.results = results
            self.error = error
        }

        public mutating func next() async throws -> (Bytes, Bytes)? {
            if let error { throw error }
            guard index < results.count else { return nil }
            let entry = results[index]
            index += 1
            return (entry.key, entry.value)
        }
    }
}

/// StorageKit.Transaction implementation for SQLite.
///
/// Write operations (`setValue`/`clear`/`clearRange`) are non-throwing per the protocol,
/// so writes are buffered and flushed on commit.
/// `getValue` checks the buffer in reverse order to provide read-your-writes semantics.
/// `getRange` flushes the buffer before executing the SQL query.
public final class SQLiteStorageTransaction: Transaction, @unchecked Sendable {

    public typealias RangeResult = SQLiteRangeResult

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

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
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
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> SQLiteRangeResult {
        guard !cancelled else {
            return SQLiteRangeResult(error: StorageError.invalidOperation("Transaction cancelled"))
        }

        // Resolve KeySelectors to SQL boundary conditions.
        //
        // For the common case (firstGreaterOrEqual), this produces:
        //   begin: key >= beginKey (inclusive)
        //   end:   key < endKey   (exclusive)
        //
        // For firstGreaterThan (orEqual=true, offset=1):
        //   begin: key > beginKey (exclusive)
        //   end:   key > endKey → effectively key > endKey
        //
        // For complex offsets, we fall back to firstGreaterOrEqual semantics
        // since SQLite doesn't have FDB's multi-step resolution.
        let (beginKey, beginInclusive) = Self.resolveForSQL(begin)
        let (endKey, endInclusive) = Self.resolveForSQL(end)

        do {
            try flushWriteBuffer()
            let results = try connection.getRange(
                begin: beginKey, beginInclusive: beginInclusive,
                end: endKey, endInclusive: endInclusive,
                limit: limit, reverse: reverse
            )
            return SQLiteRangeResult(results)
        } catch {
            return SQLiteRangeResult(error: error)
        }
    }

    /// Resolve a KeySelector to a SQL boundary condition.
    ///
    /// Returns the key and whether the boundary is inclusive.
    /// Handles the four standard factory patterns:
    /// - firstGreaterOrEqual (orEqual=false, offset=1) → key >= k
    /// - firstGreaterThan    (orEqual=true,  offset=1) → key > k
    /// - lastLessOrEqual     (orEqual=true,  offset=0) → key <= k
    /// - lastLessThan        (orEqual=false, offset=0) → key < k
    private static func resolveForSQL(_ selector: KeySelector) -> (key: Bytes, inclusive: Bool) {
        switch (selector.orEqual, selector.offset) {
        case (false, 1):
            // firstGreaterOrEqual: key >= selector.key
            return (selector.key, true)
        case (true, 1):
            // firstGreaterThan: key > selector.key
            return (selector.key, false)
        case (true, 0):
            // lastLessOrEqual: key <= selector.key
            return (selector.key, true)
        case (false, 0):
            // lastLessThan: key < selector.key
            return (selector.key, false)
        default:
            // Non-standard offset: approximate as >= (safe default for range scans)
            return (selector.key, true)
        }
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

    public func clearRange(beginKey: Bytes, endKey: Bytes) {
        guard !cancelled else { return }
        writeBuffer.append(.clearRange(begin: beginKey, end: endKey))
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

private func compareBytes(_ lhs: Bytes, _ rhs: Bytes) -> Int {
    let minLen = min(lhs.count, rhs.count)
    for i in 0..<minLen {
        if lhs[i] != rhs[i] {
            return Int(lhs[i]) - Int(rhs[i])
        }
    }
    return lhs.count - rhs.count
}
