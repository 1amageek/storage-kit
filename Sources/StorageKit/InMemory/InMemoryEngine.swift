import Synchronization

/// Lexicographic comparison of byte arrays.
///
/// - Returns: Negative: lhs < rhs, 0: lhs == rhs, Positive: lhs > rhs.
package func compareBytes(_ lhs: Bytes, _ rhs: Bytes) -> Int {
    let minLen = min(lhs.count, rhs.count)
    for i in 0..<minLen {
        if lhs[i] != rhs[i] {
            return Int(lhs[i]) - Int(rhs[i])
        }
    }
    return lhs.count - rhs.count
}

/// In-memory KV storage for testing and standalone client use.
///
/// Maintains lexicographic order using a sorted array.
/// Range scans locate the start position via binary search and iterate to end.
///
/// ## Thread safety
/// Uses Mutex for exclusive access (no I/O, memory access only).
public final class InMemoryEngine: StorageEngine, Sendable {
    public typealias TransactionType = InMemoryTransaction

    /// Sorted KV store (internal buffer).
    let _store: Mutex<[(key: Bytes, value: Bytes)]>

    public init() {
        self._store = Mutex([])
    }

    public func createTransaction() throws -> InMemoryTransaction {
        let snapshot = _store.withLock { Array($0) }
        return InMemoryTransaction(engine: self, snapshot: snapshot)
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

    /// Current store size (for testing).
    public var count: Int {
        _store.withLock { $0.count }
    }
}

/// Range result type for InMemoryEngine.
///
/// Array-based AsyncSequence. Returns results with zero copy.
public struct InMemoryRangeResult: AsyncSequence, Sendable {
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

/// Transaction implementation for InMemoryEngine.
///
/// Uses a read snapshot + write buffer approach.
/// Applies changes to the main store on commit.
public final class InMemoryTransaction: Transaction, @unchecked Sendable {

    public typealias RangeResult = InMemoryRangeResult

    private let engine: InMemoryEngine
    private var snapshot: [(key: Bytes, value: Bytes)]
    private var writeBuffer: [WriteOp] = []
    private var committed = false
    private var cancelled = false

    private enum WriteOp {
        case set(key: Bytes, value: Bytes)
        case clear(key: Bytes)
        case clearRange(begin: Bytes, end: Bytes)
    }

    init(engine: InMemoryEngine, snapshot: [(key: Bytes, value: Bytes)]) {
        self.engine = engine
        self.snapshot = snapshot
    }

    // MARK: - Read

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        guard !cancelled else { throw StorageError.invalidOperation("Transaction cancelled") }

        // Check write buffer in reverse order (latest operation takes priority)
        for op in writeBuffer.reversed() {
            switch op {
            case .set(let k, let v) where k == key:
                return v
            case .clear(let k) where k == key:
                return nil
            case .clearRange(let begin, let end)
                where compareBytes(key, begin) >= 0 && compareBytes(key, end) < 0:
                return nil
            default:
                continue
            }
        }

        // Search from snapshot (binary search)
        if let index = binarySearch(self.snapshot, for: key) {
            return self.snapshot[index].value
        }
        return nil
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> InMemoryRangeResult {
        guard !cancelled else {
            return InMemoryRangeResult(error: StorageError.invalidOperation("Transaction cancelled"))
        }

        // Build effective store by applying write buffer to snapshot
        var effective = self.snapshot
        for op in writeBuffer {
            switch op {
            case .set(let key, let value):
                if let idx = binarySearch(effective, for: key) {
                    effective[idx] = (key: key, value: value)
                } else {
                    let idx = insertionPoint(effective, for: key)
                    effective.insert((key: key, value: value), at: idx)
                }
            case .clear(let key):
                if let idx = binarySearch(effective, for: key) {
                    effective.remove(at: idx)
                }
            case .clearRange(let rangeBegin, let rangeEnd):
                effective.removeAll { compareBytes($0.key, rangeBegin) >= 0 && compareBytes($0.key, rangeEnd) < 0 }
            }
        }

        // Resolve KeySelectors using FDB-compatible algorithm
        let allKeys = effective.map(\.key)
        let startIdx = begin.resolve(in: allKeys)
        let endIdx = end.resolve(in: allKeys)

        guard startIdx < endIdx else {
            return InMemoryRangeResult([])
        }

        // Collect entries in the resolved range [startIdx, endIdx)
        var results: [(key: Bytes, value: Bytes)] = []

        if reverse {
            var i = endIdx - 1
            while i >= startIdx {
                results.append(effective[i])
                if limit > 0 && results.count >= limit { break }
                i -= 1
            }
        } else {
            var i = startIdx
            while i < endIdx {
                results.append(effective[i])
                if limit > 0 && results.count >= limit { break }
                i += 1
            }
        }

        return InMemoryRangeResult(results)
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
        guard !cancelled else { throw StorageError.invalidOperation("Transaction cancelled") }
        guard !committed else { return }

        engine._store.withLock { currentStore in
            for op in writeBuffer {
                switch op {
                case .set(let key, let value):
                    if let idx = binarySearch(currentStore, for: key) {
                        currentStore[idx] = (key: key, value: value)
                    } else {
                        let idx = insertionPoint(currentStore, for: key)
                        currentStore.insert((key: key, value: value), at: idx)
                    }
                case .clear(let key):
                    if let idx = binarySearch(currentStore, for: key) {
                        currentStore.remove(at: idx)
                    }
                case .clearRange(let begin, let end):
                    currentStore.removeAll { compareBytes($0.key, begin) >= 0 && compareBytes($0.key, end) < 0 }
                }
            }
        }
        committed = true
        writeBuffer.removeAll()
    }

    public func cancel() {
        guard !committed, !cancelled else { return }
        cancelled = true
        writeBuffer.removeAll()
    }

    // MARK: - Binary Search

    private func binarySearch(_ array: [(key: Bytes, value: Bytes)], for key: Bytes) -> Int? {
        var low = 0
        var high = array.count - 1
        while low <= high {
            let mid = low + (high - low) / 2
            let cmp = compareBytes(array[mid].key, key)
            if cmp == 0 { return mid }
            if cmp < 0 { low = mid + 1 }
            else { high = mid - 1 }
        }
        return nil
    }

    private func insertionPoint(_ array: [(key: Bytes, value: Bytes)], for key: Bytes) -> Int {
        var low = 0
        var high = array.count
        while low < high {
            let mid = low + (high - low) / 2
            if compareBytes(array[mid].key, key) < 0 {
                low = mid + 1
            } else {
                high = mid
            }
        }
        return low
    }
}
