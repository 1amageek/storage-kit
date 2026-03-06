import Synchronization

/// In-memory KV storage for testing and standalone client use.
///
/// Maintains lexicographic order via `SortedKeyValueStore`.
/// Range scans locate the start position via binary search and iterate to end.
///
/// ## Thread safety
/// Uses Mutex for exclusive access (no I/O, memory access only).
public final class InMemoryEngine: StorageEngine, Sendable {

    /// No configuration needed for in-memory storage.
    public struct Configuration: Sendable {
        public init() {}
    }

    public typealias TransactionType = InMemoryTransaction

    /// Sorted KV store (internal buffer).
    let _store: Mutex<SortedKeyValueStore>

    public init(configuration: Configuration = .init()) {
        self._store = Mutex(SortedKeyValueStore())
    }

    public func createTransaction() throws -> InMemoryTransaction {
        let snapshot = _store.withLock { SortedKeyValueStore($0.entries) }
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

/// Transaction implementation for InMemoryEngine.
///
/// Uses a read snapshot + write buffer approach.
/// Applies changes to the main store on commit.
public final class InMemoryTransaction: Transaction, Sendable {

    public typealias RangeResult = KeyValueRangeResult

    private let engine: InMemoryEngine
    private let snapshot: SortedKeyValueStore

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

    init(engine: InMemoryEngine, snapshot: SortedKeyValueStore) {
        self.engine = engine
        self.snapshot = snapshot
        self._state = Mutex(MutableState())
    }

    // MARK: - Read

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        let writeBuffer = try _state.withLock { state in
            guard !state.cancelled else { throw StorageError.invalidOperation("Transaction cancelled") }
            return state.writeBuffer
        }

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
        return self.snapshot.get(key)
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueRangeResult {
        let (cancelled, writeBuffer) = _state.withLock { ($0.cancelled, $0.writeBuffer) }
        guard !cancelled else {
            return KeyValueRangeResult(error: StorageError.invalidOperation("Transaction cancelled"))
        }

        // Build effective store by applying write buffer to snapshot
        var effective = self.snapshot
        for op in writeBuffer {
            switch op {
            case .set(let key, let value):
                effective.set(key, value)
            case .clear(let key):
                effective.delete(key)
            case .clearRange(let rangeBegin, let rangeEnd):
                effective.deleteRange(begin: rangeBegin, end: rangeEnd)
            }
        }

        // Resolve KeySelectors using FDB-compatible algorithm
        let allKeys = effective.keys
        let startIdx = begin.resolve(in: allKeys)
        let endIdx = end.resolve(in: allKeys)

        guard startIdx < endIdx else {
            return KeyValueRangeResult([])
        }

        // Collect entries in the resolved range [startIdx, endIdx)
        let slice = effective.slice(startIdx..<endIdx)
        var results: [(key: Bytes, value: Bytes)]

        if reverse {
            results = Array(slice.reversed())
        } else {
            results = Array(slice)
        }

        if limit > 0 && results.count > limit {
            results = Array(results.prefix(limit))
        }

        return KeyValueRangeResult(results)
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
        let ops = try _state.withLock { state -> [WriteOp] in
            guard !state.cancelled else { throw StorageError.invalidOperation("Transaction cancelled") }
            guard !state.committed else { return [] }
            state.committed = true
            let ops = state.writeBuffer
            state.writeBuffer.removeAll()
            return ops
        }

        guard !ops.isEmpty else { return }

        engine._store.withLock { store in
            for op in ops {
                switch op {
                case .set(let key, let value):
                    store.set(key, value)
                case .clear(let key):
                    store.delete(key)
                case .clearRange(let begin, let end):
                    store.deleteRange(begin: begin, end: end)
                }
            }
        }
    }

    public func cancel() {
        _state.withLock { state in
            guard !state.committed, !state.cancelled else { return }
            state.cancelled = true
            state.writeBuffer.removeAll()
        }
    }
}
