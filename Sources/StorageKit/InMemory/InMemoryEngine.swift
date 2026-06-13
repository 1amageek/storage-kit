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
        return try await ActiveTransactionScope.$current.withValue(tx) {
            do {
                let result = try await operation(tx)
                try await tx.commit()
                return result
            } catch {
                tx.cancel()
                throw error
            }
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
        case atomic(key: Bytes, param: Bytes, mutationType: MutationType)
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

        // Replay the write buffer in order on top of the snapshot value.
        // Atomic mutations depend on the value produced by preceding operations,
        // so forward replay is required (a reverse scan cannot resolve them).
        var value = self.snapshot.get(key)
        for op in writeBuffer {
            switch op {
            case .set(let k, let v) where k == key:
                value = v
            case .clear(let k) where k == key:
                value = nil
            case .clearRange(let begin, let end)
                where compareBytes(key, begin) >= 0 && compareBytes(key, end) < 0:
                value = nil
            case .atomic(let k, let param, let mutationType) where k == key:
                switch try mutationType.apply(to: value, param: param) {
                case .set(let bytes):
                    value = bytes
                case .clear:
                    value = nil
                case .unchanged:
                    break
                }
            default:
                continue
            }
        }
        return value
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
            case .atomic(let key, let param, let mutationType):
                do {
                    switch try mutationType.apply(to: effective.get(key), param: param) {
                    case .set(let bytes):
                        effective.set(key, bytes)
                    case .clear:
                        effective.delete(key)
                    case .unchanged:
                        break
                    }
                } catch {
                    return KeyValueRangeResult(error: error)
                }
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

    // MARK: - Atomic Operations

    public func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) {
        _state.withLock { state in
            guard !state.cancelled else { return }
            state.writeBuffer.append(.atomic(key: key, param: param, mutationType: mutationType))
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

        // Stage all operations on a copy and swap at the end, so a throwing
        // atomic mutation (versionstamp) leaves the store untouched.
        // Atomics apply to the store's value at commit time (FDB semantics),
        // not to the transaction's read snapshot.
        try engine._store.withLock { store in
            var staged = store
            for op in ops {
                switch op {
                case .set(let key, let value):
                    staged.set(key, value)
                case .clear(let key):
                    staged.delete(key)
                case .clearRange(let begin, let end):
                    staged.deleteRange(begin: begin, end: end)
                case .atomic(let key, let param, let mutationType):
                    switch try mutationType.apply(to: staged.get(key), param: param) {
                    case .set(let bytes):
                        staged.set(key, bytes)
                    case .clear:
                        staged.delete(key)
                    case .unchanged:
                        break
                    }
                }
            }
            store = staged
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
