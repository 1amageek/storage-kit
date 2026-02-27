import Synchronization

/// バイト列の辞書順比較
///
/// - Returns: 負: lhs < rhs, 0: lhs == rhs, 正: lhs > rhs
package func compareBytes(_ lhs: Bytes, _ rhs: Bytes) -> Int {
    let minLen = min(lhs.count, rhs.count)
    for i in 0..<minLen {
        if lhs[i] != rhs[i] {
            return Int(lhs[i]) - Int(rhs[i])
        }
    }
    return lhs.count - rhs.count
}

/// テストとクライアント単体利用のためのインメモリ KV ストレージ
///
/// ソート済み配列ベースで辞書順 (lexicographic order) を維持する。
/// Range scan は二分探索で開始位置を特定し、end まで順次走査する。
///
/// ## スレッドセーフティ
/// Mutex で排他制御（I/O なし、メモリアクセスのみ）。
public final class InMemoryEngine: StorageEngine, Sendable {
    public typealias TransactionType = InMemoryTransaction

    /// ソート済みの KV ストア（内部バッファ）
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
        let result = try await operation(tx)
        try await tx.commit()
        return result
    }

    /// 現在のストアサイズ（テスト用）
    public var count: Int {
        _store.withLock { $0.count }
    }
}

/// InMemoryEngine 用の getRange 結果型
///
/// 配列ベースの AsyncSequence。ゼロコピーで結果を返す。
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

/// InMemoryEngine のトランザクション実装
///
/// 読み取りスナップショット + 書き込みバッファ方式。
/// commit 時にメインストアに反映する。
public final class InMemoryTransaction: Transaction, @unchecked Sendable {

    public typealias RangeResult = InMemoryRangeResult

    private let engine: InMemoryEngine
    private var snapshot: [(key: Bytes, value: Bytes)]
    private var writeBuffer: [WriteOp] = []
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

        // 書き込みバッファを逆順に確認（最新の操作が優先）
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

        // スナップショットから検索（二分探索）
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
        writeBuffer.removeAll()
    }

    public func cancel() {
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
