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

/// InMemoryEngine のトランザクション実装
///
/// 読み取りスナップショット + 書き込みバッファ方式。
/// commit 時にメインストアに反映する。
public final class InMemoryTransaction: Transaction, @unchecked Sendable {

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

    public func getValue(for key: Bytes) async throws -> Bytes? {
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
        if let index = binarySearch(snapshot, for: key) {
            return snapshot[index].value
        }
        return nil
    }

    public func getRange(
        begin: Bytes,
        end: Bytes,
        limit: Int,
        reverse: Bool
    ) async throws -> KeyValueSequence {
        guard !cancelled else { throw StorageError.invalidOperation("Transaction cancelled") }

        // スナップショットに書き込みバッファを適用した一時ストアを構築
        var effective = snapshot
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

        // 範囲内のエントリを収集
        var results: [(key: Bytes, value: Bytes)] = []
        let startIdx = insertionPoint(effective, for: begin)

        if reverse {
            let endIdx = insertionPoint(effective, for: end)
            var i = endIdx - 1
            while i >= startIdx {
                let entry = effective[i]
                if compareBytes(entry.key, begin) >= 0 && compareBytes(entry.key, end) < 0 {
                    results.append(entry)
                    if limit > 0 && results.count >= limit { break }
                }
                i -= 1
            }
        } else {
            var i = startIdx
            while i < effective.count {
                let entry = effective[i]
                guard compareBytes(entry.key, end) < 0 else { break }
                if compareBytes(entry.key, begin) >= 0 {
                    results.append(entry)
                    if limit > 0 && results.count >= limit { break }
                }
                i += 1
            }
        }

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
