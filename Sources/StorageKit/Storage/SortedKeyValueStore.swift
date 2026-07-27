import DatabaseTypes
/// Sorted key-value store backed by a contiguous array.
///
/// Maintains lexicographic order via `compareBytes`. All key-lookup operations
/// use binary search (O(log n)). Range deletion uses binary search to find
/// boundaries and removes the subrange in a single operation (O(log n + k))
/// instead of a linear scan (O(n)).
///
/// This is the canonical sorted-array abstraction for in-memory backends.
/// It eliminates scattered `binarySearch` / `insertionPoint` / `removeAll`
/// patterns that were previously duplicated across transaction and engine code.
package struct SortedKeyValueStore: Sendable {

    private(set) var entries: [(key: ByteString, value: ByteString)]

    init() {
        self.entries = []
    }

    init(_ entries: [(key: ByteString, value: ByteString)]) {
        self.entries = entries
    }

    var count: Int { entries.count }
    var isEmpty: Bool { entries.isEmpty }

    // MARK: - Point Operations

    /// O(log n) lookup.
    func get(_ key: ByteString) -> ByteString? {
        guard let idx = findIndex(of: key) else { return nil }
        return entries[idx].value
    }

    /// O(log n) search + O(n) shift for insert; O(1) for in-place update.
    mutating func set(_ key: ByteString, _ value: ByteString) {
        if let idx = findIndex(of: key) {
            entries[idx] = (key: key, value: value)
        } else {
            let idx = insertionPoint(for: key)
            entries.insert((key: key, value: value), at: idx)
        }
    }

    /// O(log n) search + O(n) shift for removal.
    mutating func delete(_ key: ByteString) {
        if let idx = findIndex(of: key) {
            entries.remove(at: idx)
        }
    }

    /// O(log n + k) range deletion where k = number of removed entries.
    ///
    /// Uses binary search to locate the begin and end boundaries,
    /// then removes the subrange in one operation.
    /// Range is [begin, end) — begin inclusive, end exclusive.
    mutating func deleteRange(begin: ByteString, end: ByteString) {
        let lo = insertionPoint(for: begin)
        let hi = insertionPoint(for: end)
        guard lo < hi else { return }
        entries.removeSubrange(lo..<hi)
    }

    // MARK: - Range Access

    /// Returns entries in [beginIdx, endIdx) as a slice.
    func slice(_ range: Range<Int>) -> ArraySlice<(key: ByteString, value: ByteString)> {
        entries[range]
    }

    /// Extract all keys (for KeySelector resolution).
    var keys: [ByteString] {
        var keys: [ByteString] = []
        keys.reserveCapacity(entries.count)
        for entry in entries {
            keys.append(entry.key)
        }
        return keys
    }

    // MARK: - Binary Search

    /// Returns the index of the entry with the given key, or nil.
    private func findIndex(of key: ByteString) -> Int? {
        var lo = 0
        var hi = entries.count - 1
        while lo <= hi {
            let mid = lo + (hi - lo) / 2
            let cmp = compareBytes(entries[mid].key, key)
            if cmp == 0 { return mid }
            if cmp < 0 { lo = mid + 1 }
            else { hi = mid - 1 }
        }
        return nil
    }

    /// Returns the index where key should be inserted to maintain sort order.
    /// Equivalent to C++ `std::lower_bound`.
    private func insertionPoint(for key: ByteString) -> Int {
        var lo = 0
        var hi = entries.count
        while lo < hi {
            let mid = lo + (hi - lo) / 2
            if compareBytes(entries[mid].key, key) < 0 {
                lo = mid + 1
            } else {
                hi = mid
            }
        }
        return lo
    }
}
