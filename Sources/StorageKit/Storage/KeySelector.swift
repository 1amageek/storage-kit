/// Key selector for specifying relative key positions in range scans.
///
/// A KeySelector is resolved in two steps:
/// 1. Find the last key `k` in the database such that `k < key` (if orEqual is false)
///    or `k <= key` (if orEqual is true).
/// 2. Move `offset` positions forward from that key. Offset 0 stays at the found key,
///    offset 1 moves to the next key, etc.
///
/// This matches the FDB KeySelector semantics exactly.
///
/// Reference: https://apple.github.io/foundationdb/developer-guide.html#key-selectors
public struct KeySelector: Sendable, Hashable {

    /// The reference key.
    public let key: Bytes

    /// If true, the reference key itself is included in the initial search.
    public let orEqual: Bool

    /// Offset from the resolved position (positive = forward, negative = backward).
    public let offset: Int

    public init(key: Bytes, orEqual: Bool, offset: Int) {
        self.key = key
        self.orEqual = orEqual
        self.offset = offset
    }

    // MARK: - Factory Methods (FDB-compatible)

    /// Selects the first key >= the given key.
    ///
    /// Resolution: find last key `k` where `k < key` (orEqual=false), then move +1 forward.
    /// Result: the first key that is >= `key`.
    public static func firstGreaterOrEqual(_ key: Bytes) -> KeySelector {
        KeySelector(key: key, orEqual: false, offset: 1)
    }

    /// Selects the first key > the given key.
    ///
    /// Resolution: find last key `k` where `k <= key` (orEqual=true), then move +1 forward.
    /// Result: the first key that is strictly > `key`.
    public static func firstGreaterThan(_ key: Bytes) -> KeySelector {
        KeySelector(key: key, orEqual: true, offset: 1)
    }

    /// Selects the last key <= the given key.
    ///
    /// Resolution: find last key `k` where `k <= key` (orEqual=true), offset 0.
    /// Result: `key` itself if it exists, otherwise the last key before it.
    public static func lastLessOrEqual(_ key: Bytes) -> KeySelector {
        KeySelector(key: key, orEqual: true, offset: 0)
    }

    /// Selects the last key < the given key.
    ///
    /// Resolution: find last key `k` where `k < key` (orEqual=false), offset 0.
    /// Result: the last key strictly less than `key`.
    public static func lastLessThan(_ key: Bytes) -> KeySelector {
        KeySelector(key: key, orEqual: false, offset: 0)
    }

    /// Convenience initializer treating raw bytes as firstGreaterOrEqual.
    public init(_ key: Bytes) {
        self = .firstGreaterOrEqual(key)
    }

    // MARK: - Resolution for sorted-array backends

    /// Resolve this KeySelector against a sorted array of keys.
    ///
    /// Implements the FDB KeySelector resolution algorithm:
    /// 1. Find the last key satisfying the inequality (< or <=).
    /// 2. Apply the offset from that position.
    ///
    /// - Parameter keys: A sorted array of keys in lexicographic order.
    /// - Returns: The resolved index into the sorted array, clamped to [0, keys.count].
    ///   The returned index points to the selected key. A value of keys.count means
    ///   "past the end" (no key selected).
    public func resolve(in keys: [Bytes]) -> Int {
        // Step 1: Find the base position.
        // If orEqual: find the last key <= self.key → upper_bound(key) - 1
        // If !orEqual: find the last key < self.key → lower_bound(key) - 1
        let baseIndex: Int
        if orEqual {
            // upper_bound: first key > self.key
            baseIndex = upperBound(keys, for: key) - 1
        } else {
            // lower_bound: first key >= self.key
            baseIndex = lowerBound(keys, for: key) - 1
        }

        // Step 2: Apply offset.
        // baseIndex is the "last key matching the inequality" (-1 means "before all keys").
        // Adding offset 1 moves to the next key, which is baseIndex + 1.
        let resolved = baseIndex + offset

        // Clamp to valid range [0, keys.count]
        return max(0, min(resolved, keys.count))
    }
}

// MARK: - Binary search helpers (package-internal for testing)

/// Returns the index of the first key >= target (lower bound).
package func lowerBound(_ keys: [Bytes], for target: Bytes) -> Int {
    var lo = 0
    var hi = keys.count
    while lo < hi {
        let mid = lo + (hi - lo) / 2
        if compareBytes(keys[mid], target) < 0 {
            lo = mid + 1
        } else {
            hi = mid
        }
    }
    return lo
}

/// Returns the index of the first key > target (upper bound).
package func upperBound(_ keys: [Bytes], for target: Bytes) -> Int {
    var lo = 0
    var hi = keys.count
    while lo < hi {
        let mid = lo + (hi - lo) / 2
        if compareBytes(keys[mid], target) <= 0 {
            lo = mid + 1
        } else {
            hi = mid
        }
    }
    return lo
}
