import Foundation

/// Lexicographic comparison of byte arrays using `memcmp`.
///
/// - Returns: Negative: lhs < rhs, 0: lhs == rhs, Positive: lhs > rhs.
package func compareBytes(_ lhs: Bytes, _ rhs: Bytes) -> Int {
    let minLen = min(lhs.count, rhs.count)
    if minLen > 0 {
        let cmp = lhs.withUnsafeBufferPointer { lhsBuf in
            rhs.withUnsafeBufferPointer { rhsBuf in
                memcmp(lhsBuf.baseAddress!, rhsBuf.baseAddress!, minLen)
            }
        }
        if cmp != 0 { return Int(cmp) }
    }
    return lhs.count - rhs.count
}
