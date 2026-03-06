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
