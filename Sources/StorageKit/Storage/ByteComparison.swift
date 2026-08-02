import DatabaseTypes
#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#elseif canImport(Glibc)
import Glibc
#elseif canImport(WASILibc)
import WASILibc
#endif

/// Lexicographic comparison of byte arrays using `memcmp`.
///
/// - Returns: Negative: lhs < rhs, 0: lhs == rhs, Positive: lhs > rhs.
package func compareBytes(_ lhs: ByteString, _ rhs: ByteString) -> Int {
    let minLen = min(lhs.count, rhs.count)
    if minLen > 0 {
        let cmp = lhs.withUnsafeBytes { lhsBytes in
            rhs.withUnsafeBytes { rhsBytes in
                memcmp(
                    lhsBytes.baseAddress!,
                    rhsBytes.baseAddress!,
                    minLen
                )
            }
        }
        if cmp != 0 { return Int(cmp) }
    }
    return lhs.count - rhs.count
}
