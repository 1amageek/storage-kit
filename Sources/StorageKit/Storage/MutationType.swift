/// Types of atomic mutation operations.
///
/// Abstracts FDB's atomic mutation types.
public enum MutationType: Sendable {
    /// Addition (adds little-endian integer byte arrays).
    case add
    /// Set key with versionstamp.
    case setVersionstampedKey
    /// Set value with versionstamp.
    case setVersionstampedValue
    /// Bitwise OR.
    case bitOr
    /// Bitwise AND.
    case bitAnd
    /// Bitwise XOR.
    case bitXor
    /// Set to maximum value (lexicographic byte comparison).
    case max
    /// Set to minimum value (lexicographic byte comparison).
    case min
    /// Compare and clear.
    case compareAndClear
}
