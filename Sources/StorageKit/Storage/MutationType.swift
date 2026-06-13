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
    /// Set to the maximum of the existing and given values, compared as
    /// little-endian unsigned integers (consistent with `add`). A missing value
    /// is treated as zero. See `MutationType.apply(to:param:)`.
    case max
    /// Set to the minimum of the existing and given values, compared as
    /// little-endian unsigned integers (consistent with `add`). A missing value
    /// is set to the given value directly. See `MutationType.apply(to:param:)`.
    case min
    /// Compare and clear.
    case compareAndClear
}
