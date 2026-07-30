import DatabaseTypes

/// A statically inspectable value decoded by the FoundationDB Tuple Layer.
///
/// `TupleElement` remains the extensible encoding contract. `TupleValue` is the
/// closed set of values that the canonical decoder can produce. This separation
/// lets Embedded callers inspect decoded elements without runtime type casts.
public enum TupleValue: Sendable, Hashable {
    case null
    case bytes(ByteString)
    case string(String)
    indirect case nested(Tuple)
    case signedInteger(Int64)
    case unsignedInteger(UInt64)
    case float32(Float)
    case float64(Double)
    case boolean(Bool)
    case uuid(DatabaseTypes.UUID)
    case versionstamp(Versionstamp)
}
