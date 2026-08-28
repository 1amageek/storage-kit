import DatabaseTypes

/// Layout-version marker of a key-value store and its bootstrap inspection.
///
/// The marker lives under the reserved catalog prefix. Inspection is the
/// InspectRoot state of the state machine in `DESIGN.md`; the caller decides
/// between OpenV1, InitializeV1, and Reject from the returned state.
public enum StorageLayoutMarker {
    /// Every catalog key starts with this byte; no Directory root does.
    public static let reservedPrefix: ByteString = [0xFE]

    /// Key of the layout marker.
    public static let key: ByteString = [0xFE, 0x6C]

    /// Marker value of layout V1: `"SKL"` followed by version byte 1.
    public static let v1: ByteString = [0x53, 0x4B, 0x4C, 0x01]

    /// Exclusive end of the user keyspace on every backend.
    public static let userKeyspaceEnd: ByteString = [0xFF]

    public enum Inspection: Sendable, Hashable {
        case openV1
        case uninitialized
        case rejected(Rejection)
    }

    public enum Rejection: Sendable, Hashable, CustomStringConvertible {
        case markerAbsentKeyspaceNonempty
        case unknownMarker(ByteString)

        public var description: String {
            switch self {
            case .markerAbsentKeyspaceNonempty:
                return "layout marker is absent but the keyspace is not empty"
            case .unknownMarker(let bytes):
                return "layout marker holds unknown bytes \(bytes.map { String($0, radix: 16) }.joined(separator: " "))"
            }
        }
    }

    /// Reads the marker and, when absent, probes the keyspace with one
    /// limit-1 range read over `[] ..< [0xFF]`.
    public static func inspect(
        transaction: any TransactionReadAccess
    ) async throws -> Inspection {
        if let value = try await transaction.getValue(for: key) {
            return value == v1 ? .openV1 : .rejected(.unknownMarker(value))
        }
        let probe = try await transaction.collectRange(
            from: .firstGreaterOrEqual([]),
            to: .firstGreaterOrEqual(userKeyspaceEnd),
            limit: 1
        )
        return probe.isEmpty ? .uninitialized : .rejected(.markerAbsentKeyspaceNonempty)
    }
}
