import DatabaseTypes

/// Layout-version marker of one storage root and its bootstrap inspection.
///
/// The marker lives under the reserved catalog prefix, which no content prefix
/// a Directory Layer allocates can start with. Inspection is the InspectRoot
/// state of the state machine in `DESIGN.md`; the caller decides between
/// OpenV1, InitializeV1, and Reject from the returned state.
///
/// A backend whose store is its own storage root reads and writes the marker at
/// `key` and answers emptiness for the whole store. A backend that hosts
/// several storage roots in one physical store, as FoundationDB does below a
/// configured root path, records one marker per root at `key(rootPath:)` and
/// answers emptiness for that root alone, so one root is never initialized,
/// opened, or rejected because of another root's data.
public enum StorageLayoutMarker {
    /// Every catalog key starts with this byte; no allocated content prefix
    /// does. `Directory` owns the byte, so the reserved region of a store and
    /// the node subspace of a Directory Layer can never drift apart.
    public static let reservedPrefix: ByteString = [Directory.nodeSubspaceByte]

    /// Discriminator that follows the reserved prefix in a layout marker key.
    ///
    /// No Tuple type code has this value, so a marker key can never collide
    /// with an allocator key or a child edge of the root layer.
    private static let markerByte: UInt8 = 0x6C

    /// Key of the layout marker of a store that is its own storage root.
    public static let key: ByteString = [Directory.nodeSubspaceByte, markerByte]

    /// Key of the layout marker of the storage root at `rootPath`.
    ///
    /// The path is Tuple-encoded after the marker key, so distinct root paths
    /// never share a marker. The marker is only ever point-read, so one root
    /// path's key being a byte prefix of a deeper root path's key is not
    /// observable. The empty root path yields `key`, which keeps a store that
    /// is its own root on the key its V1 data already uses.
    public static func key(rootPath: [String]) -> ByteString {
        guard !rootPath.isEmpty else {
            return key
        }
        return Subspace(prefix: key).pack(
            elements: rootPath.map { $0 as any TupleElement }
        )
    }

    /// Marker value of layout V1: `"SKL"` followed by version byte 1.
    public static let v1: ByteString = [0x53, 0x4B, 0x4C, 0x01]

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

    /// Classifies one storage root from its marker value and whether that root
    /// holds any data.
    ///
    /// A backend that scopes its root to part of a shared store answers
    /// `rootIsEmpty` for that root alone. `rootIsEmpty` must be false whenever
    /// the root holds anything an earlier or foreign layout could have written,
    /// so a root with data and no marker is rejected instead of adopted.
    public static func inspect(
        marker: ByteString?,
        rootIsEmpty: Bool
    ) -> Inspection {
        guard let marker else {
            return rootIsEmpty ? .uninitialized : .rejected(.markerAbsentKeyspaceNonempty)
        }
        return marker == v1 ? .openV1 : .rejected(.unknownMarker(marker))
    }

    /// Inspects a store that is its own storage root.
    ///
    /// The marker is read at `key`. When it is absent the store is probed for
    /// its first key with no upper bound, so a key at or above `[0xFF]` counts
    /// as data exactly like any other key. `getKey` resolves through the
    /// transaction's range cursor on every backend and yields `nil` only when
    /// no key satisfies the selector.
    public static func inspect(
        transaction: any TransactionReadAccess
    ) async throws -> Inspection {
        let marker = try await transaction.getValue(for: key)
        if marker != nil {
            return inspect(marker: marker, rootIsEmpty: false)
        }
        let firstKey = try await transaction.getKey(selector: .firstGreaterOrEqual([]))
        return inspect(marker: nil, rootIsEmpty: firstKey == nil)
    }
}
