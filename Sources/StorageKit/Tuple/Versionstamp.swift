/// A FoundationDB versionstamp (12 bytes: 10-byte transaction version + 2-byte user version).
///
/// Versionstamps provide globally unique, monotonically increasing identifiers
/// assigned by FoundationDB at commit time. They enable:
/// - Optimistic concurrency control
/// - Globally ordered key generation
/// - Temporal ordering of records
///
/// ## Usage
/// ```swift
/// // Create an incomplete versionstamp (placeholder for FDB to fill at commit)
/// let vs = Versionstamp.incomplete(userVersion: 0)
///
/// // After commit, create a complete versionstamp from the committed version
/// let version = try await transaction.getVersionstamp()
/// let complete = try Versionstamp.fromBytes(version!)
/// ```
///
/// Reference: https://apple.github.io/foundationdb/developer-guide.html#versionstamps
public struct Versionstamp: Sendable, Hashable, Equatable, CustomStringConvertible {

    // MARK: - Constants

    /// Size of transaction version in bytes (10 bytes / 80 bits)
    public static let transactionVersionSize = 10

    /// Size of user version in bytes (2 bytes / 16 bits)
    public static let userVersionSize = 2

    /// Total size of versionstamp in bytes (12 bytes / 96 bits)
    public static let totalSize = transactionVersionSize + userVersionSize

    /// Placeholder for incomplete transaction version (10 bytes of 0xFF)
    private static let incompletePlaceholder = Bytes(repeating: 0xFF, count: transactionVersionSize)

    // MARK: - Properties

    /// Transaction version (10 bytes).
    /// nil for incomplete versionstamp (to be filled by FDB at commit time).
    public let transactionVersion: Bytes?

    /// User-defined version (2 bytes, big-endian).
    /// Used for ordering within a single transaction. Range: 0-65535.
    public let userVersion: UInt16

    // MARK: - Initialization

    /// Create a versionstamp.
    ///
    /// - Parameters:
    ///   - transactionVersion: 10-byte transaction version from FDB (nil for incomplete).
    ///   - userVersion: User-defined version (0-65535).
    public init(transactionVersion: Bytes?, userVersion: UInt16 = 0) {
        if let tv = transactionVersion {
            precondition(
                tv.count == Self.transactionVersionSize,
                "Transaction version must be exactly \(Self.transactionVersionSize) bytes"
            )
        }
        self.transactionVersion = transactionVersion
        self.userVersion = userVersion
    }

    /// Create an incomplete versionstamp (placeholder for FDB to fill at commit time).
    ///
    /// - Parameter userVersion: User-defined version (0-65535).
    public static func incomplete(userVersion: UInt16 = 0) -> Versionstamp {
        Versionstamp(transactionVersion: nil, userVersion: userVersion)
    }

    // MARK: - Properties

    /// Whether this versionstamp has been completed (transaction version assigned).
    public var isComplete: Bool {
        transactionVersion != nil
    }

    /// Convert to 12-byte representation.
    ///
    /// Layout: [10 bytes transaction version (big-endian)] [2 bytes user version (big-endian)]
    public func toBytes() -> Bytes {
        var bytes = transactionVersion ?? Self.incompletePlaceholder
        withUnsafeBytes(of: userVersion.bigEndian) { bytes.append(contentsOf: $0) }
        return bytes
    }

    /// Create from 12-byte representation.
    ///
    /// - Parameter bytes: 12-byte array.
    /// - Throws: `TupleError.unexpectedEndOfData` if bytes length is not 12.
    public static func fromBytes(_ bytes: Bytes) throws -> Versionstamp {
        guard bytes.count == totalSize else {
            throw TupleError.unexpectedEndOfData
        }

        let trVersionBytes = Array(bytes.prefix(transactionVersionSize))
        let userVersionBytes = bytes.suffix(userVersionSize)

        let uv = userVersionBytes.withUnsafeBytes {
            $0.load(as: UInt16.self).bigEndian
        }

        let isIncomplete = trVersionBytes == incompletePlaceholder
        return Versionstamp(
            transactionVersion: isIncomplete ? nil : trVersionBytes,
            userVersion: uv
        )
    }

    // MARK: - CustomStringConvertible

    public var description: String {
        if let tv = transactionVersion {
            let tvHex = tv.map { String(format: "%02x", $0) }.joined()
            return "Versionstamp(tr:\(tvHex), user:\(userVersion))"
        }
        return "Versionstamp(incomplete, user:\(userVersion))"
    }
}

// MARK: - Comparable

extension Versionstamp: Comparable {
    public static func < (lhs: Versionstamp, rhs: Versionstamp) -> Bool {
        lhs.toBytes().lexicographicallyPrecedes(rhs.toBytes())
    }
}

// MARK: - TupleElement

extension Versionstamp: TupleElement {
    public func encodeTuple() -> Bytes {
        var bytes: Bytes = [TupleTypeCode.versionstamp.rawValue]
        bytes.append(contentsOf: toBytes())
        return bytes
    }

    public static func decodeTuple(from bytes: Bytes, at offset: inout Int) throws -> Versionstamp {
        guard offset + Versionstamp.totalSize <= bytes.count else {
            throw TupleError.unexpectedEndOfData
        }
        let versionstampBytes = Array(bytes[offset..<(offset + Versionstamp.totalSize)])
        offset += Versionstamp.totalSize
        return try Versionstamp.fromBytes(versionstampBytes)
    }
}
