import DatabaseTypes
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
/// let pendingVersionstamp = transaction.requestVersionstamp()
/// try await transaction.commit()
/// let version = try await pendingVersionstamp.value
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
    private static let incompletePlaceholder = ByteString(
        [UInt8](repeating: 0xFF, count: transactionVersionSize)
    )

    // MARK: - Properties

    /// Transaction version (10 bytes).
    /// nil for incomplete versionstamp (to be filled by FDB at commit time).
    public let transactionVersion: ByteString?

    /// User-defined version (2 bytes, big-endian).
    /// Used for ordering within a single transaction. Range: 0-65535.
    public let userVersion: UInt16

    // MARK: - Initialization

    /// Create a versionstamp.
    ///
    /// - Parameters:
    ///   - transactionVersion: 10-byte transaction version from FDB (nil for incomplete).
    ///   - userVersion: User-defined version (0-65535).
    public init(transactionVersion: ByteString?, userVersion: UInt16 = 0) {
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
    public func toBytes() -> ByteString {
        let version: ByteString
        if let transactionVersion {
            version = transactionVersion
        } else {
            version = Self.incompletePlaceholder
        }
        return ByteString.copying(count: Self.totalSize) { output in
            version.withUnsafeBytes { source in
                let destination = UnsafeMutableRawBufferPointer(
                    start: output.baseAddress,
                    count: Self.transactionVersionSize
                )
                destination.copyMemory(from: source)
            }
            output[Self.transactionVersionSize] = UInt8(
                truncatingIfNeeded: userVersion >> 8
            )
            output[Self.transactionVersionSize + 1] = UInt8(
                truncatingIfNeeded: userVersion
            )
        }
    }

    /// Create from 12-byte representation.
    ///
    /// - Parameter bytes: 12-byte array.
    /// - Throws: `TupleError.unexpectedEndOfData` if bytes length is not 12.
    public static func fromBytes(_ bytes: ByteString) throws -> Versionstamp {
        guard bytes.count == totalSize else {
            throw TupleError.unexpectedEndOfData
        }

        let startIndex = bytes.startIndex
        let trVersionBytes = bytes[
            startIndex..<(startIndex + transactionVersionSize)
        ]
        let uv = UInt16(bytes[startIndex + transactionVersionSize]) << 8
            | UInt16(bytes[startIndex + transactionVersionSize + 1])

        let isIncomplete = trVersionBytes == incompletePlaceholder
        return Versionstamp(
            transactionVersion: isIncomplete ? nil : trVersionBytes,
            userVersion: uv
        )
    }

    // MARK: - CustomStringConvertible

    public var description: String {
        if let tv = transactionVersion {
            var hexadecimalBytes = [UInt8](
                repeating: 0,
                count: Self.transactionVersionSize * 2
            )
            tv.withUnsafeBytes { bytes in
                for index in bytes.indices {
                    let byte = bytes[index]
                    hexadecimalBytes[index * 2] = Self.hexadecimalCharacter(
                        byte >> 4
                    )
                    hexadecimalBytes[index * 2 + 1] = Self.hexadecimalCharacter(
                        byte & 0x0f
                    )
                }
            }
            let tvHex = String(decoding: hexadecimalBytes, as: UTF8.self)
            return "Versionstamp(tr:\(tvHex), user:\(userVersion))"
        }
        return "Versionstamp(incomplete, user:\(userVersion))"
    }

    private static func hexadecimalCharacter(_ value: UInt8) -> UInt8 {
        value < 10 ? 48 + value : 87 + value
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
    public var tupleValue: TupleValue? { .versionstamp(self) }

    public func encodeTuple(to sink: inout TupleEncodingSink) {
        sink.writeByte(TupleTypeCode.versionstamp.rawValue)
        if let transactionVersion {
            sink.writeBytes(transactionVersion)
        } else {
            sink.writeBytes(Self.incompletePlaceholder)
        }
        sink.writeByte(UInt8(truncatingIfNeeded: userVersion >> 8))
        sink.writeByte(UInt8(truncatingIfNeeded: userVersion))
    }

    public static func decodeTuple(from bytes: ByteString, at offset: inout Int) throws -> Versionstamp {
        guard offset + Versionstamp.totalSize <= bytes.endIndex else {
            throw TupleError.unexpectedEndOfData
        }
        let versionstampBytes = bytes[
            offset..<(offset + Versionstamp.totalSize)
        ]
        offset += Versionstamp.totalSize
        return try Versionstamp.fromBytes(versionstampBytes)
    }
}
