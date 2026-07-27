import DatabaseTypes
import StorageKit

/// Canonical storage compaction continuation for SQLite incremental vacuum.
///
/// Wire layout (8 bytes):
/// - 0...3: ASCII `SCMP`
/// - 4: protocol version
/// - 5: backend identifier (`1` = SQLite)
/// - 6: algorithm identifier (`1` = incremental vacuum)
/// - 7: reserved flags (`0`)
enum SQLiteIncrementalCompactionToken {
    static let currentVersion: UInt8 = 1

    private static let magic: ByteString = [0x53, 0x43, 0x4D, 0x50]
    private static let sqliteBackend: UInt8 = 1
    private static let incrementalVacuumAlgorithm: UInt8 = 1
    private static let reservedFlags: UInt8 = 0

    static var current: StorageCompactionContinuation {
        StorageCompactionContinuation(
            bytes: [
                0x53,
                0x43,
                0x4D,
                0x50,
                currentVersion,
                sqliteBackend,
                incrementalVacuumAlgorithm,
                reservedFlags,
            ]
        )
    }

    static func validate(
        _ continuation: StorageCompactionContinuation
    ) throws(StorageCompactionError) {
        let bytes = continuation.bytes
        let startIndex = bytes.startIndex
        guard bytes.count == 8,
              bytes[startIndex..<(startIndex + 4)] == magic else {
            throw .invalidContinuation
        }
        guard bytes[startIndex + 4] == currentVersion else {
            throw .unsupportedContinuationVersion(
                actual: bytes[startIndex + 4],
                supported: currentVersion
            )
        }
        guard bytes[startIndex + 5] == sqliteBackend,
              bytes[startIndex + 6] == incrementalVacuumAlgorithm else {
            throw .incompatibleContinuation
        }
        guard bytes[startIndex + 7] == reservedFlags else {
            throw .invalidContinuation
        }
    }
}
