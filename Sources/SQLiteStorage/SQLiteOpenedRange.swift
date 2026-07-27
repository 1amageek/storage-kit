import DatabaseTypes
import StorageKit

struct SQLiteOpenedRange: Sendable {
    let registrationIdentifier: UInt64
    let cursorIdentifier: UInt64
    let first: (ByteString, ByteString)?
}
