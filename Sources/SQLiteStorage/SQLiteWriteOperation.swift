import DatabaseTypes
import StorageKit

enum SQLiteWriteOperation: Sendable {
    case set(key: ByteString, value: ByteString)
    case clear(key: ByteString)
    case clearRange(begin: ByteString, end: ByteString)
    case atomic(key: ByteString, parameter: ByteString, mutationType: MutationType)
}
