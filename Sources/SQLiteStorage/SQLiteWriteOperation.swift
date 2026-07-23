import StorageKit

enum SQLiteWriteOperation: Sendable {
    case set(key: Bytes, value: Bytes)
    case clear(key: Bytes)
    case clearRange(begin: Bytes, end: Bytes)
    case atomic(key: Bytes, parameter: Bytes, mutationType: MutationType)
}
