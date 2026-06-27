import StorageKit

enum CloudflareDurableObjectWriteOp: Sendable, Hashable {
    case set(key: Bytes, value: Bytes)
    case clear(key: Bytes)
    case clearRange(begin: Bytes, end: Bytes)
    case atomic(key: Bytes, param: Bytes, mutationType: MutationType)

    var mutation: CloudflareDurableObjectMutation {
        switch self {
        case .set(let key, let value):
            return .set(
                key: CloudflareDurableObjectBytes(key),
                value: CloudflareDurableObjectBytes(value)
            )
        case .clear(let key):
            return .clear(key: CloudflareDurableObjectBytes(key))
        case .clearRange(let begin, let end):
            return .clearRange(
                begin: CloudflareDurableObjectBytes(begin),
                end: CloudflareDurableObjectBytes(end)
            )
        case .atomic(let key, let param, let mutationType):
            return .atomic(
                key: CloudflareDurableObjectBytes(key),
                param: CloudflareDurableObjectBytes(param),
                mutationType: CloudflareDurableObjectMutationTypeCode(mutationType)
            )
        }
    }
}
