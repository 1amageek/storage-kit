/// Ordered mutation sent to the Durable Object host for atomic commit.
public enum CloudflareDurableObjectMutation: Sendable, Codable, Hashable {
    case set(key: CloudflareDurableObjectBytes, value: CloudflareDurableObjectBytes)
    case clear(key: CloudflareDurableObjectBytes)
    case clearRange(begin: CloudflareDurableObjectBytes, end: CloudflareDurableObjectBytes)
    case atomic(
        key: CloudflareDurableObjectBytes,
        param: CloudflareDurableObjectBytes,
        mutationType: CloudflareDurableObjectMutationTypeCode
    )
}
