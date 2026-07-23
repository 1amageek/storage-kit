/// StorageKit Wire key-value row returned by a range request.
public struct CloudflareDurableObjectKeyValue: Sendable, Hashable {
    public let key: CloudflareDurableObjectBytes
    public let value: CloudflareDurableObjectBytes

    public init(key: CloudflareDurableObjectBytes, value: CloudflareDurableObjectBytes) {
        self.key = key
        self.value = value
    }
}
