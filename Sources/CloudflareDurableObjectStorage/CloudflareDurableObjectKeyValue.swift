/// Codable key-value row used by range, export, and import requests.
public struct CloudflareDurableObjectKeyValue: Sendable, Hashable, Codable {
    public let key: CloudflareDurableObjectBytes
    public let value: CloudflareDurableObjectBytes

    public init(key: CloudflareDurableObjectBytes, value: CloudflareDurableObjectBytes) {
        self.key = key
        self.value = value
    }
}
