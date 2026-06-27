import StorageKit

/// Codable representation of a StorageKit key selector.
public struct CloudflareDurableObjectKeySelector: Sendable, Hashable, Codable {
    public let key: CloudflareDurableObjectBytes
    public let orEqual: Bool
    public let offset: Int

    public init(key: CloudflareDurableObjectBytes, orEqual: Bool, offset: Int) {
        self.key = key
        self.orEqual = orEqual
        self.offset = offset
    }

    public init(_ selector: KeySelector) {
        self.key = CloudflareDurableObjectBytes(selector.key)
        self.orEqual = selector.orEqual
        self.offset = selector.offset
    }

    public var storageKitSelector: KeySelector {
        KeySelector(key: key.rawValue, orEqual: orEqual, offset: offset)
    }
}
