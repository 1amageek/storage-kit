import StorageKit

/// Wire representation of a byte array.
public struct CloudflareDurableObjectBytes: Sendable, Hashable {
    public let rawValue: Bytes

    public init(_ rawValue: Bytes) {
        self.rawValue = rawValue
    }

}
