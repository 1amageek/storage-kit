import DatabaseTypes
import StorageKit

/// Wire representation of a byte array.
public struct CloudflareDurableObjectBytes: Sendable, Hashable {
    public let rawValue: ByteString

    public init(_ rawValue: ByteString) {
        self.rawValue = rawValue
    }

}
