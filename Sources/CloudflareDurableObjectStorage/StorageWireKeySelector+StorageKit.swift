import CloudflareDurableObjectStorageWire
import StorageKit

extension StorageWireKeySelector {
    public init(_ selector: KeySelector) {
        self.init(
            key: selector.key,
            orEqual: selector.orEqual,
            offset: selector.offset
        )
    }

    public var keySelector: KeySelector {
        KeySelector(key: key, orEqual: orEqual, offset: offset)
    }
}
