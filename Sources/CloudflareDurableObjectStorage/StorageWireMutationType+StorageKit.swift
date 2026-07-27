import CloudflareDurableObjectStorageWire
import StorageKit

extension StorageWireMutationType {
    public init(_ mutationType: MutationType) {
        switch mutationType {
        case .add: self = .add
        case .setVersionstampedKey: self = .setVersionstampedKey
        case .setVersionstampedValue: self = .setVersionstampedValue
        case .bitOr: self = .bitOr
        case .bitAnd: self = .bitAnd
        case .bitXor: self = .bitXor
        case .max: self = .max
        case .min: self = .min
        case .compareAndClear: self = .compareAndClear
        }
    }

    public var mutationType: MutationType {
        switch self {
        case .add: return .add
        case .setVersionstampedKey: return .setVersionstampedKey
        case .setVersionstampedValue: return .setVersionstampedValue
        case .bitOr: return .bitOr
        case .bitAnd: return .bitAnd
        case .bitXor: return .bitXor
        case .max: return .max
        case .min: return .min
        case .compareAndClear: return .compareAndClear
        }
    }
}
