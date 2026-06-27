import StorageKit

/// Wire mutation code matching StorageKit mutation names.
public enum CloudflareDurableObjectMutationTypeCode: String, Sendable, Codable, Hashable {
    case add
    case bitOr
    case bitAnd
    case bitXor
    case max
    case min
    case compareAndClear
    case setVersionstampedKey
    case setVersionstampedValue

    public init(_ mutationType: MutationType) {
        switch mutationType {
        case .add:
            self = .add
        case .bitOr:
            self = .bitOr
        case .bitAnd:
            self = .bitAnd
        case .bitXor:
            self = .bitXor
        case .max:
            self = .max
        case .min:
            self = .min
        case .compareAndClear:
            self = .compareAndClear
        case .setVersionstampedKey:
            self = .setVersionstampedKey
        case .setVersionstampedValue:
            self = .setVersionstampedValue
        }
    }

    public var storageKitMutationType: MutationType {
        switch self {
        case .add:
            return .add
        case .bitOr:
            return .bitOr
        case .bitAnd:
            return .bitAnd
        case .bitXor:
            return .bitXor
        case .max:
            return .max
        case .min:
            return .min
        case .compareAndClear:
            return .compareAndClear
        case .setVersionstampedKey:
            return .setVersionstampedKey
        case .setVersionstampedValue:
            return .setVersionstampedValue
        }
    }
}
