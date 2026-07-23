public enum CloudflareDurableObjectRangeBoundary:
    Sendable,
    Hashable {
    case unbounded
    case selector(CloudflareDurableObjectKeySelector)
}
