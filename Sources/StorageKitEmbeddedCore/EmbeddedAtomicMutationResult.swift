/// Result of applying an atomic mutation to an existing byte value.
public enum EmbeddedAtomicMutationResult: Sendable, Equatable {
    case set([UInt8])
    case clear
    case unchanged
}
