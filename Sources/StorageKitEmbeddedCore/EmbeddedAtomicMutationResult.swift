import DatabaseTypes
/// Result of applying an atomic mutation to an existing byte value.
public enum EmbeddedAtomicMutationResult: Sendable, Equatable {
    case set(ByteString)
    case clear
    case unchanged
}
