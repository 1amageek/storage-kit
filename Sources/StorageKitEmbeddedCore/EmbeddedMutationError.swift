/// Errors raised by embedded atomic mutation evaluation.
public enum EmbeddedMutationError: Error, Sendable, Equatable {
    case versionstampRequiresCommitVersion
}
