/// Errors raised while applying range overlays.
public enum EmbeddedRangeOverlayError: Error, Sendable, Equatable {
    case invalidRangeLimit
    case mutation(EmbeddedMutationError)
}
