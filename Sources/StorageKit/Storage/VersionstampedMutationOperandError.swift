/// A malformed or boundary-violating versionstamped mutation operand.
public enum VersionstampedMutationOperandError: Error, Sendable, Equatable {
    case operandTooShort(actualByteCount: Int)
    case offsetCannotBeRepresented(UInt32)
    case replacementOutsidePayload(offset: Int, payloadByteCount: Int)
    case invalidPlaceholder(offset: Int)
    case replacementOverlapsProtectedPrefix(
        offset: Int,
        protectedPrefixByteCount: Int
    )
}
