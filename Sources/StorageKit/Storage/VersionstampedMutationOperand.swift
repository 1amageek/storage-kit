import DatabaseTypes

/// The validated layout of a FoundationDB-compatible versionstamped operand.
///
/// The final four bytes contain the little-endian offset of a ten-byte `0xFF`
/// placeholder within the preceding payload.
public struct VersionstampedMutationOperand: Sendable, Equatable {
    public static let replacementByteCount = 10
    public static let offsetTrailerByteCount = 4

    public let payloadByteCount: Int
    public let replacementOffset: Int

    public init(_ operand: borrowing ByteString) throws {
        let minimumByteCount = Self.replacementByteCount
            + Self.offsetTrailerByteCount
        guard operand.count >= minimumByteCount else {
            throw VersionstampedMutationOperandError.operandTooShort(
                actualByteCount: operand.count
            )
        }

        let payloadByteCount = operand.count - Self.offsetTrailerByteCount
        let rawOffset = operand.withUnsafeBytes { bytes in
            UInt32(bytes[payloadByteCount])
                | (UInt32(bytes[payloadByteCount + 1]) << 8)
                | (UInt32(bytes[payloadByteCount + 2]) << 16)
                | (UInt32(bytes[payloadByteCount + 3]) << 24)
        }
        guard let replacementOffset = Int(exactly: rawOffset) else {
            throw VersionstampedMutationOperandError
                .offsetCannotBeRepresented(rawOffset)
        }
        guard replacementOffset <= payloadByteCount - Self.replacementByteCount
        else {
            throw VersionstampedMutationOperandError.replacementOutsidePayload(
                offset: replacementOffset,
                payloadByteCount: payloadByteCount
            )
        }
        let replacementEnd = replacementOffset + Self.replacementByteCount
        let placeholderIsValid = operand.withUnsafeBytes { bytes in
            for index in replacementOffset..<replacementEnd
            where bytes[index] != 0xFF {
                return false
            }
            return true
        }
        guard placeholderIsValid else {
            throw VersionstampedMutationOperandError.invalidPlaceholder(
                offset: replacementOffset
            )
        }

        self.payloadByteCount = payloadByteCount
        self.replacementOffset = replacementOffset
    }

    /// Requires the commit-time replacement to remain after an immutable key
    /// prefix such as a selected database data root.
    public func validateReplacement(
        afterProtectedPrefixByteCount protectedPrefixByteCount: Int
    ) throws {
        guard protectedPrefixByteCount >= 0,
              replacementOffset >= protectedPrefixByteCount else {
            throw VersionstampedMutationOperandError
                .replacementOverlapsProtectedPrefix(
                    offset: replacementOffset,
                    protectedPrefixByteCount: protectedPrefixByteCount
                )
        }
    }
}
