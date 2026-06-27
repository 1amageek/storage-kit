import StorageKitEmbeddedCore

/// Result of applying an atomic mutation to an existing value.
public enum AtomicMutationResult: Sendable, Equatable {
    /// Store the given bytes as the new value.
    case set(Bytes)
    /// Remove the key.
    case clear
    /// Leave the existing value untouched.
    case unchanged
}

extension MutationType {

    /// Apply this mutation to an existing value, following FoundationDB's
    /// atomic operation semantics exactly.
    ///
    /// Reference: https://apple.github.io/foundationdb/api-c.html#c.FDBMutationType
    ///
    /// - `add`: little-endian integer addition. The existing value is
    ///   zero-extended or truncated to `param.count` before the addition;
    ///   overflow wraps.
    /// - `bitAnd`: bitwise AND. A missing value stores `param` directly.
    /// - `bitOr` / `bitXor`: bitwise OR / XOR. A missing value is treated
    ///   as zero bytes of `param.count` length.
    /// - `max`: little-endian unsigned comparison; the larger value wins.
    ///   A missing value is treated as zero bytes.
    /// - `min`: little-endian unsigned comparison; the smaller value wins.
    ///   A missing value stores `param` directly.
    /// - `compareAndClear`: clears the key when the existing value equals
    ///   `param`; otherwise leaves it unchanged.
    /// - `setVersionstampedKey` / `setVersionstampedValue`: require a commit
    ///   version and cannot be computed outside FoundationDB — always throws.
    ///
    /// - Parameters:
    ///   - existing: The current value for the key, or nil if absent.
    ///   - param: The mutation parameter.
    /// - Returns: The mutation outcome.
    /// - Throws: `StorageError(.invalidOperation)` for versionstamp mutations.
    public func apply(to existing: Bytes?, param: Bytes) throws -> AtomicMutationResult {
        do {
            let result = try embeddedMutationType.apply(to: existing, param: param)
            switch result {
            case .set(let bytes):
                return .set(bytes)
            case .clear:
                return .clear
            case .unchanged:
                return .unchanged
            }
        } catch EmbeddedMutationError.versionstampRequiresCommitVersion {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                message: "Versionstamp mutations require a FoundationDB commit version "
                    + "and cannot be applied by non-FDB backends"
            )
        }
    }

    private var embeddedMutationType: EmbeddedMutationType {
        switch self {
        case .add:
            return .add
        case .setVersionstampedKey:
            return .setVersionstampedKey
        case .setVersionstampedValue:
            return .setVersionstampedValue
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
        }
    }
}
