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
        switch self {
        case .add:
            var result = Self.adjusted(existing ?? [], to: param.count)
            var carry: UInt16 = 0
            for index in 0..<param.count {
                let sum = UInt16(result[index]) + UInt16(param[index]) + carry
                result[index] = UInt8(truncatingIfNeeded: sum)
                carry = sum >> 8
            }
            return .set(result)

        case .bitAnd:
            guard let existing else { return .set(param) }
            var result = Self.adjusted(existing, to: param.count)
            for index in 0..<param.count {
                result[index] &= param[index]
            }
            return .set(result)

        case .bitOr:
            var result = Self.adjusted(existing ?? [], to: param.count)
            for index in 0..<param.count {
                result[index] |= param[index]
            }
            return .set(result)

        case .bitXor:
            var result = Self.adjusted(existing ?? [], to: param.count)
            for index in 0..<param.count {
                result[index] ^= param[index]
            }
            return .set(result)

        case .max:
            let current = Self.adjusted(existing ?? [], to: param.count)
            return .set(Self.compareLittleEndian(current, param) >= 0 ? current : param)

        case .min:
            guard let existing else { return .set(param) }
            let current = Self.adjusted(existing, to: param.count)
            return .set(Self.compareLittleEndian(current, param) <= 0 ? current : param)

        case .compareAndClear:
            if let existing, existing == param {
                return .clear
            }
            return .unchanged

        case .setVersionstampedKey, .setVersionstampedValue:
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                message: "Versionstamp mutations require a FoundationDB commit version "
                    + "and cannot be applied by non-FDB backends"
            )
        }
    }

    /// Zero-extend or truncate `value` to exactly `length` bytes,
    /// matching FDB's operand adjustment rules.
    private static func adjusted(_ value: Bytes, to length: Int) -> Bytes {
        if value.count == length {
            return value
        }
        if value.count > length {
            return Array(value.prefix(length))
        }
        return value + Array(repeating: 0, count: length - value.count)
    }

    /// Compare two equal-length byte strings as little-endian unsigned
    /// integers (most significant byte last).
    private static func compareLittleEndian(_ lhs: Bytes, _ rhs: Bytes) -> Int {
        precondition(lhs.count == rhs.count, "Operands must be adjusted to equal length")
        var index = lhs.count - 1
        while index >= 0 {
            if lhs[index] != rhs[index] {
                return lhs[index] < rhs[index] ? -1 : 1
            }
            index -= 1
        }
        return 0
    }
}
