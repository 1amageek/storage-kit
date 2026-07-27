import DatabaseTypes

/// Result of applying an atomic mutation to an existing value.
public enum AtomicMutationResult: Sendable, Equatable {
    /// Store the given bytes as the new value.
    case set(ByteString)
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
    ///   version and cannot be evaluated before commit — always throws.
    ///
    /// - Parameters:
    ///   - existing: The current value for the key, or nil if absent.
    ///   - param: The mutation parameter.
    /// - Returns: The mutation outcome.
    /// - Throws: `StorageError(.invalidOperation)` for versionstamp mutations.
    public func apply(
        to existing: ByteString?,
        param: ByteString
    ) throws -> AtomicMutationResult {
        switch self {
        case .add:
            return .set(
                Self.combine(existing, param: param) {
                    current,
                    parameter,
                    carry in
                    let sum = UInt16(current) + UInt16(parameter) + carry
                    return (UInt8(truncatingIfNeeded: sum), sum >> 8)
                }
            )
        case .bitAnd:
            guard let existing else { return .set(param) }
            return .set(
                Self.combine(existing, param: param) {
                    current,
                    parameter,
                    _ in
                    (current & parameter, 0)
                }
            )
        case .bitOr:
            return .set(
                Self.combine(existing, param: param) {
                    current,
                    parameter,
                    _ in
                    (current | parameter, 0)
                }
            )
        case .bitXor:
            return .set(
                Self.combine(existing, param: param) {
                    current,
                    parameter,
                    _ in
                    (current ^ parameter, 0)
                }
            )
        case .max:
            let current = Self.adjusted(existing ?? [], to: param.count)
            return .set(
                Self.compareLittleEndian(current, param) >= 0
                    ? current
                    : param
            )
        case .min:
            guard let existing else { return .set(param) }
            let current = Self.adjusted(existing, to: param.count)
            return .set(
                Self.compareLittleEndian(current, param) <= 0
                    ? current
                    : param
            )
        case .compareAndClear:
            if let existing, existing == param {
                return .clear
            }
            return .unchanged
        case .setVersionstampedKey, .setVersionstampedValue:
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                message: "Versionstamp mutations require a commit version"
            )
        }
    }

    private static func adjusted(
        _ value: ByteString,
        to length: Int
    ) -> ByteString {
        if value.count == length {
            return value
        }
        if value.count > length {
            return value[
                value.startIndex..<(value.startIndex + length)
            ]
        }
        return ByteString.copying(count: length) { destination in
            destination.initializeMemory(as: UInt8.self, repeating: 0)
            value.withUnsafeBytes { source in
                guard !source.isEmpty else {
                    return
                }
                destination.copyMemory(from: source)
            }
        }
    }

    private static func combine(
        _ existing: ByteString?,
        param: ByteString,
        operation: (
            _ current: UInt8,
            _ parameter: UInt8,
            _ carry: UInt16
        ) -> (value: UInt8, carry: UInt16)
    ) -> ByteString {
        ByteString.copying(count: param.count) { destination in
            param.withUnsafeBytes { parameter in
                if let existing {
                    existing.withUnsafeBytes { current in
                        var carry: UInt16 = 0
                        for index in parameter.indices {
                            let currentByte = index < current.count
                                ? current[index]
                                : 0
                            let output = operation(
                                currentByte,
                                parameter[index],
                                carry
                            )
                            destination[index] = output.value
                            carry = output.carry
                        }
                    }
                } else {
                    var carry: UInt16 = 0
                    for index in parameter.indices {
                        let output = operation(
                            0,
                            parameter[index],
                            carry
                        )
                        destination[index] = output.value
                        carry = output.carry
                    }
                }
            }
        }
    }

    private static func compareLittleEndian(
        _ lhs: ByteString,
        _ rhs: ByteString
    ) -> Int {
        precondition(lhs.count == rhs.count)
        return lhs.withUnsafeBytes { lhsBytes in
            rhs.withUnsafeBytes { rhsBytes in
                var index = lhsBytes.count
                while index > 0 {
                    index -= 1
                    if lhsBytes[index] != rhsBytes[index] {
                        return lhsBytes[index] < rhsBytes[index] ? -1 : 1
                    }
                }
                return 0
            }
        }
    }
}
