/// FoundationDB-compatible atomic mutation type used by the shared embedded core.
public enum EmbeddedMutationType: UInt8, Sendable, Hashable {
    case add = 1
    case setVersionstampedKey = 2
    case setVersionstampedValue = 3
    case bitOr = 4
    case bitAnd = 5
    case bitXor = 6
    case max = 7
    case min = 8
    case compareAndClear = 9
}

extension EmbeddedMutationType {
    public func encode(into writer: inout EmbeddedBinaryWriter) {
        writer.writeUInt8(rawValue)
    }

    public init(from reader: inout EmbeddedBinaryReader) throws(EmbeddedWireError) {
        let code = try reader.readUInt8()
        guard let value = EmbeddedMutationType(rawValue: code) else {
            throw EmbeddedWireError.unknownMutationType(code)
        }
        self = value
    }

    /// Apply this mutation to an existing value using FoundationDB atomic
    /// operation semantics.
    public func apply(to existing: [UInt8]?, param: [UInt8]) throws(EmbeddedMutationError) -> EmbeddedAtomicMutationResult {
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
            throw EmbeddedMutationError.versionstampRequiresCommitVersion
        }
    }

    private static func adjusted(_ value: [UInt8], to length: Int) -> [UInt8] {
        if value.count == length {
            return value
        }
        if value.count > length {
            return Array(value.prefix(length))
        }
        return value + Array(repeating: 0, count: length - value.count)
    }

    private static func compareLittleEndian(_ lhs: [UInt8], _ rhs: [UInt8]) -> Int {
        precondition(lhs.count == rhs.count, "Operands must be adjusted to equal length")
        var index = lhs.count
        while index > 0 {
            index -= 1
            if lhs[index] != rhs[index] {
                return lhs[index] < rhs[index] ? -1 : 1
            }
        }
        return 0
    }
}
