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
    public func encode(into writer: inout EmbeddedWireWriter) {
        writer.writeUInt8(rawValue)
    }

    public init(from reader: inout EmbeddedWireReader) throws(EmbeddedWireError) {
        let code = try reader.readUInt8()
        guard let value = EmbeddedMutationType(rawValue: code) else {
            throw EmbeddedWireError.unknownMutationType(code)
        }
        self = value
    }

    /// Apply this mutation to an existing value using FoundationDB atomic
    /// operation semantics.
    public func apply(
        to existing: EmbeddedBytes?,
        param: EmbeddedBytes
    ) throws(EmbeddedMutationError) -> EmbeddedAtomicMutationResult {
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

    private static func adjusted(
        _ value: EmbeddedBytes,
        to length: Int
    ) -> EmbeddedBytes {
        if value.count == length {
            return value
        }
        if value.count > length {
            return value.slice(0..<length)
        }
        return EmbeddedBytes.copying(count: length) { destination in
            destination.initializeMemory(as: UInt8.self, repeating: 0)
            value.withUnsafeBytes { source in
                guard source.count > 0 else {
                    return
                }
                destination.copyMemory(from: source)
            }
        }
    }

    private static func combine(
        _ existing: EmbeddedBytes?,
        param: EmbeddedBytes,
        operation: (
            _ current: UInt8,
            _ parameter: UInt8,
            _ carry: UInt16
        ) -> (value: UInt8, carry: UInt16)
    ) -> EmbeddedBytes {
        EmbeddedBytes.copying(count: param.count) { destination in
            param.withUnsafeBytes { parameter in
                if let existing {
                    existing.withUnsafeBytes { current in
                        var carry: UInt16 = 0
                        for index in 0..<parameter.count {
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
                    for index in 0..<parameter.count {
                        let output = operation(0, parameter[index], carry)
                        destination[index] = output.value
                        carry = output.carry
                    }
                }
            }
        }
    }

    private static func compareLittleEndian(
        _ lhs: EmbeddedBytes,
        _ rhs: EmbeddedBytes
    ) -> Int {
        precondition(lhs.count == rhs.count, "Operands must be adjusted to equal length")
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
