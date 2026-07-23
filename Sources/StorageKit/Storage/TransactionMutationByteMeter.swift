import Synchronization

/// A typed failure raised before a transaction accepts more logical mutation
/// payload than the configured portable limit.
public enum TransactionMutationByteLimitError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible
{
    case invalidMaximum(Int)
    case alreadyConfigured
    case configurationAfterAdmission
    case exceeded(actual: Int, maximum: Int)

    public var description: String {
        switch self {
        case .invalidMaximum(let value):
            return "Transaction mutation byte limit must be positive; received \(value)"
        case .alreadyConfigured:
            return "Transaction mutation byte admission is already configured"
        case .configurationAfterAdmission:
            return "Transaction mutation byte admission cannot be configured after accepting a mutation"
        case .exceeded(let actual, let maximum):
            return "Transaction mutation payload contains \(actual) bytes, exceeding the limit of \(maximum)"
        }
    }
}

/// Counts logical mutation bytes without materializing keys or values.
///
/// A meter belongs to one transaction attempt. Retried attempts must receive a
/// fresh meter because the previous attempt's buffered mutations are discarded.
public final class TransactionMutationByteMeter: Sendable {
    private struct State: Sendable {
        var maximumBytes: Int?
        var consumedBytes: Int = 0
        var isConfigured = false
    }

    private let state = Mutex(State())

    /// Creates an unconfigured meter owned by one transaction attempt.
    public init() {}

    public init(
        maximumBytes: Int
    ) throws(TransactionMutationByteLimitError) {
        guard maximumBytes > 0 else {
            throw .invalidMaximum(maximumBytes)
        }
        state.withLock { state in
            state.maximumBytes = maximumBytes
            state.isConfigured = true
        }
    }

    public var maximumBytes: Int? {
        state.withLock { $0.maximumBytes }
    }

    public var consumedBytes: Int {
        state.withLock { $0.consumedBytes }
    }

    /// Configures the admission policy before the transaction accepts writes.
    /// A nil maximum explicitly selects an unbounded trusted transaction.
    public func configure(
        maximumBytes: Int?
    ) throws(TransactionMutationByteLimitError) {
        if let maximumBytes, maximumBytes <= 0 {
            throw .invalidMaximum(maximumBytes)
        }
        try state.withLock { state throws(TransactionMutationByteLimitError) in
            guard !state.isConfigured else {
                throw .alreadyConfigured
            }
            guard state.consumedBytes == 0 else {
                throw .configurationAfterAdmission
            }
            state.maximumBytes = maximumBytes
            state.isConfigured = true
        }
    }

    public func consume(
        _ byteCount: Int
    ) throws(TransactionMutationByteLimitError) {
        guard byteCount >= 0 else {
            throw .exceeded(
                actual: Int.max,
                maximum: maximumBytes ?? Int.max
            )
        }
        try state.withLock { state throws(TransactionMutationByteLimitError) in
            let addition = state.consumedBytes.addingReportingOverflow(byteCount)
            guard !addition.overflow else {
                throw .exceeded(
                    actual: Int.max,
                    maximum: state.maximumBytes ?? Int.max
                )
            }
            if let maximumBytes = state.maximumBytes,
               addition.partialValue > maximumBytes {
                throw .exceeded(
                    actual: addition.partialValue,
                    maximum: maximumBytes
                )
            }
            state.consumedBytes = addition.partialValue
        }
    }

    private static let operationTagByteCount = 1
    private static let lengthFieldByteCount = 8
    private static let mutationTypeByteCount = 4

    public func recordSet(
        key: Bytes,
        value: Bytes
    ) throws(TransactionMutationByteLimitError) {
        try consume(
            Self.operationTagByteCount,
            Self.lengthFieldByteCount,
            key.count,
            Self.lengthFieldByteCount,
            value.count
        )
    }

    public func recordClear(
        key: Bytes
    ) throws(TransactionMutationByteLimitError) {
        try consume(
            Self.operationTagByteCount,
            Self.lengthFieldByteCount,
            key.count
        )
    }

    public func recordClearRange(
        beginKey: Bytes,
        endKey: Bytes
    ) throws(TransactionMutationByteLimitError) {
        try consume(
            Self.operationTagByteCount,
            Self.lengthFieldByteCount,
            beginKey.count,
            Self.lengthFieldByteCount,
            endKey.count
        )
    }

    public func recordAtomic(
        key: Bytes,
        parameter: Bytes
    ) throws(TransactionMutationByteLimitError) {
        try consume(
            Self.operationTagByteCount,
            Self.mutationTypeByteCount,
            Self.lengthFieldByteCount,
            key.count,
            Self.lengthFieldByteCount,
            parameter.count
        )
    }

    private func consume(
        _ first: Int,
        _ second: Int = 0,
        _ third: Int = 0,
        _ fourth: Int = 0,
        _ fifth: Int = 0,
        _ sixth: Int = 0
    ) throws(TransactionMutationByteLimitError) {
        let maximum = maximumBytes ?? Int.max
        let firstSum = try Self.adding(first, second, maximum: maximum)
        let secondSum = try Self.adding(firstSum, third, maximum: maximum)
        let thirdSum = try Self.adding(secondSum, fourth, maximum: maximum)
        let fourthSum = try Self.adding(thirdSum, fifth, maximum: maximum)
        let total = try Self.adding(fourthSum, sixth, maximum: maximum)
        try consume(total)
    }

    private static func adding(
        _ lhs: Int,
        _ rhs: Int,
        maximum: Int
    ) throws(TransactionMutationByteLimitError) -> Int {
        let addition = lhs.addingReportingOverflow(rhs)
        guard !addition.overflow else {
            throw .exceeded(actual: Int.max, maximum: maximum)
        }
        return addition.partialValue
    }
}
