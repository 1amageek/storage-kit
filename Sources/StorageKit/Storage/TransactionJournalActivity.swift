import DatabaseTypes
import Synchronization

/// Shared admission and byte accounting retained by cursors without their journal.
final class TransactionJournalActivity: Sendable {
    enum Phase: Sendable { case open, barrier, sealed, closed }
    private struct State: Sendable {
        var phase: Phase = .open
        var operations = 0
        var drain: TransactionOperationCompletion?
        var barrierCompletion: TransactionOperationCompletion?
        var work = 0
        var bytes = 0
    }
    private let state = Mutex(State())
    let limits: TransactionCheckpointLimits

    init(limits: TransactionCheckpointLimits) { self.limits = limits }

    func beginOperation(nested: Bool = false) throws {
        try state.withLock { state in
            switch state.phase {
            case .open: break
            case .barrier where nested: break
            case .barrier: throw TransactionCheckpointError.busy
            case .sealed, .closed: throw TransactionCheckpointError.closed
            }
            try Self.addWork(1, state: &state, maximum: limits.maximumWorkUnits)
            state.operations += 1
        }
    }

    func endOperation() {
        let drain = state.withLock { state -> TransactionOperationCompletion? in
            precondition(state.operations > 0)
            state.operations -= 1
            guard state.operations == 0 else { return nil }
            let drain = state.drain
            state.drain = nil
            return drain
        }
        drain?.succeed()
    }

    func requireOpen() throws {
        try state.withLock { state in
            switch state.phase {
            case .open: return
            case .barrier:
                guard let operation = TransactionJournalContext.operation,
                      operation.isActive, operation.journal.activity === self else {
                    throw TransactionCheckpointError.busy
                }
            case .sealed, .closed: throw TransactionCheckpointError.closed
            }
        }
    }

    func beginBarrier() throws -> TransactionOperationCompletion? {
        try state.withLock { state in
            switch state.phase {
            case .open: break
            case .barrier: throw TransactionCheckpointError.busy
            case .sealed, .closed: throw TransactionCheckpointError.closed
            }
            state.phase = .barrier
            state.barrierCompletion = TransactionOperationCompletion()
            guard state.operations > 0 else { return nil }
            let drain = TransactionOperationCompletion()
            state.drain = drain
            return drain
        }
    }

    var activeBarrier: TransactionOperationCompletion? { state.withLock { $0.barrierCompletion } }

    private func transition(to phase: Phase) {
        let completion = state.withLock { state in
            state.phase = phase
            let completion = state.barrierCompletion
            state.barrierCompletion = nil
            return completion
        }
        completion?.succeed()
    }

    func reopen() { transition(to: .open) }
    func seal() { transition(to: .sealed) }
    func close() { transition(to: .closed) }

    func consumeWork(_ units: Int) throws {
        try state.withLock {
            try Self.addWork(units, state: &$0, maximum: limits.maximumWorkUnits)
        }
    }

    private static func addWork(_ units: Int, state: inout State, maximum: Int) throws {
        let result = state.work.addingReportingOverflow(units)
        guard units >= 0, !result.overflow, result.partialValue <= maximum else {
            throw TransactionCheckpointError.limitExceeded(
                resource: .workUnits,
                actual: result.overflow ? Int.max : result.partialValue,
                maximum: maximum
            )
        }
        state.work = result.partialValue
    }

    var availableBytes: Int {
        state.withLock { limits.maximumRetainedBytes - $0.bytes }
    }

    func reserve(_ bytes: Int) throws {
        try state.withLock { state in
            let result = state.bytes.addingReportingOverflow(bytes)
            guard bytes >= 0, !result.overflow,
                  result.partialValue <= limits.maximumRetainedBytes else {
                throw TransactionCheckpointError.limitExceeded(
                    resource: .retainedBytes,
                    actual: result.overflow ? Int.max : result.partialValue,
                    maximum: limits.maximumRetainedBytes
                )
            }
            state.bytes = result.partialValue
        }
    }

    func release(_ bytes: Int) {
        state.withLock { state in
            precondition(bytes >= 0 && bytes <= state.bytes)
            state.bytes -= bytes
        }
    }

    /// Unknown backend owners are copied only at this explicit transfer boundary.
    func retaining(_ bytes: ByteString) throws -> (ByteString, Int) {
        if bytes.isStorageSelfContained, let retained = bytes.retainedByteCount {
            try reserve(retained)
            return (bytes, retained)
        }
        try reserve(bytes.count)
        // A future/row/frame slice can keep unknown storage alive. The exact
        // copied owner bounds journal retention independently of backend memory.
        let detached = ByteString.copying(count: bytes.count) { destination in
            bytes.withUnsafeBytes { destination.copyMemory(from: $0) }
        }
        return (detached, bytes.count)
    }
}
