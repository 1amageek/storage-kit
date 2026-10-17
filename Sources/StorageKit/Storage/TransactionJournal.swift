import DatabaseTypes
import Synchronization

/// One bounded, deferred byte-mutation owner inside a concrete transaction.
public final class TransactionJournal: TransactionCheckpointAccess, Sendable {
    private struct Entry: Sendable {
        let mutation: TransactionJournalMutation
        let bytes: Int
    }
    private struct Marker: Sendable {
        let identifier: UInt64
        let entries: Int
        let provenance: Int
        let suppressNext: Bool
    }
    private struct ProvenanceEntry: Sendable {
        let address: StorageAddress
        let prefix: ByteString
        let witness: DirectoryProvenance
    }
    private struct CursorEntry: Sendable {
        var cursor: KeyValueCursor
        let lifetime: TransactionJournalCursorLifetime
    }
    private struct State: Sendable {
        var entries: [Entry] = []
        var pendingEntries = 0
        var markers: [Marker] = []
        var nextMarker: UInt64 = 0
        var suppressNext = false
        var provenance: [ProvenanceEntry] = []
        var cursors: [CursorEntry] = []
    }

    private let identity = TransactionCheckpointIdentity()
    private let state = Mutex(State())
    let activity: TransactionJournalActivity
    let backend: StorageBackend
    private let mutationByteMeter: TransactionMutationByteMeter

    public init(
        limits: TransactionCheckpointLimits,
        backend: StorageBackend,
        mutationByteMeter: TransactionMutationByteMeter
    ) {
        self.activity = TransactionJournalActivity(limits: limits)
        self.backend = backend
        self.mutationByteMeter = mutationByteMeter
    }

    package static var current: TransactionJournal? {
        guard let operation = TransactionJournalContext.operation,
              operation.isActive else { return nil }
        return operation.journal
    }

    private func beginOperation() throws {
        let operation = TransactionJournalContext.operation
        let matching = operation?.journal === self
        if matching, operation?.isActive != true { throw TransactionCheckpointError.closed }
        try activity.beginOperation(nested: matching)
    }

    public func performSynchronousOperation<Result>(
        _ operation: () throws -> Result
    ) throws -> Result {
        try beginOperation()
        let lease = TransactionJournalOperation(journal: self)
        defer {
            lease.finish()
            activity.endOperation()
        }
        return try TransactionJournalContext.$operation.withValue(lease, operation: operation)
    }

    public func performOperation(
        _ operation: @escaping @Sendable () async throws -> Void
    ) async throws {
        try beginOperation()
        let lease = TransactionJournalOperation(journal: self)
        defer {
            lease.finish()
            activity.endOperation()
        }
        try await TransactionJournalContext.$operation.withValue(lease) {
            try await operation()
        }
    }

    /// Admits a synchronous adapter-owned copy before constructing its operands.
    public func append(
        copyingByteCount: Int,
        _ makeMutation: () throws -> TransactionJournalMutation
    ) throws {
        try beginOperation()
        defer { activity.endOperation() }
        try activity.reserve(copyingByteCount)
        defer { activity.release(copyingByteCount) }
        let lease = TransactionJournalOperation(journal: self)
        defer { lease.finish() }
        try TransactionJournalContext.$operation.withValue(lease) {
            try append(makeMutation())
        }
    }

    public func append(_ mutation: TransactionJournalMutation) throws {
        try beginOperation()
        defer { activity.endOperation() }
        try state.withLock { state in
            try requireEntryCapacity(state.entries.count + state.provenance.count + state.pendingEntries + 1)
            state.pendingEntries += 1
        }
        var charge = 0
        var ownsEntryReservation = true
        do {
            // Borrowing an external ByteStringOwner can execute user code.
            // Reserve the entry first, then materialize outside every journal lock.
            let retained = try retain(mutation, suppressNext: false, charge: &charge)
            try state.withLock { state in
                state.pendingEntries -= 1
                ownsEntryReservation = false
                let admitted = Self.applyingSuppression(state.suppressNext, to: retained)
                switch admitted {
                case .set(let key, let value, _):
                    try mutationByteMeter.recordSet(key: key, value: value)
                case .clear(let key, _):
                    try mutationByteMeter.recordClear(key: key)
                case .clearRange(let begin, let end, _):
                    try mutationByteMeter.recordClearRange(beginKey: begin, endKey: end)
                case .atomic(let key, let parameter, _, _):
                    try mutationByteMeter.recordAtomic(key: key, parameter: parameter)
                case .writeConflict:
                    break
                }
                state.entries.append(Entry(mutation: admitted, bytes: charge))
                if case .writeConflict = admitted {} else { state.suppressNext = false }
            }
        } catch {
            if ownsEntryReservation { state.withLock { $0.pendingEntries -= 1 } }
            activity.release(charge)
            throw error
        }
    }

    private static func applyingSuppression(_ pending: Bool, to mutation: TransactionJournalMutation) -> TransactionJournalMutation {
        guard pending else { return mutation }
        switch mutation {
        case .set(let key, let value, _): return .set(key: key, value: value, suppressWriteConflict: true)
        case .clear(let key, _): return .clear(key: key, suppressWriteConflict: true)
        case .clearRange(let begin, let end, _): return .clearRange(begin: begin, end: end, suppressWriteConflict: true)
        case .atomic(let key, let parameter, let type, _): return .atomic(key: key, parameter: parameter, type: type, suppressWriteConflict: true)
        case .writeConflict: return mutation
        }
    }

    public func suppressNextWriteConflict() throws {
        try beginOperation()
        defer { activity.endOperation() }
        state.withLock { $0.suppressNext = true }
    }

    private func retain(
        _ mutation: TransactionJournalMutation,
        suppressNext: Bool,
        charge: inout Int
    ) throws -> TransactionJournalMutation {
        func value(_ bytes: ByteString) throws -> ByteString {
            let (retained, count) = try activity.retaining(bytes)
            charge += count
            return retained
        }
        switch mutation {
        case .set(let key, let bytes, let suppress):
            return .set(key: try value(key), value: try value(bytes), suppressWriteConflict: suppress || suppressNext)
        case .clear(let key, let suppress):
            return .clear(key: try value(key), suppressWriteConflict: suppress || suppressNext)
        case .clearRange(let begin, let end, let suppress):
            return .clearRange(begin: try value(begin), end: try value(end), suppressWriteConflict: suppress || suppressNext)
        case .atomic(let key, let parameter, let type, let suppress):
            return .atomic(key: try value(key), parameter: try value(parameter), type: type, suppressWriteConflict: suppress || suppressNext)
        case .writeConflict(let begin, let end):
            return .writeConflict(begin: try value(begin), end: try value(end))
        }
    }

    public func createCheckpoint() async throws -> TransactionCheckpoint {
        try await barrier {
            try state.withLock { state in
                let count = state.markers.count + 1
                guard count <= activity.limits.maximumDepth else {
                    throw TransactionCheckpointError.limitExceeded(
                        resource: .depth, actual: count, maximum: activity.limits.maximumDepth
                    )
                }
                guard state.nextMarker < UInt64.max else { throw TransactionCheckpointError.invalidToken }
                let identifier = state.nextMarker
                state.nextMarker += 1
                state.markers.append(Marker(
                    identifier: identifier, entries: state.entries.count,
                    provenance: state.provenance.count, suppressNext: state.suppressNext
                ))
                return TransactionCheckpoint(attempt: identity, identifier: identifier)
            }
        }
    }

    public func rollback(to checkpoint: TransactionCheckpoint) async throws {
        try validate(checkpoint)
        try await barrier {
            try state.withLock { state in
                let index = try markerIndex(checkpoint, state: state)
                let marker = state.markers[index]
                for entry in state.entries.dropFirst(marker.entries) { activity.release(entry.bytes) }
                state.entries.removeSubrange(marker.entries...)
                for entry in state.provenance.dropFirst(marker.provenance) { entry.witness.invalidate() }
                state.provenance.removeSubrange(marker.provenance...)
                state.markers.removeSubrange((index + 1)...)
                state.suppressNext = marker.suppressNext
            }
        }
    }

    public func release(_ checkpoint: TransactionCheckpoint) async throws {
        try validate(checkpoint)
        try await barrier {
            try state.withLock { state in
                let index = try markerIndex(checkpoint, state: state)
                state.markers.removeSubrange(index...)
            }
        }
    }

    private func validate(_ checkpoint: TransactionCheckpoint) throws {
        try state.withLock { _ = try markerIndex(checkpoint, state: $0) }
    }

    private func markerIndex(_ checkpoint: TransactionCheckpoint, state: State) throws -> Int {
        guard checkpoint.attempt === identity else { throw TransactionCheckpointError.foreignToken }
        guard let index = state.markers.firstIndex(where: { $0.identifier == checkpoint.identifier }) else {
            throw TransactionCheckpointError.invalidToken
        }
        return index
    }

    private func barrier<Result>(_ operation: () throws -> Result, sealing: Bool = false, chargeWork: Bool = true) async throws -> Result {
        if TransactionJournalContext.operation?.journal === self {
            throw TransactionCheckpointError.barrierInsideOperation
        }
        if chargeWork { try activity.consumeWork(1) }
        let drain = try activity.beginBarrier()
        do {
            try await drain?.wait()
            var cursors = state.withLock { state in
                let cursors = state.cursors
                state.cursors.removeAll(keepingCapacity: false)
                return cursors
            }
            var failures: [any Error] = []
            for index in cursors.indices.reversed() {
                do { try await cursors[index].cursor.completeCleanup() }
                catch { failures.append(error) }
            }
            guard failures.isEmpty else {
                if failures.count == 1 { throw failures[0] }
                throw StorageRangeCleanupError(iterationError: failures[0], cleanupError: failures[1])
            }
        } catch {
            activity.close()
            throw error
        }
        do {
            let result = try operation()
            if sealing { activity.seal() } else { activity.reopen() }
            return result
        } catch {
            // Token/depth validation is caller-owned and changes no view.
            activity.reopen()
            throw error
        }
    }

    public func sealForCommit() async throws -> [TransactionJournalMutation] {
        try await barrier({ state.withLock { $0.entries.map { $0.mutation } } }, sealing: true)
    }

    public func close(committed: Bool) async throws {
        if committed {
            activity.close()
            clear(publish: true)
        } else {
            while true {
                do {
                    try await barrier({ () }, sealing: true, chargeWork: false)
                    break
                } catch TransactionCheckpointError.closed {
                    break
                } catch TransactionCheckpointError.busy {
                    if let completion = activity.activeBarrier { try await completion.wait() }
                }
            }
            activity.close()
            clear(publish: false)
        }
    }

    public func closeWithUnknownCommit() async throws {
        activity.close()
        clear(publish: false)
    }

    private func clear(publish: Bool) {
        state.withLock { state in
            for entry in state.entries { activity.release(entry.bytes) }
            for entry in state.provenance {
                if publish { entry.witness.publish() } else { entry.witness.invalidate() }
            }
            state.entries.removeAll(keepingCapacity: false)
            state.markers.removeAll(keepingCapacity: false)
            state.provenance.removeAll(keepingCapacity: false)
            state.suppressNext = false
        }
    }

    package func recordDirectoryProvenance(address: StorageAddress, prefix: ByteString) throws -> DirectoryProvenance {
        try state.withLock { state in
            try requireEntryCapacity(state.entries.count + state.provenance.count + state.pendingEntries + 1)
            try activity.consumeWork(1)
            let witness = DirectoryProvenance()
            state.provenance.append(ProvenanceEntry(address: address, prefix: prefix, witness: witness))
            return witness
        }
    }

    package func directoryProvenance(address: StorageAddress, prefix: ByteString) -> DirectoryProvenance? {
        let entries = state.withLock { $0.provenance }
        return entries.last { $0.address == address && $0.prefix == prefix }?.witness
    }

    package func chargeDirectoryDependencies(_ count: Int) throws { try activity.consumeWork(count) }

    private func requireEntryCapacity(_ count: Int) throws {
        guard count <= activity.limits.maximumEntries else {
            throw TransactionCheckpointError.limitExceeded(
                resource: .entries, actual: count, maximum: activity.limits.maximumEntries
            )
        }
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int? = nil,
        using access: any TransactionJournalBackendAccess
    ) async throws -> ByteString? {
        try beginOperation()
        defer { activity.endOperation() }
        let mutations = state.withLock { $0.entries.map { $0.mutation } }
        return try await TransactionJournalView.value(
            for: key, snapshot: snapshot, maximumByteCount: maximumByteCount,
            mutations: mutations, access: access, activity: activity, backend: backend
        )
    }

    public func getKey(
        selector: KeySelector,
        snapshot: Bool,
        using access: any TransactionJournalBackendAccess
    ) async throws -> ByteString? {
        let (nextOffset, overflow) = selector.offset.addingReportingOverflow(1)
        guard !overflow else { throw TransactionCheckpointError.invalidLimits }
        var cursor = rangeCursor(
            from: selector,
            to: KeySelector(key: selector.key, orEqual: selector.orEqual, offset: nextOffset),
            limit: 1, reverse: false, snapshot: snapshot, streamingMode: .exact,
            using: access
        )
        do {
            let key = try await cursor.next()?.0
            try await cursor.finish()
            return key
        } catch {
            let operationError = error
            do { try await cursor.finish() }
            catch { throw StorageRangeCleanupError(iterationError: operationError, cleanupError: error) }
            throw operationError
        }
    }

    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode,
        using access: any TransactionJournalBackendAccess
    ) -> KeyValueCursor {
        do {
            try beginOperation()
            defer { activity.endOperation() }
            let lifetime = TransactionJournalCursorLifetime()
            let mutations = state.withLock { $0.entries.map { $0.mutation } }
            let cursor = KeyValueCursor(consuming: TransactionJournalRangeResult(
                begin: begin, end: end, limit: limit, reverse: reverse,
                snapshot: snapshot, access: access, mutations: mutations,
                activity: activity, backend: backend, lifetime: lifetime
            )).validatingScope { [activity] in try activity.requireOpen() }
            state.withLock { state in
                state.cursors.removeAll { $0.lifetime.isFinished }
                state.cursors.append(CursorEntry(cursor: cursor, lifetime: lifetime))
            }
            return cursor
        } catch {
            return KeyValueCursor(validatingScope: { throw error })
        }
    }
}
