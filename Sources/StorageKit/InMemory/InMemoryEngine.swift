import DatabaseTypes
import Synchronization

private enum InMemoryConflictCutSide: Sendable {
    case before
    case after
}

extension InMemoryTransaction {
    public func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await TransactionKeySelection.resolve(
            selector,
            in: self,
            snapshot: snapshot
        )
    }

    public func getValue(for key: ByteString) async throws -> ByteString? {
        try await getValue(for: key, snapshot: false)
    }

    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        KeyValueCursor(
            consuming: getRange(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        )
    }

    public func collectRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) async throws -> [(ByteString, ByteString)] {
        try await TransactionRangeCollection.collect(
            using: self,
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }
}

/// A position immediately before or after one exact key. Cut-based ranges
/// represent point keys and strict KeySelector boundaries without constructing
/// synthetic `key + 0x00` buffers.
private struct InMemoryConflictCut: Sendable {
    let key: ByteString
    let side: InMemoryConflictCutSide

    static func compare(
        _ left: InMemoryConflictCut,
        _ right: InMemoryConflictCut
    ) -> Int {
        let keyComparison = compareBytes(left.key, right.key)
        guard keyComparison == 0 else { return keyComparison }
        switch (left.side, right.side) {
        case (.before, .after):
            return -1
        case (.after, .before):
            return 1
        case (.before, .before), (.after, .after):
            return 0
        }
    }
}

/// A non-empty half-open conflict range. Nil lower and upper cuts mean negative
/// and positive infinity respectively.
private struct InMemoryConflictRegion: Sendable {
    let lower: InMemoryConflictCut?
    let upper: InMemoryConflictCut?

    static var entireKeyspace: InMemoryConflictRegion {
        InMemoryConflictRegion(lower: nil, upper: nil)
    }

    static func point(_ key: ByteString) -> InMemoryConflictRegion {
        InMemoryConflictRegion(
            lower: InMemoryConflictCut(key: key, side: .before),
            upper: InMemoryConflictCut(key: key, side: .after)
        )
    }

    static func halfOpen(begin: ByteString, end: ByteString) -> InMemoryConflictRegion? {
        let lower = InMemoryConflictCut(key: begin, side: .before)
        let upper = InMemoryConflictCut(key: end, side: .before)
        guard InMemoryConflictCut.compare(lower, upper) < 0 else {
            return nil
        }
        return InMemoryConflictRegion(lower: lower, upper: upper)
    }

    static func between(
        _ lower: InMemoryConflictCut,
        _ upper: InMemoryConflictCut
    ) -> InMemoryConflictRegion? {
        guard InMemoryConflictCut.compare(lower, upper) < 0 else {
            return nil
        }
        return InMemoryConflictRegion(lower: lower, upper: upper)
    }

    func overlaps(_ other: InMemoryConflictRegion) -> Bool {
        guard Self.lowerPrecedesUpper(lower, other.upper) else {
            return false
        }
        return Self.lowerPrecedesUpper(other.lower, upper)
    }

    private static func lowerPrecedesUpper(
        _ lower: InMemoryConflictCut?,
        _ upper: InMemoryConflictCut?
    ) -> Bool {
        guard let lower else { return true }
        guard let upper else { return true }
        return InMemoryConflictCut.compare(lower, upper) < 0
    }
}

/// In-memory KV storage for testing and standalone client use.
///
/// Maintains lexicographic order via `SortedKeyValueStore`.
/// Range scans locate the start position via binary search and iterate to end.
///
/// ## Thread safety
/// Uses Mutex for exclusive access (no I/O, memory access only).
public final class InMemoryEngine: StorageEngine, Sendable {

    /// No configuration needed for in-memory storage.
    public struct Configuration: Sendable {
        public init() {}
    }

    public typealias TransactionType = InMemoryTransaction

    struct StoreState: Sendable {
        var store = SortedKeyValueStore()
        var version: Int64 = 0
        var nextTransactionIdentifier: UInt64 = 0
        var activeSnapshotVersions: [UInt64: Int64] = [:]
        fileprivate var committedWriteHistory: [CommittedWriteSet] = []

        var activeTransactionCount: Int { activeSnapshotVersions.count }
        var retainedConflictVersionCount: Int { committedWriteHistory.count }

        mutating func registerTransaction() throws -> UInt64 {
            let identifier = nextTransactionIdentifier
            let (nextIdentifier, overflow) = identifier.addingReportingOverflow(1)
            guard !overflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .beginTransaction,
                    backend: .inMemory,
                    message: "In-memory transaction identifier exhausted"
                )
            }
            nextTransactionIdentifier = nextIdentifier
            activeSnapshotVersions[identifier] = version
            return identifier
        }

        mutating func releaseTransaction(_ identifier: UInt64) {
            activeSnapshotVersions.removeValue(forKey: identifier)
            pruneCommittedWriteHistory()
        }

        private mutating func pruneCommittedWriteHistory() {
            guard let minimumSnapshotVersion = activeSnapshotVersions.values.min() else {
                committedWriteHistory.removeAll(keepingCapacity: false)
                return
            }
            var firstRequiredIndex = committedWriteHistory.startIndex
            while firstRequiredIndex < committedWriteHistory.endIndex,
                  committedWriteHistory[firstRequiredIndex].version
                    <= minimumSnapshotVersion {
                firstRequiredIndex += 1
            }
            guard firstRequiredIndex > committedWriteHistory.startIndex else {
                return
            }
            committedWriteHistory.removeSubrange(
                committedWriteHistory.startIndex..<firstRequiredIndex
            )
        }
    }

    fileprivate struct CommittedWriteSet: Sendable {
        let version: Int64
        let regions: [InMemoryConflictRegion]
    }

    /// Sorted KV store and its commit version. Both change under one lock so a
    /// transaction can never observe a version that belongs to another state.
    let _store: Mutex<StoreState>
    public let transactionDomain: StorageTransactionDomain
    public let directoryAccess: any DirectoryAccess
    private let storageLifecycle = StorageEngineLifecycle()

    public init(configuration: Configuration = .init()) {
        let domain = StorageTransactionDomain()
        self._store = Mutex(StoreState())
        self.transactionDomain = domain
        self.directoryAccess = KeyValueDirectoryCatalog(
            transactionDomain: domain,
            backend: .inMemory
        )
    }

    public func createTransaction() throws -> InMemoryTransaction {
        try storageLifecycle.withActiveAdmission(
            backend: .inMemory,
            operation: .beginTransaction
        ) {
            let snapshot = try _store.withLock { state in
                let transactionIdentifier = try state.registerTransaction()
                return (
                    store: SortedKeyValueStore(state.store.entries),
                    version: state.version,
                    transactionIdentifier: transactionIdentifier
                )
            }
            return InMemoryTransaction(
                engine: self,
                snapshot: snapshot.store,
                snapshotVersion: snapshot.version,
                transactionIdentifier: snapshot.transactionIdentifier
            )
        }
    }

    fileprivate func releaseTransaction(_ identifier: UInt64) {
        _store.withLock { state in
            state.releaseTransaction(identifier)
        }
    }

    /// Current store size (for testing).
    public var count: Int {
        _store.withLock { $0.store.count }
    }

    public func requestShutdown() {
        transactionDomain.requestShutdown()
        storageLifecycle.requestShutdown()
    }

    public func waitUntilShutdown() async {
        requestShutdown()
        await storageLifecycle.waitUntilShutdown()
    }
}

/// Transaction implementation for InMemoryEngine.
///
/// Uses a read snapshot + write buffer approach.
/// Applies changes to the main store on commit.
public final class InMemoryTransaction: Transaction, Sendable {

    public typealias RangeResult = KeyValueRangeResult

    public static let declaredCapabilities = TransactionCapabilities(
            transactionCheckpoints: true,
            readVersion: true,
            committedVersion: true,
            committedVersionstamp: true
    )

    public var capabilities: TransactionCapabilities { Self.declaredCapabilities }
    public var compaction: StorageCompactionAccess? { nil }

    public var mutationByteLimit: Int? { mutationByteMeter.maximumBytes }
    public var transactionDomain: StorageTransactionDomain {
        engine.transactionDomain
    }

    public var storageFailure: StorageError? {
        _state.value.withLock { state in
            guard case .failed(let error) = state.lifecycle else { return nil }
            return error as? StorageError
        }
    }

    private let engine: InMemoryEngine
    private let snapshot: SortedKeyValueStore
    private let snapshotVersion: Int64
    private let transactionIdentifier: UInt64
    private let mutationByteMeter = TransactionMutationByteMeter()
    private let versionstampCompletion = TransactionVersionstampCompletion()

    private struct MutableState: Sendable {
        var journal: TransactionJournal?
        var hasAdmittedOperation = false
        var writeBuffer: [WriteOp] = []
        var readConflictRegions: [InMemoryConflictRegion] = []
        var writeConflictRegions: [InMemoryConflictRegion] = []
        var lifecycle: Lifecycle = .open
        var committedVersion: Int64?
    }

    private enum Lifecycle: Sendable {
        case open
        case committing(TransactionOperationCompletion)
        case committed
        case cancelling(TransactionOperationCompletion)
        case cancelled
        case failed(any Error)
    }
    private final class StateOwner: Sendable {
        let value: Mutex<MutableState>
        init(_ state: MutableState) { value = Mutex(state) }
    }
    private let _state: StateOwner

    private enum WriteOp: Sendable {
        case set(key: ByteString, value: ByteString)
        case clear(key: ByteString)
        case clearRange(begin: ByteString, end: ByteString)
        case atomic(key: ByteString, param: ByteString, mutationType: MutationType)
    }

    private struct CommitPayload: Sendable {
        let operations: [WriteOp]
        let readConflictRegions: [InMemoryConflictRegion]
        let writeConflictRegions: [InMemoryConflictRegion]
    }

    init(
        engine: InMemoryEngine,
        snapshot: SortedKeyValueStore,
        snapshotVersion: Int64,
        transactionIdentifier: UInt64
    ) {
        self.engine = engine
        self.snapshot = snapshot
        self.snapshotVersion = snapshotVersion
        self.transactionIdentifier = transactionIdentifier
        self._state = StateOwner(MutableState())
    }

    deinit {
        engine.releaseTransaction(transactionIdentifier)
    }

    public var checkpoints: (any TransactionCheckpointAccess)? {
        _state.value.withLock { $0.journal }
    }

    public func configureCheckpoints(limits: TransactionCheckpointLimits) throws {
        try _state.value.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .beginTransaction)
            guard !state.hasAdmittedOperation, state.journal == nil else {
                throw TransactionCheckpointError.configurationAfterAdmission
            }
            state.journal = TransactionJournal(limits: limits, backend: .inMemory, mutationByteMeter: mutationByteMeter)
        }
    }

    public func performOperation(_ operation: @escaping @Sendable () async throws -> Void) async throws {
        if let journal = try admittedJournal(operation: .execute) {
            try await journal.performOperation(operation)
        } else { try await operation() }
    }

    private func admittedJournal(operation: StorageOperation) throws -> TransactionJournal? {
        try _state.value.withLock { state in
            let admitted = state.journal.map { TransactionJournal.current === $0 } ?? false
            switch state.lifecycle {
            case .committing where admitted, .cancelling where admitted:
                break
            default:
                try Self.validateOpen(state.lifecycle, operation: operation)
            }
            state.hasAdmittedOperation = true
            return state.journal
        }
    }

    private var rawCheckpointAccess: RawCheckpointAccess {
        RawCheckpointAccess(snapshot: snapshot, state: _state)
    }

    public func configureMutationByteLimit(maximumBytes: Int?) throws {
        try mutationByteMeter.configure(maximumBytes: maximumBytes)
    }

    // MARK: - Read

    public func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        if let journal = try admittedJournal(operation: .read) {
            return try await journal.getValue(for: key, snapshot: snapshot, using: rawCheckpointAccess)
        }
        return try readValue(for: key, snapshot: snapshot)
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        guard maximumByteCount >= 0 else {
            throw StorageError.invalidPointReadMaximum(
                maximumByteCount,
                backend: .inMemory
            )
        }
        if let journal = try admittedJournal(operation: .read) {
            return try await journal.getValue(for: key, snapshot: snapshot, maximumByteCount: maximumByteCount, using: rawCheckpointAccess)
        }
        let value = try readValue(for: key, snapshot: snapshot)
        guard let value else { return nil }
        guard value.count <= maximumByteCount else {
            throw StorageError.pointReadValueTooLarge(
                observedByteCount: value.count,
                maximumByteCount: maximumByteCount,
                backend: .inMemory
            )
        }
        return value
    }

    private func readValue(
        for key: ByteString,
        snapshot: Bool
    ) throws -> ByteString? {
        try _state.value.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .read)
            if !snapshot {
                state.readConflictRegions.append(.point(key))
            }

            // Replay the write buffer in order on top of the snapshot value.
            // Atomic mutations depend on preceding operations, so forward
            // replay is required.
            var value = self.snapshot.get(key)
            for operation in state.writeBuffer {
                switch operation {
                case .set(let operationKey, let operationValue)
                    where operationKey == key:
                    value = operationValue
                case .clear(let operationKey) where operationKey == key:
                    value = nil
                case .clearRange(let begin, let end)
                    where compareBytes(key, begin) >= 0
                        && compareBytes(key, end) < 0:
                    value = nil
                case .atomic(
                    let operationKey,
                    let parameter,
                    let mutationType
                ) where operationKey == key:
                    switch try mutationType.apply(to: value, param: parameter) {
                    case .set(let bytes):
                        value = bytes
                    case .clear:
                        value = nil
                    case .unchanged:
                        break
                    }
                default:
                    continue
                }
            }
            return value
        }
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueRangeResult {
        do {
            if let journal = try admittedJournal(operation: .rangeRead) {
                return KeyValueRangeResult(cursor: journal.rangeCursor(
                    from: begin, to: end, limit: limit, reverse: reverse, snapshot: snapshot,
                    streamingMode: streamingMode, using: rawCheckpointAccess
                ))
            }
            return try _state.value.withLock { state throws in
                try Self.validateOpen(state.lifecycle, operation: .rangeRead)
                if !snapshot,
                   let conflictRegion = Self.readConflictRegion(
                       from: begin,
                       to: end
                   ) {
                    state.readConflictRegions.append(conflictRegion)
                }

                // Build the transaction view by applying pending writes to the
                // immutable snapshot. The backing array remains shared when the
                // write buffer is empty.
                var effective = self.snapshot
                for operation in state.writeBuffer {
                    switch operation {
                    case .set(let key, let value):
                        effective.set(key, value)
                    case .clear(let key):
                        effective.delete(key)
                    case .clearRange(let rangeBegin, let rangeEnd):
                        effective.deleteRange(begin: rangeBegin, end: rangeEnd)
                    case .atomic(let key, let parameter, let mutationType):
                        switch try mutationType.apply(
                            to: effective.get(key),
                            param: parameter
                        ) {
                        case .set(let bytes):
                            effective.set(key, bytes)
                        case .clear:
                            effective.delete(key)
                        case .unchanged:
                            break
                        }
                    }
                }

                // Resolve KeySelectors using the FDB-compatible algorithm.
                let allKeys = effective.keys
                let startIndex = begin.resolve(in: allKeys)
                let endIndex = end.resolve(in: allKeys)

                guard startIndex < endIndex else {
                    return KeyValueRangeResult([])
                }

                let slice = effective.slice(startIndex..<endIndex)
                var results: [(key: ByteString, value: ByteString)]

                if reverse {
                    results = Array(slice.reversed())
                } else {
                    results = Array(slice)
                }

                if limit > 0 && results.count > limit {
                    results.removeSubrange(limit..<results.endIndex)
                }

                return KeyValueRangeResult(results)
            }
        } catch {
            return KeyValueRangeResult(error: error)
        }
    }

    // MARK: - Write

    public func setValue(_ value: ByteString, for key: ByteString) throws {
        if let journal = try admittedJournal(operation: .write) {
            try journal.append(.set(key: key, value: value, suppressWriteConflict: false)); return
        }
        try _state.value.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .write)
            try mutationByteMeter.recordSet(
                key: key,
                value: value
            )
            state.writeBuffer.append(.set(key: key, value: value))
            state.writeConflictRegions.append(.point(key))
        }
    }

    public func clear(key: ByteString) throws {
        if let journal = try admittedJournal(operation: .delete) {
            try journal.append(.clear(key: key, suppressWriteConflict: false)); return
        }
        try _state.value.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .delete)
            try mutationByteMeter.recordClear(key: key)
            state.writeBuffer.append(.clear(key: key))
            state.writeConflictRegions.append(.point(key))
        }
    }

    public func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        if let journal = try admittedJournal(operation: .deleteRange) {
            try journal.append(.clearRange(begin: beginKey, end: endKey, suppressWriteConflict: false)); return
        }
        try _state.value.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .deleteRange)
            try mutationByteMeter.recordClearRange(
                beginKey: beginKey,
                endKey: endKey
            )
            state.writeBuffer.append(.clearRange(begin: beginKey, end: endKey))
            if let conflictRegion = InMemoryConflictRegion.halfOpen(
                begin: beginKey,
                end: endKey
            ) {
                state.writeConflictRegions.append(conflictRegion)
            }
        }
    }

    // MARK: - Atomic Operations

    public func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        if let journal = try admittedJournal(operation: .write) {
            guard mutationType != .setVersionstampedKey, mutationType != .setVersionstampedValue else {
                throw StorageError.unsupportedOperation("In-memory versionstamped mutations are unavailable", operation: .write, backend: .inMemory)
            }
            try journal.append(.atomic(key: key, parameter: param, type: mutationType, suppressWriteConflict: false)); return
        }
        try _state.value.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .write)
            try mutationByteMeter.recordAtomic(
                key: key,
                parameter: param
            )
            state.writeBuffer.append(.atomic(key: key, param: param, mutationType: mutationType))
            state.writeConflictRegions.append(.point(key))
        }
    }

    public func addConflictRange(
        beginKey: ByteString,
        endKey: ByteString,
        type: ConflictRangeType
    ) throws {
        guard let conflictRegion = InMemoryConflictRegion.halfOpen(
            begin: beginKey,
            end: endKey
        ) else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .inMemory,
                message: "Conflict range must be non-empty and ordered"
            )
        }
        if let journal = try admittedJournal(operation: .write) {
            switch type {
            case .write:
                try journal.append(.writeConflict(begin: beginKey, end: endKey))
            case .read:
                try journal.performSynchronousOperation {
                    _state.value.withLock { $0.readConflictRegions.append(conflictRegion) }
                }
            }
            return
        }
        try _state.value.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .write)
            switch type {
            case .read:
                state.readConflictRegions.append(conflictRegion)
            case .write:
                state.writeConflictRegions.append(conflictRegion)
            }
        }
    }

    // MARK: - Transaction Management

    public func commit() async throws {
        enum Start {
            case leader(TransactionOperationCompletion, CommitPayload)
            case wait(TransactionOperationCompletion)
            case committed
            case failed(any Error)
        }

        let start = _state.value.withLock { state -> Start in
            switch state.lifecycle {
            case .open:
                let completion = TransactionOperationCompletion()
                let payload = CommitPayload(
                    operations: state.writeBuffer,
                    readConflictRegions: state.readConflictRegions,
                    writeConflictRegions: state.writeConflictRegions
                )
                state.writeBuffer.removeAll(keepingCapacity: false)
                state.readConflictRegions.removeAll(keepingCapacity: false)
                state.writeConflictRegions.removeAll(keepingCapacity: false)
                state.lifecycle = .committing(completion)
                return .leader(completion, payload)
            case .committing(let completion):
                return .wait(completion)
            case .committed:
                return .committed
            case .cancelling(let completion):
                return .wait(completion)
            case .cancelled:
                return .failed(Self.stateError("Transaction cancelled", operation: .commit))
            case .failed(let error):
                return .failed(error)
            }
        }

        switch start {
        case .leader(let completion, let payload):
            let journal = _state.value.withLock { $0.journal }
            let finalPayload: CommitPayload
            do {
                if let journal {
                    let entries = try await journal.sealForCommit()
                    finalPayload = checkpointPayload(entries, original: payload)
                } else { finalPayload = payload }
            } catch {
                _state.value.withLock { $0.lifecycle = .failed(error) }
                versionstampCompletion.fail(error)
                completion.fail(error)
                throw error
            }
            let result = commitOperations(finalPayload)
            do {
                if let journal {
                    if case .success = result { try await journal.close(committed: true) }
                    else { try await journal.close(committed: false) }
                }
            } catch {
                let failure: any Error
                if case .failure(let operationError) = result {
                    failure = StorageTransactionCleanupError(operationError: operationError, cancellationError: error)
                } else { failure = error }
                _state.value.withLock { $0.lifecycle = .failed(failure) }
                versionstampCompletion.fail(failure)
                completion.fail(failure)
                throw failure
            }
            _state.value.withLock { state in
                switch result {
                case .success(let committedVersion):
                    state.lifecycle = .committed
                    state.committedVersion = committedVersion
                case .failure(let error):
                    state.lifecycle = .failed(error)
                }
            }
            switch Self.versionstampResult(for: result) {
            case .success(let versionstamp):
                versionstampCompletion.succeed(versionstamp)
            case .failure(let error):
                versionstampCompletion.fail(error)
            }
            switch result {
            case .success:
                completion.succeed()
            case .failure(let error):
                completion.fail(error)
            }
            if case .failure(let error) = result {
                throw error
            }
        case .wait(let completion):
            try await completion.wait()
            let lifecycle = _state.value.withLock { $0.lifecycle }
            if case .cancelled = lifecycle {
                throw Self.stateError("Transaction cancelled", operation: .commit)
            }
        case .committed:
            return
        case .failed(let error):
            throw error
        }
    }

    public func cancel() async throws {
        enum Start {
            case leader(TransactionOperationCompletion)
            case waitForCancellation(TransactionOperationCompletion)
            case waitForCommit(TransactionOperationCompletion)
            case cancelled
            case committed
            case failed
        }

        let start = _state.value.withLock { state -> Start in
            switch state.lifecycle {
            case .open:
                let completion = TransactionOperationCompletion()
                state.lifecycle = .cancelling(completion)
                state.writeBuffer.removeAll(keepingCapacity: false)
                return .leader(completion)
            case .committing(let completion):
                return .waitForCommit(completion)
            case .committed:
                return .committed
            case .cancelling(let completion):
                return .waitForCancellation(completion)
            case .cancelled:
                return .cancelled
            case .failed:
                return .failed
            }
        }

        switch start {
        case .leader(let completion):
            if let journal = _state.value.withLock({ $0.journal }) {
                do { try await journal.close(committed: false) }
                catch {
                    _state.value.withLock { $0.lifecycle = .failed(error) }
                    versionstampCompletion.fail(error)
                    completion.fail(error)
                    throw error
                }
            }
            engine.releaseTransaction(transactionIdentifier)
            _state.value.withLock { $0.lifecycle = .cancelled }
            versionstampCompletion.fail(
                Self.stateError(
                    "Transaction cancelled",
                    operation: .read
                )
            )
            completion.succeed()
        case .waitForCancellation(let completion):
            try await completion.wait()
        case .waitForCommit(let completion):
            do {
                try await completion.wait()
            } catch {
                return
            }
            throw Self.stateError("Transaction committed", operation: .cancel)
        case .failed:
            if let journal = _state.value.withLock({ $0.journal }) { try await journal.close(committed: false) }
            return
        case .cancelled:
            return
        case .committed:
            throw Self.stateError("Transaction committed", operation: .cancel)
        }
    }

    public func setReadVersion(_ version: Int64) throws {
        let journal = try admittedJournal(operation: .read)
        let operation = { [snapshotVersion] in
            guard version == snapshotVersion else {
                throw StorageError.unsupportedOperation(
                    "In-memory transactions cannot switch to a historical snapshot",
                    operation: .read, backend: .inMemory
                )
            }
        }
        if let journal { try journal.performSynchronousOperation(operation) }
        else { try operation() }
    }

    public func getReadVersion() async throws -> Int64 {
        if let journal = try admittedJournal(operation: .read) {
            return try journal.performSynchronousOperation { snapshotVersion }
        }
        return snapshotVersion
    }

    public func getCommittedVersion() throws -> Int64 {
        try _state.value.withLock { state in
            guard case .committed = state.lifecycle,
                  let committedVersion = state.committedVersion else {
                throw Self.stateError(
                    "Committed version is unavailable before a successful commit",
                    operation: .read
                )
            }
            return committedVersion
        }
    }

    public func requestVersionstamp() -> any PendingTransactionVersionstamp {
        let failure = _state.value.withLock { state -> StorageError? in
            state.hasAdmittedOperation = true
            guard case .open = state.lifecycle else {
                return Self.stateError(
                    "Versionstamp must be requested before commit begins",
                    operation: .read
                )
            }
            return nil
        }
        if let failure {
            return TransactionVersionstampRequest(failure: failure)
        }
        return TransactionVersionstampRequest(
            completion: versionstampCompletion
        )
    }

    private struct RawCheckpointAccess: TransactionJournalBackendAccess {
        let snapshot: SortedKeyValueStore
        let state: StateOwner

        func getValue(for key: ByteString, snapshot isSnapshot: Bool, maximumByteCount: Int) async throws -> ByteString? {
            if !isSnapshot { state.value.withLock { $0.readConflictRegions.append(.point(key)) } }
            let value = snapshot.get(key)
            if let value, value.count > maximumByteCount {
                throw StorageError.pointReadValueTooLarge(observedByteCount: value.count, maximumByteCount: maximumByteCount, backend: .inMemory)
            }
            return value
        }

        func rangeCursor(in range: TransactionJournalRange, reverse: Bool, snapshot isSnapshot: Bool, pageSize: Int, maximumByteCount: Int) -> KeyValueCursor {
            if !isSnapshot { state.value.withLock { $0.readConflictRegions.append(.entireKeyspace) } }
            func bound(_ key: ByteString, afterEqual: Bool) -> Int {
                var low = 0
                var high = snapshot.entries.count
                while low < high {
                    let middle = low + (high - low) / 2
                    let comparison = compareBytes(snapshot.entries[middle].key, key)
                    if comparison < 0 || (comparison == 0 && afterEqual) { low = middle + 1 }
                    else { high = middle }
                }
                return low
            }
            let lower: Int
            switch range.lower {
            case .unbounded: lower = 0
            case .inclusive(let key): lower = bound(key, afterEqual: false)
            case .exclusive(let key): lower = bound(key, afterEqual: true)
            }
            let upper: Int
            switch range.upper {
            case .unbounded: upper = snapshot.entries.count
            case .inclusive(let key): upper = bound(key, afterEqual: true)
            case .exclusive(let key): upper = bound(key, afterEqual: false)
            }
            return KeyValueCursor(consuming: RawCheckpointRange(
                snapshot: snapshot, lower: lower, upper: max(lower, upper),
                reverse: reverse, maximumByteCount: maximumByteCount
            ))
        }
    }

    private struct RawCheckpointRange: TransactionRangeResult {
        let snapshot: SortedKeyValueStore
        let lower: Int
        let upper: Int
        let reverse: Bool
        let maximumByteCount: Int

        func makeCursor() -> Cursor {
            Cursor(snapshot: snapshot, lower: lower, upper: upper, reverse: reverse,
                   maximumByteCount: maximumByteCount, index: reverse ? upper - 1 : lower)
        }

        struct Cursor: TransactionRangeCursor {
            let snapshot: SortedKeyValueStore
            let lower: Int
            let upper: Int
            let reverse: Bool
            let maximumByteCount: Int
            var index: Int
            var closed = false

            mutating func next() async throws -> (ByteString, ByteString)? {
                guard !closed, index >= lower, index < upper else { return nil }
                let row = snapshot.entries[index]
                let (bytes, overflow) = row.key.count.addingReportingOverflow(row.value.count)
                guard !overflow, bytes <= maximumByteCount else {
                    throw TransactionCheckpointError.limitExceeded(resource: .retainedBytes,
                        actual: overflow ? Int.max : bytes, maximum: maximumByteCount)
                }
                index += reverse ? -1 : 1
                return (row.key, row.value)
            }

            mutating func finish(isolation actor: isolated (any Actor)?) async throws { closed = true }
        }
    }

    private func checkpointPayload(_ entries: [TransactionJournalMutation], original: CommitPayload) -> CommitPayload {
        var operations: [WriteOp] = []
        var conflicts: [InMemoryConflictRegion] = []
        for entry in entries {
            switch entry {
            case .set(let key, let value, let suppress):
                operations.append(.set(key: key, value: value))
                if !suppress { conflicts.append(.point(key)) }
            case .clear(let key, let suppress):
                operations.append(.clear(key: key))
                if !suppress { conflicts.append(.point(key)) }
            case .clearRange(let begin, let end, let suppress):
                operations.append(.clearRange(begin: begin, end: end))
                if !suppress, let range = InMemoryConflictRegion.halfOpen(begin: begin, end: end) { conflicts.append(range) }
            case .atomic(let key, let parameter, let type, let suppress):
                operations.append(.atomic(key: key, param: parameter, mutationType: type))
                if !suppress { conflicts.append(.point(key)) }
            case .writeConflict(let begin, let end):
                if let range = InMemoryConflictRegion.halfOpen(begin: begin, end: end) { conflicts.append(range) }
            }
        }
        let reads = _state.value.withLock { $0.readConflictRegions }
        return CommitPayload(operations: operations, readConflictRegions: original.readConflictRegions + reads, writeConflictRegions: conflicts)
    }

    private func commitOperations(
        _ payload: CommitPayload
    ) -> Result<Int64, StorageError> {
        do {
            let version = try engine._store.withLock {
                state throws(StorageError) -> Int64 in
                defer {
                    state.releaseTransaction(transactionIdentifier)
                }

                for writeSet in state.committedWriteHistory
                where writeSet.version > snapshotVersion {
                    if Self.intersects(
                        payload.readConflictRegions,
                        writeSet.regions
                    ) || Self.intersects(
                        payload.writeConflictRegions,
                        writeSet.regions
                    ) {
                        throw StorageError(
                            code: .transactionConflict,
                            operation: .commit,
                            backend: .inMemory,
                            message: "Transaction conflict"
                        )
                    }
                }

                var staged = state.store
                for operation in payload.operations {
                    switch operation {
                    case .set(let key, let value):
                        staged.set(key, value)
                    case .clear(let key):
                        staged.delete(key)
                    case .clearRange(let begin, let end):
                        staged.deleteRange(begin: begin, end: end)
                    case .atomic(let key, let param, let mutationType):
                        switch try mutationType.apply(to: staged.get(key), param: param) {
                        case .set(let bytes):
                            staged.set(key, bytes)
                        case .clear:
                            staged.delete(key)
                        case .unchanged:
                            break
                        }
                    }
                }
                guard !payload.operations.isEmpty
                        || !payload.writeConflictRegions.isEmpty else {
                    return state.version
                }
                let (nextVersion, overflow) = state.version.addingReportingOverflow(1)
                guard !overflow else {
                    throw StorageError(
                        code: .resourceUnavailable,
                        operation: .commit,
                        backend: .inMemory,
                        message: "In-memory commit version exhausted"
                    )
                }
                state.store = staged
                state.version = nextVersion
                if !payload.writeConflictRegions.isEmpty {
                    state.committedWriteHistory.append(
                        InMemoryEngine.CommittedWriteSet(
                            version: nextVersion,
                            regions: payload.writeConflictRegions
                        )
                    )
                }
                return nextVersion
            }
            return .success(version)
        } catch {
            return .failure(error)
        }
    }

    private static func readConflictRegion(
        from begin: KeySelector,
        to end: KeySelector
    ) -> InMemoryConflictRegion? {
        guard begin.offset == 1, end.offset == 1 else {
            return .entireKeyspace
        }
        let lower = InMemoryConflictCut(
            key: begin.key,
            side: begin.orEqual ? .after : .before
        )
        let upper = InMemoryConflictCut(
            key: end.key,
            side: end.orEqual ? .after : .before
        )
        return .between(lower, upper)
    }

    private static func intersects(
        _ left: [InMemoryConflictRegion],
        _ right: [InMemoryConflictRegion]
    ) -> Bool {
        for leftRegion in left {
            for rightRegion in right where leftRegion.overlaps(rightRegion) {
                return true
            }
        }
        return false
    }

    private static func validateOpen(
        _ lifecycle: Lifecycle,
        operation: StorageOperation
    ) throws {
        switch lifecycle {
        case .open:
            return
        case .committing:
            throw stateError("Transaction is committing", operation: operation)
        case .committed:
            throw stateError("Transaction committed", operation: operation)
        case .cancelling:
            throw stateError("Transaction is cancelling", operation: operation)
        case .cancelled:
            throw stateError("Transaction cancelled", operation: operation)
        case .failed(let error):
            throw error
        }
    }

    private static func stateError(
        _ message: String,
        operation: StorageOperation
    ) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: .inMemory,
            message: message
        )
    }

    private static func versionstampResult(
        for result: Result<Int64, StorageError>
    ) -> Result<TransactionVersionstamp, StorageError> {
        switch result {
        case .success(let committedVersion):
            do {
                return .success(
                    try TransactionVersionstamp(
                        committedVersion: committedVersion
                    )
                )
            } catch {
                return .failure(error)
            }
        case .failure(let error):
            return .failure(error)
        }
    }
}
