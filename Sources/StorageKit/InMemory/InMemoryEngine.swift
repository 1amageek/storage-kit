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
        transactionDomain.leases.requestShutdown()
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
        _state.withLock { state in
            guard case .failed(let error) = state.lifecycle else { return nil }
            return error
        }
    }

    private let engine: InMemoryEngine
    private let snapshot: SortedKeyValueStore
    private let snapshotVersion: Int64
    private let transactionIdentifier: UInt64
    private let mutationByteMeter = TransactionMutationByteMeter()
    private let versionstampCompletion = TransactionVersionstampCompletion()

    private struct MutableState: Sendable {
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
        case failed(StorageError)
    }
    private let _state: Mutex<MutableState>

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
        self._state = Mutex(MutableState())
    }

    deinit {
        engine.releaseTransaction(transactionIdentifier)
    }

    public func configureMutationByteLimit(maximumBytes: Int?) throws {
        try mutationByteMeter.configure(maximumBytes: maximumBytes)
    }

    // MARK: - Read

    public func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try readValue(for: key, snapshot: snapshot)
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
        try _state.withLock { state in
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
            return try _state.withLock { state throws(StorageError) in
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
        try _state.withLock { state in
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
        try _state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .delete)
            try mutationByteMeter.recordClear(key: key)
            state.writeBuffer.append(.clear(key: key))
            state.writeConflictRegions.append(.point(key))
        }
    }

    public func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        try _state.withLock { state in
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
        try _state.withLock { state in
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
        try _state.withLock { state in
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
            case failed(StorageError)
        }

        let start = _state.withLock { state -> Start in
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
            let result = commitOperations(payload)
            _state.withLock { state in
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
            let lifecycle = _state.withLock { $0.lifecycle }
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

        let start = _state.withLock { state -> Start in
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
            engine.releaseTransaction(transactionIdentifier)
            _state.withLock { $0.lifecycle = .cancelled }
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
        case .cancelled, .failed:
            return
        case .committed:
            throw Self.stateError("Transaction committed", operation: .cancel)
        }
    }

    public func setReadVersion(_ version: Int64) throws {
        try _state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .read)
            guard version == snapshotVersion else {
                throw StorageError.unsupportedOperation(
                    "In-memory transactions cannot switch to a historical snapshot",
                    operation: .read,
                    backend: .inMemory
                )
            }
        }
    }

    public func getReadVersion() async throws -> Int64 {
        try _state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .read)
            return snapshotVersion
        }
    }

    public func getCommittedVersion() throws -> Int64 {
        try _state.withLock { state in
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
        let failure = _state.withLock { state -> StorageError? in
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
    ) throws(StorageError) {
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
