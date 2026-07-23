import StorageKit
import FoundationDB
import Synchronization

/// StorageKit.Transaction adapter for FoundationDB transactions.
///
/// ## Range iteration
/// Returns a thin wrapper around `FDB.AsyncKVSequence` so iteration errors are
/// normalized to `StorageError`.
public final class FDBStorageTransaction: Transaction, Sendable {

    public typealias RangeResult = FDBStorageRangeResult

    public static let declaredCapabilities = TransactionCapabilities(
            transactionTimeout: true,
            schedulingPriority: true,
            readPriority: true,
            readCacheControl: true,
            systemKeyAccess: true,
            historicalReadVersion: true,
            readVersion: true,
            committedVersion: true,
            explicitConflictRanges: true,
            committedVersionstamp: true,
            versionstampedMutations: true
    )

    public var capabilities: TransactionCapabilities { Self.declaredCapabilities }
    public var mutationByteLimit: Int? { mutationByteMeter.maximumBytes }

    let fdbTransaction: any TransactionProtocol
    private let transactionDomain: FoundationDBTransactionDomain
    private let commitRequestLimit: CommitRequestLimit
    private let state = Mutex(MutableState())
    private let mutationByteMeter = TransactionMutationByteMeter()

    private struct MutableState: Sendable {
        var lifecycle: Lifecycle = .open
        var hasPendingMutations = false
        var activeOperationCount = 0
        var commitFollowerCount = 0
        var commitCancellationWaiterCount = 0
        var activeDirectoryOperation: UInt64?
        var nextDirectoryOperation: UInt64 = 1
    }

    private struct CommitPreparation: Sendable {
        let outcome = TransactionOperationCompletion()
        let cleanup = TransactionOperationCompletion()
    }

    private enum Lifecycle: Sendable {
        case open
        case preparing(CommitPreparation)
        case failingPreparation(CommitPreparation)
        case cancellingPreparation(CommitPreparation, StorageError)
        case committing(TransactionOperationCompletion)
        case committed
        case cancelling(TransactionOperationCompletion, StorageError)
        case cancelled(StorageError)
        case failed(StorageError)
        case commitUnknown(StorageError)
    }

    private enum CommitExecutionResolution: Sendable {
        case resolve(Result<Void, StorageError>)
        case resolvedByCancellation
    }

    private enum CommitPreparationAction: Sendable {
        case dispatch
        case fail(StorageError)
        case completeCancellation(CommitPreparation, StorageError)
    }

    private enum CommitDispatchState: Sendable {
        case notStarted
        case started
    }

    var commitFollowerCount: Int {
        state.withLock { $0.commitFollowerCount }
    }

    var commitCancellationWaiterCount: Int {
        state.withLock { $0.commitCancellationWaiterCount }
    }

    init(
        _ fdbTransaction: any TransactionProtocol,
        transactionDomain: FoundationDBTransactionDomain,
        commitRequestLimit: CommitRequestLimit = .default
    ) throws {
        self.fdbTransaction = fdbTransaction
        self.transactionDomain = transactionDomain
        self.commitRequestLimit = commitRequestLimit
        do {
            try fdbTransaction.setOption(
                to: commitRequestLimit.maximumByteCount,
                forOption: .sizeLimit
            )
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .beginTransaction)
        } catch {
            throw Self.convertBackendError(error, operation: .beginTransaction)
        }
    }

    public func configureMutationByteLimit(maximumBytes: Int?) throws {
        try state.withLock { state in
            try Self.validateExclusiveAccess(
                state,
                operation: .beginTransaction
            )
            guard !state.hasPendingMutations else {
                throw TransactionMutationByteLimitError.configurationAfterAdmission
            }
            try mutationByteMeter.configure(maximumBytes: maximumBytes)
        }
    }

    func withDirectoryOperation<T: Sendable>(
        transactionDomain: FoundationDBTransactionDomain,
        writes: Bool,
        operation: StorageOperation,
        _ body: (any TransactionProtocol) async throws -> T
    ) async throws -> T {
        guard self.transactionDomain === transactionDomain else {
            throw StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: .foundationDB,
                message: "FoundationDB directory service and transaction belong to different engines"
            )
        }
        let token = try state.withLock { state -> UInt64 in
            try Self.validateExclusiveAccess(state, operation: operation)
            let token = state.nextDirectoryOperation
            state.nextDirectoryOperation &+= 1
            state.activeDirectoryOperation = token
            if writes {
                state.hasPendingMutations = true
            }
            return token
        }
        defer {
            state.withLock { state in
                if state.activeDirectoryOperation == token {
                    state.activeDirectoryOperation = nil
                }
            }
        }
        return try await body(fdbTransaction)
    }

    // MARK: - Type Conversion

    private func toFDB(_ ks: KeySelector) -> FDB.KeySelector {
        FDB.KeySelector(
            key: FoundationDBByteSource(ks.key),
            orEqual: ks.orEqual,
            offset: ks.offset
        )
    }

    private func toFDB(_ sm: StreamingMode) -> FDB.StreamingMode {
        FDB.StreamingMode(rawValue: sm.rawValue)!
    }

    private func toFDB(_ mt: MutationType) -> FDB.MutationType {
        switch mt {
        case .add: return .add
        case .bitAnd: return .bitAnd
        case .bitOr: return .bitOr
        case .bitXor: return .bitXor
        case .max: return .max
        case .min: return .min
        case .setVersionstampedKey: return .setVersionstampedKey
        case .setVersionstampedValue: return .setVersionstampedValue
        case .compareAndClear: return .compareAndClear
        }
    }

    private func toFDB(_ ct: ConflictRangeType) -> FDB.ConflictRangeType {
        switch ct {
        case .read: return .read
        case .write: return .write
        }
    }

    private static func validateFoundationDBLength(
        _ bytes: Bytes,
        operation: StorageOperation
    ) throws {
        guard Int32(exactly: bytes.count) != nil else {
            throw StorageError(
                code: .backendContractViolation,
                operation: operation,
                backend: .foundationDB,
                message: "Byte count cannot be represented by the FoundationDB C API"
            )
        }
    }

    // MARK: - Read

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        try beginOperation(.read)
        defer { finishOperation() }
        do {
            let value = try await fdbTransaction.getValue(
                for: FoundationDBByteSource(key),
                snapshot: snapshot
            )
            return value.map {
                Bytes(retaining: FoundationDBResultBytesOwner($0))
            }
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .read)
        } catch {
            throw Self.convertBackendError(error, operation: .read)
        }
    }

    public func getKey(selector: KeySelector, snapshot: Bool) async throws -> Bytes? {
        try beginOperation(.read)
        defer { finishOperation() }
        do {
            let key = try await fdbTransaction.getKey(
                selector: toFDB(selector),
                snapshot: snapshot
            )
            return key.map {
                Bytes(retaining: FoundationDBResultBytesOwner($0))
            }
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .read)
        } catch {
            throw Self.convertBackendError(error, operation: .read)
        }
    }

    /// Returns a thin sequence wrapper that normalizes FDB iteration errors.
    ///
    /// Uses the existential opening pattern to call extension methods on `any TransactionProtocol`.
    /// Swift 5.7+ implicit existential opening allows access to concrete type extension methods.
    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> FDBStorageRangeResult {
        do {
            try validateOpen(operation: .rangeRead)
        } catch {
            return FDBStorageRangeResult(error: Self.convertBackendError(
                error,
                operation: .rangeRead
            ))
        }
        let fdbBegin = toFDB(begin)
        let fdbEnd = toFDB(end)
        let fdbMode = toFDB(streamingMode)

        func makeRangeSequence<T: TransactionProtocol>(
            from transaction: T
        ) -> FDB.AsyncKVSequence {
            transaction.getRange(
                from: fdbBegin,
                to: fdbEnd,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: fdbMode
            )
        }
        return FDBStorageRangeResult(
            makeRangeSequence(from: fdbTransaction),
            transaction: self
        )
    }

    // MARK: - Write

    public func setValue(_ value: Bytes, for key: Bytes) throws {
        try Self.validateFoundationDBLength(key, operation: .write)
        try Self.validateFoundationDBLength(value, operation: .write)
        try key.withUnsafeBytes { keyBytes in
            try value.withUnsafeBytes { valueBytes in
                let borrowedKey = BorrowedFoundationDBByteSource(keyBytes)
                let borrowedValue = BorrowedFoundationDBByteSource(valueBytes)
                try state.withLock { state in
                    try Self.validateExclusiveAccess(state, operation: .write)
                    try mutationByteMeter.recordSet(key: key, value: value)
                    do {
                        try fdbTransaction.setValue(
                            borrowedValue,
                            for: borrowedKey
                        )
                        state.hasPendingMutations = true
                    } catch {
                        let converted = Self.convertBackendError(
                            error,
                            operation: .write
                        )
                        state.lifecycle = .failed(converted)
                        throw converted
                    }
                }
            }
        }
    }

    public func clear(key: Bytes) throws {
        try Self.validateFoundationDBLength(key, operation: .delete)
        try key.withUnsafeBytes { keyBytes in
            let borrowedKey = BorrowedFoundationDBByteSource(keyBytes)
            try state.withLock { state in
                try Self.validateExclusiveAccess(state, operation: .delete)
                try mutationByteMeter.recordClear(key: key)
                do {
                    try fdbTransaction.clear(key: borrowedKey)
                    state.hasPendingMutations = true
                } catch {
                    let converted = Self.convertBackendError(
                        error,
                        operation: .delete
                    )
                    state.lifecycle = .failed(converted)
                    throw converted
                }
            }
        }
    }

    public func clearRange(beginKey: Bytes, endKey: Bytes) throws {
        try Self.validateFoundationDBLength(beginKey, operation: .deleteRange)
        try Self.validateFoundationDBLength(endKey, operation: .deleteRange)
        try beginKey.withUnsafeBytes { beginBytes in
            try endKey.withUnsafeBytes { endBytes in
                let borrowedBegin = BorrowedFoundationDBByteSource(beginBytes)
                let borrowedEnd = BorrowedFoundationDBByteSource(endBytes)
                try state.withLock { state in
                    try Self.validateExclusiveAccess(
                        state,
                        operation: .deleteRange
                    )
                    try mutationByteMeter.recordClearRange(
                        beginKey: beginKey,
                        endKey: endKey
                    )
                    do {
                        try fdbTransaction.clearRange(
                            beginKey: borrowedBegin,
                            endKey: borrowedEnd
                        )
                        state.hasPendingMutations = true
                    } catch {
                        let converted = Self.convertBackendError(
                            error,
                            operation: .deleteRange
                        )
                        state.lifecycle = .failed(converted)
                        throw converted
                    }
                }
            }
        }
    }

    // MARK: - Atomic Operations

    public func atomicOp(
        key: Bytes,
        param: Bytes,
        mutationType: MutationType
    ) throws {
        try Self.validateFoundationDBLength(key, operation: .write)
        try Self.validateFoundationDBLength(param, operation: .write)
        try key.withUnsafeBytes { keyBytes in
            try param.withUnsafeBytes { parameterBytes in
                let borrowedKey = BorrowedFoundationDBByteSource(keyBytes)
                let borrowedParameter = BorrowedFoundationDBByteSource(
                    parameterBytes
                )
                try state.withLock { state in
                    try Self.validateExclusiveAccess(state, operation: .write)
                    try mutationByteMeter.recordAtomic(
                        key: key,
                        parameter: param
                    )
                    do {
                        try fdbTransaction.atomicOp(
                            key: borrowedKey,
                            param: borrowedParameter,
                            mutationType: toFDB(mutationType)
                        )
                        state.hasPendingMutations = true
                    } catch {
                        let converted = Self.convertBackendError(
                            error,
                            operation: .write
                        )
                        state.lifecycle = .failed(converted)
                        throw converted
                    }
                }
            }
        }
    }

    // MARK: - Transaction Control

    public func commit() async throws {
        enum Start {
            case leader(TransactionOperationCompletion)
            case waitForCommit(TransactionOperationCompletion)
            case waitForCancellation(
                TransactionOperationCompletion,
                StorageError
            )
            case committed
            case failed(StorageError)
        }

        let start = state.withLock { state -> Start in
            switch state.lifecycle {
            case .open:
                guard state.activeDirectoryOperation == nil,
                      state.activeOperationCount == 0 else {
                    return .failed(Self.lifecycleError(
                        "FoundationDB transaction has active operations",
                        operation: .commit,
                        code: .transactionBusy
                    ))
                }
                let preparation = CommitPreparation()
                state.lifecycle = .preparing(preparation)
                return .leader(preparation.outcome)
            case .preparing(let preparation),
                 .failingPreparation(let preparation),
                 .cancellingPreparation(let preparation, _):
                state.commitFollowerCount = state.commitFollowerCount == Int.max
                    ? Int.max
                    : state.commitFollowerCount + 1
                return .waitForCommit(preparation.outcome)
            case .committing(let completion):
                state.commitFollowerCount = state.commitFollowerCount == Int.max
                    ? Int.max
                    : state.commitFollowerCount + 1
                return .waitForCommit(completion)
            case .committed:
                return .committed
            case .cancelling(let completion, let error):
                return .waitForCancellation(completion, error)
            case .cancelled(let error):
                return .failed(error)
            case .failed(let error), .commitUnknown(let error):
                return .failed(error)
            }
        }

        switch start {
        case .leader(let completion):
            switch await performCommit() {
            case .resolve(let result):
                await completion.resolve(result)
                try result.get()
            case .resolvedByCancellation:
                try await completion.wait().get()
            }
        case .waitForCommit(let completion):
            try await completion.wait().get()
        case .waitForCancellation(let completion, let error):
            try await completion.wait().get()
            throw error
        case .committed:
            return
        case .failed(let error):
            throw error
        }
    }

    private func performCommit() async -> CommitExecutionResolution {
        let admissionResult: Result<Void, StorageError>
        do {
            try await checkCommitRequestFootprint()
            admissionResult = .success(())
        } catch is CancellationError {
            admissionResult = .failure(StorageError(
                code: .transactionCancelled,
                operation: .prepare,
                backend: .foundationDB,
                message: "FoundationDB commit admission was cancelled"
            ))
        } catch let error as FDBError {
            admissionResult = .failure(Self.convertFDBError(
                error,
                operation: .prepare
            ))
        } catch let error as StorageError {
            admissionResult = .failure(
                Self.normalizeCommitAdmissionError(error)
            )
        } catch {
            admissionResult = .failure(Self.convertBackendError(
                error,
                operation: .prepare
            ))
        }

        switch selectCommitPreparationAction(for: admissionResult) {
        case .completeCancellation(let preparation, let error):
            state.withLock { state in
                guard case .cancellingPreparation = state.lifecycle else {
                    return
                }
                state.lifecycle = .cancelled(error)
            }
            await preparation.cleanup.resolve(.success(()))
            return .resolvedByCancellation
        case .fail(let error):
            fdbTransaction.cancel()
            state.withLock { state in
                guard case .failingPreparation = state.lifecycle else { return }
                state.lifecycle = .failed(error)
            }
            return .resolve(.failure(error))
        case .dispatch:
            break
        }

        do {
            let committed = try await fdbTransaction.commit()
            if !committed {
                throw StorageError(
                    code: .transactionConflict,
                    operation: .commit,
                    backend: .foundationDB,
                    message: "FoundationDB transaction commit reported a conflict"
                )
            }
            state.withLock { $0.lifecycle = .committed }
            return .resolve(.success(()))
        } catch let error as StorageError {
            let converted = Self.convertBackendError(error, operation: .commit)
            state.withLock { state in
                state.lifecycle = converted.code == .commitUnknownResult
                    ? .commitUnknown(converted)
                    : .failed(converted)
            }
            return .resolve(.failure(converted))
        } catch is CancellationError {
            let converted = Self.convertBackendError(
                CancellationError(),
                operation: .commit
            )
            state.withLock { $0.lifecycle = .commitUnknown(converted) }
            return .resolve(.failure(converted))
        } catch let error as FDBError {
            let converted = Self.convertFDBError(error, operation: .commit)
            state.withLock { state in
                state.lifecycle = converted.code == .commitUnknownResult
                    ? .commitUnknown(converted)
                    : .failed(converted)
            }
            return .resolve(.failure(converted))
        } catch {
            let converted = Self.convertBackendError(error, operation: .commit)
            state.withLock { state in
                state.lifecycle = converted.code == .commitUnknownResult
                    ? .commitUnknown(converted)
                    : .failed(converted)
            }
            return .resolve(.failure(converted))
        }
    }

    private func selectCommitPreparationAction(
        for admissionResult: Result<Void, StorageError>
    ) -> CommitPreparationAction {
        state.withLock { state in
            switch state.lifecycle {
            case .preparing(let preparation):
                switch admissionResult {
                case .success:
                    state.lifecycle = .committing(preparation.outcome)
                    return .dispatch
                case .failure(let error):
                    state.lifecycle = .failingPreparation(preparation)
                    return .fail(error)
                }
            case .cancellingPreparation(let preparation, let error):
                return .completeCancellation(preparation, error)
            case .open, .failingPreparation, .committing, .committed,
                 .cancelled,
                 .cancelling, .failed, .commitUnknown:
                let error = Self.lifecycleError(
                    "FoundationDB commit preparation entered an invalid state",
                    operation: .prepare,
                    code: .backendContractViolation
                )
                state.lifecycle = .failed(error)
                return .fail(error)
            }
        }
    }

    /// Validates FoundationDB's complete transaction footprint without reading
    /// or materializing any key/value payload. The native commit remains the
    /// authoritative final gate because FoundationDB documents this metric as
    /// approximate.
    private func checkCommitRequestFootprint() async throws {
        let observedByteCount = try await fdbTransaction.approximateSize()
        guard let observedByteCount = UInt64(exactly: observedByteCount) else {
            throw StorageError(
                code: .backendContractViolation,
                operation: .prepare,
                backend: .foundationDB,
                message: "FoundationDB returned a negative transaction footprint"
            )
        }

        let maximumByteCount = UInt64(commitRequestLimit.maximumByteCount)
        guard observedByteCount <= maximumByteCount else {
            throw StorageError(
                code: .transactionTooLarge,
                operation: .prepare,
                backend: .foundationDB,
                message: "FoundationDB transaction footprint exceeds the native limit",
                byteLimitViolation: StorageByteLimitViolation(
                    resource: .commitRequest,
                    observedByteCount: observedByteCount,
                    maximumByteCount: maximumByteCount,
                    measurement: .estimated
                )
            )
        }
    }

    public func cancel() async throws {
        enum Start {
            case leader(TransactionOperationCompletion)
            case preparationLeader(CommitPreparation, StorageError)
            case waitForPreparationCleanup(CommitPreparation)
            case waitForCancellation(TransactionOperationCompletion)
            case waitForCommit(TransactionOperationCompletion)
            case cancelled
            case committed
            case failed
            case commitUnknown(StorageError)
            case busy(StorageError)
        }

        let start = state.withLock { state -> Start in
            switch state.lifecycle {
            case .open:
                guard state.activeDirectoryOperation == nil else {
                    return .busy(Self.lifecycleError(
                        "FoundationDB transaction has an active directory operation",
                        operation: .cancel
                    ))
                }
                let completion = TransactionOperationCompletion()
                let error = Self.cancellationError(operation: .cancel)
                state.lifecycle = .cancelling(completion, error)
                return .leader(completion)
            case .preparing(let preparation):
                let error = Self.cancellationError(operation: .prepare)
                state.lifecycle = .cancellingPreparation(preparation, error)
                return .preparationLeader(preparation, error)
            case .failingPreparation(let preparation):
                state.commitCancellationWaiterCount =
                    state.commitCancellationWaiterCount == Int.max
                    ? Int.max
                    : state.commitCancellationWaiterCount + 1
                return .waitForCommit(preparation.outcome)
            case .cancellingPreparation(let preparation, _):
                state.commitCancellationWaiterCount =
                    state.commitCancellationWaiterCount == Int.max
                    ? Int.max
                    : state.commitCancellationWaiterCount + 1
                return .waitForPreparationCleanup(preparation)
            case .committing(let completion):
                state.commitCancellationWaiterCount =
                    state.commitCancellationWaiterCount == Int.max
                    ? Int.max
                    : state.commitCancellationWaiterCount + 1
                return .waitForCommit(completion)
            case .committed:
                return .committed
            case .cancelling(let completion, _):
                return .waitForCancellation(completion)
            case .cancelled:
                return .cancelled
            case .failed:
                return .failed
            case .commitUnknown(let error):
                return .commitUnknown(error)
            }
        }

        switch start {
        case .leader(let completion):
            fdbTransaction.cancel()
            let error = Self.cancellationError(operation: .cancel)
            state.withLock { $0.lifecycle = .cancelled(error) }
            await completion.resolve(.success(()))
        case .preparationLeader(let preparation, let error):
            fdbTransaction.cancel()
            await preparation.outcome.resolve(.failure(error))
            try await preparation.cleanup.wait().get()
        case .waitForPreparationCleanup(let preparation):
            try await preparation.cleanup.wait().get()
        case .waitForCancellation(let completion):
            try await completion.wait().get()
        case .waitForCommit(let completion):
            let result = await completion.wait()
            switch result {
            case .success:
                throw Self.lifecycleError(
                    "FoundationDB transaction committed",
                    operation: .cancel
                )
            case .failure(let error) where error.code == .commitUnknownResult:
                throw error
            case .failure:
                return
            }
        case .cancelled, .failed:
            return
        case .committed:
            throw Self.lifecycleError(
                "FoundationDB transaction committed",
                operation: .cancel
            )
        case .commitUnknown(let error):
            throw error
        case .busy(let error):
            throw error
        }
    }

    // MARK: - Version Management

    public func setReadVersion(_ version: Int64) throws {
        try state.withLock { state in
            try Self.validateExclusiveAccess(state, operation: .read)
            fdbTransaction.setReadVersion(version)
        }
    }

    public func getReadVersion() async throws -> Int64 {
        try beginOperation(.read)
        defer { finishOperation() }
        do {
            return try await fdbTransaction.getReadVersion()
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .read)
        } catch {
            throw Self.convertBackendError(error, operation: .read)
        }
    }

    public func getCommittedVersion() throws -> Int64 {
        try state.withLock { state in
            guard case .committed = state.lifecycle else {
                throw Self.lifecycleError(
                    "Committed version is unavailable before a successful commit",
                    operation: .read
                )
            }
        }
        do {
            return try fdbTransaction.getCommittedVersion()
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .read)
        } catch {
            throw Self.convertBackendError(error, operation: .read)
        }
    }

    // MARK: - Transaction Options

    public func setOption(forOption option: TransactionOption) throws {
        try state.withLock { state in
            try Self.validateExclusiveAccess(state, operation: .execute)
            do {
                switch option {
                case .timeout(let milliseconds):
                    try fdbTransaction.setOption(to: milliseconds, forOption: .timeout)
                default:
                    try fdbTransaction.setOption(forOption: toFDBOption(option))
                }
            } catch let error as FDBError {
                throw Self.convertFDBError(error, operation: .execute)
            } catch {
                throw Self.convertBackendError(error, operation: .execute)
            }
        }
    }

    public func setOption(to value: Bytes?, forOption option: TransactionOption) throws {
        guard let value else {
            return try setOption(forOption: option)
        }
        try Self.validateFoundationDBLength(value, operation: .execute)
        try value.withUnsafeBytes { valueBytes in
            let borrowedValue = BorrowedFoundationDBByteSource(valueBytes)
            try state.withLock { state in
                try Self.validateExclusiveAccess(state, operation: .execute)
                do {
                    try fdbTransaction.setOption(
                        to: borrowedValue,
                        forOption: toFDBOption(option)
                    )
                } catch let error as FDBError {
                    throw Self.convertFDBError(error, operation: .execute)
                } catch {
                    throw Self.convertBackendError(error, operation: .execute)
                }
            }
        }
    }

    public func setOption(to value: Int, forOption option: TransactionOption) throws {
        try state.withLock { state in
            try Self.validateExclusiveAccess(state, operation: .execute)
            do {
                try fdbTransaction.setOption(to: value, forOption: toFDBOption(option))
            } catch let error as FDBError {
                throw Self.convertFDBError(error, operation: .execute)
            } catch {
                throw Self.convertBackendError(error, operation: .execute)
            }
        }
    }

    private func toFDBOption(_ option: TransactionOption) -> FDB.TransactionOption {
        switch option {
        case .timeout: return .timeout
        case .priorityBatch: return .priorityBatch
        case .prioritySystemImmediate: return .prioritySystemImmediate
        case .readPriorityLow: return .readPriorityLow
        case .readPriorityHigh: return .readPriorityHigh
        case .accessSystemKeys: return .accessSystemKeys
        case .readServerSideCacheDisable: return .readServerSideCacheDisable
        }
    }

    // MARK: - Error Conversion

    /// Convert FDBError to the appropriate StorageError at the boundary.
    static func convertFDBError(_ error: FDBError, operation: StorageOperation) -> StorageError {
        convertFDBError(
            error,
            operation: operation,
            commitDispatchState: operation == .commit ? .started : .notStarted
        )
    }

    private static func convertFDBError(
        _ error: FDBError,
        operation: StorageOperation,
        commitDispatchState: CommitDispatchState
    ) -> StorageError {
        let code: StorageError.Code
        switch error.knownCode {
        case .transactionTooLarge:
            code = .transactionTooLarge
        case .keyTooLarge:
            code = .keyTooLarge
        case .valueTooLarge:
            code = .valueTooLarge
        default:
            code = Self.classifyFDBErrorCode(
                error.code,
                operation: operation,
                commitDispatchState: commitDispatchState
            )
        }

        return StorageError(
            code: code,
            operation: operation,
            backend: .foundationDB,
            message: "FoundationDB transaction failed: \(error.description)",
            backendCode: error.code,
            underlyingDescription: error.description
        )
    }

    private static func classifyFDBErrorCode(
        _ errorCode: Int32,
        operation: StorageOperation,
        commitDispatchState: CommitDispatchState
    ) -> StorageError.Code {
        switch errorCode {
        case 1007:
            return .transactionTooOld
        case 1009:
            return .transactionFutureVersion
        case 1020:
            return .transactionConflict
        case 1021, 1022:
            return commitDispatchState == .started
                ? .commitUnknownResult
                : .backendContractViolation
        case 1004, 1031:
            return commitDispatchState == .started
                ? .commitUnknownResult
                : .transactionTimedOut
        case 1025, 1101:
            return commitDispatchState == .started
                ? .commitUnknownResult
                : .transactionCancelled
        case 1026, 1049:
            return commitDispatchState == .started
                ? .commitUnknownResult
                : .connectionFailure
        case 1039:
            return commitDispatchState == .started
                ? .commitUnknownResult
                : .connectionFailure
        case 1037, 1038, 1042, 1051, 1078, 1213, 1223:
            return .transactionBusy
        default:
            return commitDispatchState == .started
                ? .commitUnknownResult
                : .backendFailure
        }
    }

    private static func normalizeCommitAdmissionError(
        _ error: StorageError
    ) -> StorageError {
        guard error.code == .commitUnknownResult else {
            return error
        }
        return StorageError(
            code: .backendContractViolation,
            operation: .prepare,
            backend: .foundationDB,
            message: "FoundationDB reported an unknown commit result before commit dispatch",
            backendCode: error.backendCode,
            underlyingDescription: error.description
        )
    }

    static func convertBackendError(_ error: any Error, operation: StorageOperation) -> StorageError {
        if let storageError = error as? StorageError {
            guard operation == .commit else { return storageError }
            switch storageError.code {
            case .transactionConflict, .transactionTooOld, .transactionFutureVersion,
                 .transactionBusy, .invalidOperation, .unsupportedOperation,
                 .backendContractViolation, .dataCorruption,
                 .resourceUnavailable, .keyNotFound,
                 .transactionTooLarge, .keyTooLarge, .valueTooLarge:
                return storageError
            case .transactionTimedOut, .transactionCancelled, .connectionFailure,
                 .commitUnknownResult, .backendFailure:
                return StorageError(
                    code: .commitUnknownResult,
                    operation: .commit,
                    backend: .foundationDB,
                    message: "FoundationDB commit outcome is unknown",
                    backendCode: storageError.backendCode,
                    underlyingDescription: storageError.description
                )
            }
        }
        return StorageError(
            code: operation == .commit ? .commitUnknownResult : .backendFailure,
            operation: operation,
            backend: .foundationDB,
            message: operation == .commit
                ? "FoundationDB commit outcome is unknown"
                : "FoundationDB backend error",
            underlyingDescription: String(describing: error)
        )
    }

    // MARK: - Conflict Range

    public func addConflictRange(beginKey: Bytes, endKey: Bytes, type: ConflictRangeType) throws {
        try Self.validateFoundationDBLength(beginKey, operation: .write)
        try Self.validateFoundationDBLength(endKey, operation: .write)
        try beginKey.withUnsafeBytes { beginBytes in
            try endKey.withUnsafeBytes { endBytes in
                let borrowedBegin = BorrowedFoundationDBByteSource(beginBytes)
                let borrowedEnd = BorrowedFoundationDBByteSource(endBytes)
                try state.withLock { state in
                    try Self.validateExclusiveAccess(state, operation: .write)
                    do {
                        try fdbTransaction.addConflictRange(
                            beginKey: borrowedBegin,
                            endKey: borrowedEnd,
                            type: toFDB(type)
                        )
                    } catch let error as FDBError {
                        throw Self.convertFDBError(error, operation: .write)
                    } catch {
                        throw Self.convertBackendError(error, operation: .write)
                    }
                }
            }
        }
    }

    // MARK: - Statistics

    public func getEstimatedRangeSizeBytes(beginKey: Bytes, endKey: Bytes) async throws -> Int {
        try beginOperation(.rangeRead)
        defer { finishOperation() }
        let hasPendingMutations = state.withLock { state in
            return state.hasPendingMutations
        }
        if hasPendingMutations {
            guard compareBytes(beginKey, endKey) <= 0 else {
                throw StorageError(
                    code: .invalidOperation,
                    operation: .rangeRead,
                    backend: .foundationDB,
                    message: "Range size boundaries are not ordered"
                )
            }
            return try await StorageRangeMetrics.exactSize(
                getRange(
                    begin: beginKey,
                    end: endKey,
                    snapshot: true,
                    streamingMode: .wantAll
                )
            )
        }
        do {
            return try await fdbTransaction.getEstimatedRangeSizeBytes(
                beginKey: FoundationDBByteSource(beginKey),
                endKey: FoundationDBByteSource(endKey)
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .rangeRead)
        } catch {
            throw Self.convertBackendError(error, operation: .rangeRead)
        }
    }

    public func getRangeSplitPoints(
        beginKey: Bytes,
        endKey: Bytes,
        chunkSize: Int
    ) async throws -> [Bytes] {
        try beginOperation(.rangeRead)
        defer { finishOperation() }
        return try await StorageRangeMetrics.splitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: chunkSize,
            maximumPointCount: StorageRangeMetrics
                .defaultMaximumSplitPointCount,
            rows: getRange(
                begin: beginKey,
                end: endKey,
                snapshot: true,
                streamingMode: .wantAll
            )
        )
    }

    // MARK: - Versionstamp

    public func getVersionstamp() async throws -> Bytes? {
        try state.withLock { state in
            guard case .committed = state.lifecycle else {
                throw Self.lifecycleError(
                    "Versionstamp is unavailable before a successful commit",
                    operation: .read
                )
            }
        }
        do {
            let versionstamp = try await fdbTransaction.getVersionstamp()
            return versionstamp.map {
                Bytes(retaining: FoundationDBResultBytesOwner($0))
            }
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as FDBError {
            throw Self.convertFDBError(error, operation: .read)
        } catch {
            throw Self.convertBackendError(error, operation: .read)
        }
    }

    private func validateOpen(operation: StorageOperation) throws {
        try state.withLock { state in
            try Self.validateAvailable(state, operation: operation)
        }
    }

    private func beginOperation(_ operation: StorageOperation) throws {
        try state.withLock { state in
            try Self.validateAvailable(state, operation: operation)
            let nextCount = state.activeOperationCount.addingReportingOverflow(1)
            guard !nextCount.overflow else {
                throw Self.lifecycleError(
                    "FoundationDB transaction operation count overflowed",
                    operation: operation,
                    code: .backendContractViolation
                )
            }
            state.activeOperationCount = nextCount.partialValue
        }
    }

    private func finishOperation() {
        state.withLock { state in
            precondition(
                state.activeOperationCount > 0,
                "FoundationDB transaction operation count underflow"
            )
            state.activeOperationCount -= 1
        }
    }

    func makeOperationLease(
        for operation: StorageOperation
    ) throws -> FDBStorageOperationLease {
        try beginOperation(operation)
        return FDBStorageOperationLease { [self] in
            finishOperation()
        }
    }

    func validateOperationLease(for operation: StorageOperation) throws {
        try state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: operation)
        }
    }

    private static func validateAvailable(
        _ state: MutableState,
        operation: StorageOperation
    ) throws {
        try validateOpen(state.lifecycle, operation: operation)
        guard state.activeDirectoryOperation == nil else {
            throw lifecycleError(
                "FoundationDB transaction has an active directory operation",
                operation: operation
            )
        }
    }

    private static func validateExclusiveAccess(
        _ state: MutableState,
        operation: StorageOperation
    ) throws {
        try validateAvailable(state, operation: operation)
        guard state.activeOperationCount == 0 else {
            throw lifecycleError(
                "FoundationDB transaction has active operations",
                operation: operation,
                code: .transactionBusy
            )
        }
    }

    private static func validateOpen(
        _ lifecycle: Lifecycle,
        operation: StorageOperation
    ) throws {
        switch lifecycle {
        case .open:
            return
        case .preparing, .failingPreparation, .cancellingPreparation,
             .committing:
            throw lifecycleError("FoundationDB transaction is committing", operation: operation)
        case .committed:
            throw lifecycleError("FoundationDB transaction committed", operation: operation)
        case .cancelling, .cancelled:
            throw cancellationError(operation: operation)
        case .failed(let error), .commitUnknown(let error):
            throw error
        }
    }

    private static func lifecycleError(
        _ message: String,
        operation: StorageOperation,
        code: StorageError.Code = .invalidOperation
    ) -> StorageError {
        StorageError(
            code: code,
            operation: operation,
            backend: .foundationDB,
            message: message
        )
    }

    private static func cancellationError(
        operation: StorageOperation
    ) -> StorageError {
        StorageError(
            code: .transactionCancelled,
            operation: operation,
            backend: .foundationDB,
            message: "FoundationDB transaction was cancelled"
        )
    }
}
