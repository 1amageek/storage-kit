import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit
import Synchronization

/// Transaction facade for one Cloudflare Durable Object partition identity.
public final class CloudflareDurableObjectStorageTransaction: Transaction, Sendable {
    public typealias RangeResult = CloudflareDurableObjectRangeResult

    public static let declaredCapabilities = TransactionCapabilities(
        transactionCheckpoints: true,
        transactionTimeout: true,
        readVersion: true,
        committedVersion: true,
        explicitConflictRanges: true,
        committedVersionstamp: true,
        versionstampedMutations: true
    )

    public var capabilities: TransactionCapabilities { Self.declaredCapabilities }
    public var compaction: StorageCompactionAccess? { nil }
    public var mutationByteLimit: Int? { mutationByteMeter.maximumBytes }
    public let transactionDomain: StorageTransactionDomain

    public var storageFailure: StorageError? {
        state.withLock { state in
            switch state.phase {
            case .failed(let error): return error as? StorageError
            case .commitUnknown(let error): return error
            case .open, .sealing, .committing, .committed, .cancelling, .cancelled:
                return nil
            }
        }
    }

    private let partitionIdentity: StoragePartitionIdentity
    private let client: any CloudflareDurableObjectStorageClient
    private let limits: CloudflareDurableObjectLimits
    private let monotonicClock: any StorageMonotonicClock
    private let state = CloudflareDurableObjectTransactionStateOwner()
    private let checkpointMode = Mutex<TransactionJournal?>(nil)
    private let mutationByteMeter = TransactionMutationByteMeter()
    private let versionstampCompletion = TransactionVersionstampCompletion()
    private let reader: CloudflareDurableObjectTransactionReader

    public var checkpoints: (any TransactionCheckpointAccess)? {
        checkpointMode.withLock { $0 }
    }

    public func configureCheckpoints(limits: TransactionCheckpointLimits) throws {
        try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .execute)
            guard !state.hasAdmittedAccess else {
                throw TransactionCheckpointError.configurationAfterAdmission
            }
            try checkpointMode.withLock { journal in
                guard journal == nil else {
                    throw TransactionCheckpointError.configurationAfterAdmission
                }
                journal = TransactionJournal(limits: limits,
                    backend: .cloudflareDurableObject, mutationByteMeter: mutationByteMeter)
                state.checkpointEnabled = true
            }
        }
    }

    public func performOperation(
        _ operation: @escaping @Sendable () async throws -> Void
    ) async throws {
        if let journal = try journalForAccess(operation: .execute) {
            try await journal.performOperation(operation)
        } else {
            try await operation()
        }
    }

    private func journalForAccess(operation: StorageOperation) throws -> TransactionJournal? {
        try state.withLock { state in
            let journal = checkpointMode.withLock { $0 }
            if journal == nil {
                try Self.ensureOpen(state.phase, operation: operation)
            } else {
                try Self.ensureHostAccess(state.phase, allowingBarrier: state.checkpointEnabled, operation: operation)
            }
            state.hasAdmittedAccess = true
            return journal
        }
    }

    public func configureMutationByteLimit(maximumBytes: Int?) throws {
        try mutationByteMeter.configure(maximumBytes: maximumBytes)
    }

    private struct CommitBatch: Sendable {
        let observedReadVersion: Int64?
        let mutations: [StorageWireWriteOperation]
        let readConflictRanges: [StorageWireKeyRange]
        let writeConflictRanges: [StorageWireKeyRange]
    }

    init(
        partitionIdentity: StoragePartitionIdentity,
        client: any CloudflareDurableObjectStorageClient,
        limits: CloudflareDurableObjectLimits,
        monotonicClock: any StorageMonotonicClock,
        transactionDomain: StorageTransactionDomain = StorageTransactionDomain()
    ) {
        self.partitionIdentity = partitionIdentity
        self.client = client
        self.limits = limits
        self.monotonicClock = monotonicClock
        self.transactionDomain = transactionDomain
        self.reader = CloudflareDurableObjectTransactionReader(
            partitionIdentity: partitionIdentity,
            client: client,
            limits: limits,
            monotonicClock: monotonicClock,
            state: state,
            versionstampCompletion: versionstampCompletion
        )
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await readValue(
            for: key,
            snapshot: snapshot,
            maximumByteCount: nil
        )
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        guard maximumByteCount >= 0 else {
            throw StorageError.invalidPointReadMaximum(
                maximumByteCount,
                backend: .cloudflareDurableObject
            )
        }
        return try await readValue(
            for: key,
            snapshot: snapshot,
            maximumByteCount: maximumByteCount
        )
    }

    private func readValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int?
    ) async throws -> ByteString? {
        try validateKey(key)
        if let journal = try journalForAccess(operation: .read) {
            let reader = reader
            return try await TransactionOperationExecutor(access: self).withOperation {
                try reader.validateAccess(operation: .read)
                let value = try await journal.getValue(for: key, snapshot: snapshot,
                    maximumByteCount: maximumByteCount, using: reader)
                try reader.validateAccess(operation: .read)
                return value
            }
        }
        let (phase, mutations, observedReadVersion) = state.withLock {
            ($0.phase, $0.mutations, $0.observedReadVersion)
        }
        try Self.ensureOpen(phase, operation: .read)

        let response = try await performHostCall(operation: .read) { [self] in
            try await self.client.read(
                StorageWireReadRequest(
                    partitionIdentity: self.partitionIdentity,
                    key: key,
                    snapshot: snapshot,
                    expectedReadVersion: observedReadVersion
                )
            )
        }
        if let value = response.value {
            try validateStoredRow(key: key, value: value)
        }
        try acceptReadVersion(response.currentCommitVersion)
        if !snapshot {
            try recordReadConflictRange(
                .singleKey(key)
            )
        }

        let result = try value(
            for: key,
            committed: response.value,
            applying: mutations
        )
        if let maximumByteCount, let result {
            guard result.count <= maximumByteCount else {
                throw StorageError.pointReadValueTooLarge(
                    observedByteCount: result.count,
                    maximumByteCount: maximumByteCount,
                    backend: .cloudflareDurableObject
                )
            }
        }
        return result
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> CloudflareDurableObjectRangeResult {
        do {
            if let journal = try journalForAccess(operation: .rangeRead) {
                let reader = reader
                return CloudflareDurableObjectRangeResult(cursor: {
                    journal.rangeCursor(from: begin, to: end, limit: limit,
                        reverse: reverse, snapshot: snapshot,
                        streamingMode: streamingMode, using: reader)
                        .validatingScope { try reader.validateAccess(operation: .rangeRead) }
                })
            }
        } catch {
            return CloudflareDurableObjectRangeResult(cursor: {
                KeyValueCursor(validatingScope: { throw error })
            })
        }
        return CloudflareDurableObjectRangeResult { [self] in
            let (mutations, observedReadVersion) = state.withLock {
                ($0.mutations, $0.observedReadVersion)
            }

            return CloudflareDurableObjectRangeScan(
                read: { [self] request in
                    try await self.performHostCall(operation: .read) { [self] in
                        try await self.client.read(request)
                    }
                },
                range: { [self] request in
                    try await self.performHostCall(operation: .rangeRead) { [self] in
                        try await self.client.range(request)
                    }
                },
                partitionIdentity: partitionIdentity,
                begin: begin,
                end: end,
                snapshot: snapshot,
                initialExpectedReadVersion: observedReadVersion,
                pageLimit: limits.maxRangeLimit,
                maxSelectorResolutionSteps: limits.maxSelectorResolutionSteps,
                userLimit: limit,
                reverse: reverse,
                mutations: mutations,
                ensureOpen: { [self] in
                    let currentPhase = state.withLock { $0.phase }
                    try Self.ensureOpen(currentPhase, operation: .rangeRead)
                },
                recordReadVersion: { [self] version in
                    try acceptReadVersion(version)
                },
                recordReadConflictRange: { [self] range in
                    try recordReadConflictRange(range)
                },
                validateStoredRow: { [self] key, value in
                    try validateStoredRow(key: key, value: value)
                }
            )
        }
    }

    public func setValue(_ value: ByteString, for key: ByteString) throws {
        try validateKey(key)
        try validateValue(value)
        try validateStoredPair(keyBytes: key.count, valueBytes: value.count)
        if let journal = try journalForAccess(operation: .write) {
            try journal.append(.set(key: key, value: value, suppressWriteConflict: false))
            return
        }
        try state.withLock { state in
            try prepareMutation(state: &state)
            try mutationByteMeter.recordSet(
                key: key,
                value: value
            )
            state.mutations.append(
                .set(
                    key: key,
                    value: value
                )
            )
        }
    }

    public func clear(key: ByteString) throws {
        try validateKey(key)
        if let journal = try journalForAccess(operation: .write) {
            try journal.append(.clear(key: key, suppressWriteConflict: false))
            return
        }
        try state.withLock { state in
            try prepareMutation(state: &state)
            try mutationByteMeter.recordClear(key: key)
            state.mutations.append(
                .clear(key: key)
            )
        }
    }

    public func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        try validateBoundary(beginKey)
        try validateBoundary(endKey)
        guard
            CloudflareDurableObjectByteOrdering.compare(
                beginKey,
                endKey
            ) <= 0
        else {
            throw StorageError(
                code: .invalidOperation,
                operation: .deleteRange,
                backend: .cloudflareDurableObject,
                message: "Clear range boundaries are not ordered"
            )
        }
        if let journal = try journalForAccess(operation: .deleteRange) {
            try journal.append(.clearRange(begin: beginKey, end: endKey, suppressWriteConflict: false))
            return
        }
        try state.withLock { state in
            try prepareMutation(state: &state)
            try mutationByteMeter.recordClearRange(
                beginKey: beginKey,
                endKey: endKey
            )
            state.mutations.append(
                .clearRange(
                    begin: beginKey,
                    end: endKey
                )
            )
        }
    }

    public func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        switch mutationType {
        case .setVersionstampedKey:
            try validateBytes(
                key,
                maximum: limits.maxKeyBytes + 4,
                message: "Versionstamped key operand exceeds configured byte limit"
            )
            try validateValue(param)
            if key.count >= 4 {
                try validateStoredPair(
                    keyBytes: key.count - 4,
                    valueBytes: param.count
                )
            }
        case .setVersionstampedValue:
            try validateKey(key)
            try validateBytes(
                param,
                maximum: limits.maxValueBytes + 4,
                message: "Versionstamped value operand exceeds configured byte limit"
            )
            if param.count >= 4 {
                try validateStoredPair(
                    keyBytes: key.count,
                    valueBytes: param.count - 4
                )
            }
        default:
            try validateKey(key)
            try validateValue(param)
            switch mutationType {
            case .compareAndClear:
                break
            default:
                try validateStoredPair(
                    keyBytes: key.count,
                    valueBytes: param.count
                )
            }
        }
        if let journal = try journalForAccess(operation: .write) {
            try journal.append(.atomic(key: key, parameter: param, type: mutationType,
                suppressWriteConflict: false))
            return
        }
        try state.withLock { state in
            try prepareMutation(state: &state)
            try mutationByteMeter.recordAtomic(
                key: key,
                parameter: param
            )
            state.mutations.append(
                .atomic(
                    key: key,
                    param: param,
                    mutationType:
                        StorageWireMutationType(mutationType)
                )
            )
        }
    }

    public func commit() async throws {
        enum Start {
            case leader(TransactionOperationCompletion, CommitBatch?)
            case waitForCommit(TransactionOperationCompletion)
            case waitForCancellation(TransactionOperationCompletion)
            case committed
            case cancelled
            case failed(any Error)
        }

        let start = state.withLock { state -> Start in
            switch state.phase {
            case .open:
                state.hasAdmittedAccess = true
                if checkpointMode.withLock({ $0 != nil }) {
                    let completion = TransactionOperationCompletion()
                    state.checkpointCompletion = completion
                    state.phase = .sealing(completion)
                    return .leader(completion, nil)
                }
                do {
                    try ensureDeadlineNotExpired(
                        state.deadline,
                        operation: .commit
                    )
                    try validate(state.mutations)
                    guard state.readConflictRanges.count <= limits.maxConflictRangesPerCommit,
                        state.writeConflictRanges.count <= limits.maxConflictRangesPerCommit
                    else {
                        throw StorageError(
                            code: .invalidOperation,
                            operation: .commit,
                            backend: .cloudflareDurableObject,
                            message: "Conflict range batch exceeds configured limit"
                        )
                    }
                } catch {
                    let storageError = Self.storageError(
                        error,
                        operation: .commit
                    )
                    state.phase = .failed(storageError)
                    Self.clearPendingState(&state)
                    return .failed(storageError)
                }
                let completion = TransactionOperationCompletion()
                let payload = CommitBatch(
                    observedReadVersion: state.observedReadVersion,
                    mutations: state.mutations,
                    readConflictRanges: state.readConflictRanges,
                    writeConflictRanges: state.writeConflictRanges
                )
                state.phase = .committing(completion)
                Self.clearPendingState(&state)
                return .leader(completion, payload)
            case .sealing(let completion), .committing(let completion):
                return .waitForCommit(completion)
            case .committed:
                if let completion = state.checkpointCompletion { return .waitForCommit(completion) }
                return .committed
            case .cancelling(let completion):
                return .waitForCancellation(completion)
            case .cancelled:
                return .cancelled
            case .failed(let error):
                if let completion = state.checkpointCompletion { return .waitForCommit(completion) }
                return .failed(error)
            case .commitUnknown(let error):
                if let completion = state.checkpointCompletion { return .waitForCommit(completion) }
                return .failed(error)
            }
        }

        switch start {
        case .leader(let completion, let bufferedPayload):
            let journal = checkpointMode.withLock { $0 }
            do {
                let payload: CommitBatch
                if let bufferedPayload {
                    payload = bufferedPayload
                } else {
                    guard let journal else { throw TransactionCheckpointError.closed }
                    let entries = try await journal.sealForCommit()
                    payload = try state.withLock { state in
                        try ensureDeadlineNotExpired(state.deadline, operation: .commit)
                        for entry in entries {
                            switch entry {
                            case .set(let key, let value, let suppress):
                                try validateWriteConflictSuppression(suppress)
                                state.mutations.append(.set(key: key, value: value))
                            case .clear(let key, let suppress):
                                try validateWriteConflictSuppression(suppress)
                                state.mutations.append(.clear(key: key))
                            case .clearRange(let begin, let end, let suppress):
                                try validateWriteConflictSuppression(suppress)
                                state.mutations.append(.clearRange(begin: begin, end: end))
                            case .atomic(let key, let parameter, let type, let suppress):
                                try validateWriteConflictSuppression(suppress)
                                state.mutations.append(.atomic(key: key, param: parameter,
                                    mutationType: StorageWireMutationType(type)))
                            case .writeConflict(let begin, let end):
                                state.writeConflictRanges = Self.merging(state.writeConflictRanges,
                                    with: StorageWireKeyRange(begin: begin, end: end))
                            }
                        }
                        try validate(state.mutations)
                        guard state.readConflictRanges.count <= limits.maxConflictRangesPerCommit,
                              state.writeConflictRanges.count <= limits.maxConflictRangesPerCommit else {
                            throw StorageError(code: .transactionTooLarge, operation: .commit,
                                backend: .cloudflareDurableObject,
                                message: "Checkpoint conflict range batch exceeds configured limit")
                        }
                        let batch = CommitBatch(observedReadVersion: state.observedReadVersion,
                            mutations: state.mutations, readConflictRanges: state.readConflictRanges,
                            writeConflictRanges: state.writeConflictRanges)
                        state.phase = .committing(completion)
                        Self.clearPendingState(&state)
                        return batch
                    }
                }
                try await performCommit(payload)
                if let journal { try await journal.close(committed: true) }
                completion.succeed()
            } catch {
                let failure = error
                let unknown = state.withLock { state -> Bool in
                    if case .commitUnknown = state.phase { return true }
                    state.phase = .failed(failure)
                    Self.clearPendingState(&state)
                    return false
                }
                do {
                    if let journal {
                        if unknown { try await journal.closeWithUnknownCommit() }
                        else { try await journal.close(committed: false) }
                    }
                } catch {
                    let combined = StorageTransactionCleanupError(
                        operationError: failure, cancellationError: error)
                    state.withLock { $0.phase = .failed(combined) }
                    versionstampCompletion.fail(combined)
                    completion.fail(combined)
                    throw combined
                }
                versionstampCompletion.fail(failure)
                completion.fail(failure)
                throw failure
            }
        case .waitForCommit(let completion):
            try await completion.wait()
        case .waitForCancellation(let completion):
            try await completion.wait()
            throw Self.phaseError(.cancelled, operation: .commit)
        case .committed:
            return
        case .cancelled:
            throw Self.phaseError(.cancelled, operation: .commit)
        case .failed(let error):
            let operationError = error
            if let journal = checkpointMode.withLock({ $0 }) {
                do { try await journal.close(committed: false) }
                catch { throw StorageTransactionCleanupError(operationError: operationError, cancellationError: error) }
            }
            versionstampCompletion.fail(operationError)
            throw operationError
        }
    }

    private func performCommit(
        _ payload: CommitBatch
    ) async throws(StorageError) {
        do {
            let response = try await performHostCall(operation: .commit) { [self] in
                try await self.client.commit(
                    StorageWireCommitRequest(
                        partitionIdentity: self.partitionIdentity,
                        observedReadVersion: payload.observedReadVersion,
                        mutations: payload.mutations,
                        readConflictRanges: payload.readConflictRanges,
                        writeConflictRanges: payload.writeConflictRanges
                    )
                )
            }

            guard response.committedVersion >= 0 else {
                throw commitUnknownError(
                    underlyingDescription: "Host returned a negative committed version"
                )
            }

            let versionstamp = try TransactionVersionstamp(
                committedVersion: response.committedVersion
            )
            state.withLock { state in
                state.phase = .committed
                state.committedVersion = response.committedVersion
            }
            versionstampCompletion.succeed(versionstamp)
        } catch is CancellationError {
            let error = commitUnknownError(
                underlyingDescription: "Commit task was cancelled after dispatch"
            )
            state.withLock { state in
                state.phase = .commitUnknown(error)
                Self.clearPendingState(&state)
            }
            versionstampCompletion.fail(error)
            throw error
        } catch let error as StorageError
            where error.code == .transactionTimedOut
        {
            let unknown = commitUnknownError(
                underlyingDescription: error.description
            )
            state.withLock { state in
                state.phase = .commitUnknown(unknown)
                Self.clearPendingState(&state)
            }
            versionstampCompletion.fail(unknown)
            throw unknown
        } catch {
            let storageError = Self.storageError(error, operation: .commit)
            state.withLock { state in
                if storageError.code == .commitUnknownResult {
                    state.phase = .commitUnknown(storageError)
                } else {
                    state.phase = .failed(storageError)
                }
                Self.clearPendingState(&state)
            }
            versionstampCompletion.fail(storageError)
            throw storageError
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
            case commitUnknown(StorageError)
        }

        let start = state.withLock { state -> Start in
            switch state.phase {
            case .open:
                let completion = TransactionOperationCompletion()
                state.phase = .cancelling(completion)
                Self.clearPendingState(&state)
                return .leader(completion)
            case .sealing(let completion), .committing(let completion):
                return .waitForCommit(completion)
            case .committed:
                if let completion = state.checkpointCompletion { return .waitForCommit(completion) }
                return .committed
            case .cancelling(let completion):
                return .waitForCancellation(completion)
            case .cancelled:
                return .cancelled
            case .failed:
                if let completion = state.checkpointCompletion { return .waitForCommit(completion) }
                return .failed
            case .commitUnknown(let error):
                return .commitUnknown(error)
            }
        }

        switch start {
        case .leader(let completion):
            if let journal = checkpointMode.withLock({ $0 }) {
                do {
                    try await journal.close(committed: false)
                } catch {
                    let failure = error
                    state.withLock { $0.phase = .failed(failure) }
                    versionstampCompletion.fail(failure)
                    completion.fail(failure)
                    throw failure
                }
            }
            state.withLock { state in
                Self.clearPendingState(&state)
                state.phase = .cancelled
            }
            versionstampCompletion.fail(
                Self.phaseError(.cancelled, operation: .read)
            )
            completion.succeed()
        case .waitForCancellation(let completion):
            try await completion.wait()
        case .waitForCommit(let completion):
            do {
                try await completion.wait()
            } catch {
                if let error = error as? StorageError, error.code == .commitUnknownResult {
                    throw error
                }
                return
            }
            throw Self.phaseError(.committed, operation: .cancel)
        case .failed:
            if let journal = checkpointMode.withLock({ $0 }) { try await journal.close(committed: false) }
            return
        case .cancelled:
            return
        case .committed:
            throw Self.phaseError(.committed, operation: .cancel)
        case .commitUnknown(let error):
            throw error
        }
    }

    public func setReadVersion(_ version: Int64) throws {
        let journal = try journalForAccess(operation: .read)
        guard version >= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .read,
                backend: .cloudflareDurableObject,
                message: "Transaction read version must be non-negative"
            )
        }
        let update = {
            try self.state.withLock { state in
                try Self.ensureHostAccess(state.phase,
                    allowingBarrier: state.checkpointEnabled, operation: .read)
                if let observedReadVersion = state.observedReadVersion {
                    guard observedReadVersion == version else {
                        throw StorageError(
                            code: .invalidOperation,
                            operation: .read,
                            backend: .cloudflareDurableObject,
                            message: "Transaction read version is already fixed"
                        )
                    }
                    return
                }
                state.observedReadVersion = version
            }
        }
        if let journal { try journal.performSynchronousOperation(update) }
        else { try update() }
    }

    public func getReadVersion() async throws -> Int64 {
        if try journalForAccess(operation: .read) != nil {
            return try await TransactionOperationExecutor(access: self).withOperation { [self] in
                try await readVersionFromHost()
            }
        }
        return try await readVersionFromHost()
    }

    private func readVersionFromHost() async throws -> Int64 {
        let readVersion = try state.withLock { state in
            try Self.ensureHostAccess(state.phase, allowingBarrier: state.checkpointEnabled, operation: .read)
            return state.observedReadVersion
        }
        if let readVersion {
            return readVersion
        }
        let response = try await performHostCall(operation: .read) { [self] in
            try await self.client.readiness(
                StorageWireReadinessRequest(partitionIdentity: self.partitionIdentity)
            )
        }
        try acceptReadVersion(response.commitVersion)
        return response.commitVersion
    }

    public func setOption(forOption option: TransactionOption) throws {
        _ = try journalForAccess(operation: .execute)
        switch option {
        case .timeout(let milliseconds):
            try configureTimeout(milliseconds: milliseconds)
        case .priorityBatch, .prioritySystemImmediate,
            .readPriorityLow, .readPriorityHigh,
            .accessSystemKeys, .readServerSideCacheDisable:
            try state.withLock { state in
                try Self.ensureOpen(state.phase, operation: .execute)
            }
            throw StorageError.unsupportedOperation(
                "Cloudflare Durable Object transactions do not support this transaction option",
                operation: .execute,
                backend: .cloudflareDurableObject
            )
        }
    }

    public func setOption(
        to value: ByteString?,
        forOption option: TransactionOption
    ) throws {
        guard value == nil else {
            throw invalidOptionValue(
                "Cloudflare transaction options do not accept byte values"
            )
        }
        try setOption(forOption: option)
    }

    public func setOption(
        to value: Int,
        forOption option: TransactionOption
    ) throws {
        guard case .timeout(let declaredMilliseconds) = option else {
            throw invalidOptionValue(
                "Only the timeout option accepts an integer value"
            )
        }
        guard value == declaredMilliseconds else {
            throw invalidOptionValue(
                "Timeout option and integer value must match"
            )
        }
        try configureTimeout(milliseconds: value)
    }

    public func addConflictRange(
        beginKey: ByteString,
        endKey: ByteString,
        type: ConflictRangeType
    ) throws {
        try validateBoundary(beginKey)
        try validateBoundary(endKey)
        guard
            CloudflareDurableObjectByteOrdering.compare(
                beginKey,
                endKey
            ) < 0
        else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .cloudflareDurableObject,
                message: "Conflict range must be non-empty and ordered"
            )
        }
        let journal = try journalForAccess(operation: .write)
        if case .write = type, let journal {
            try journal.append(.writeConflict(begin: beginKey, end: endKey))
            return
        }
        let range = StorageWireKeyRange(
            begin: beginKey,
            end: endKey
        ).detached()
        let update = {
            try self.state.withLock { state in
                try Self.ensureHostAccess(state.phase,
                    allowingBarrier: state.checkpointEnabled, operation: .write)
                switch type {
                case .read:
                    state.readConflictRanges = Self.merging(
                        state.readConflictRanges, with: range)
                case .write:
                    state.writeConflictRanges = Self.merging(
                        state.writeConflictRanges, with: range)
                }
            }
        }
        if let journal { try journal.performSynchronousOperation(update) }
        else { try update() }
    }

    public func getEstimatedRangeSizeBytes(
        beginKey: ByteString,
        endKey: ByteString
    ) async throws -> Int {
        try validateOrderedRange(
            beginKey: beginKey,
            endKey: endKey,
            message: "Range size boundaries are not ordered"
        )
        let journal = try journalForAccess(operation: .rangeRead)
        let transactionView = try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .rangeRead)
            return (
                readVersion: state.observedReadVersion,
                hasPendingMutations: !state.mutations.isEmpty
            )
        }
        if journal != nil || transactionView.hasPendingMutations {
            return try await StorageRangeMetrics.exactSize(
                getRange(
                    from: .firstGreaterOrEqual(beginKey),
                    to: .firstGreaterOrEqual(endKey),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .wantAll
                )
            )
        }
        let response = try await performHostCall(operation: .rangeRead) { [self] in
            try await self.client.rangeSize(
                StorageWireRangeSizeRequest(
                    partitionIdentity: self.partitionIdentity,
                    begin: beginKey,
                    end: endKey,
                    expectedReadVersion: transactionView.readVersion
                )
            )
        }
        try acceptReadVersion(response.currentCommitVersion)
        guard response.byteCount >= 0,
            let result = Int(exactly: response.byteCount)
        else {
            throw StorageError(
                code: .dataCorruption,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Host returned an invalid range byte count"
            )
        }
        return result
    }

    public func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString] {
        try validateOrderedRange(
            beginKey: beginKey,
            endKey: endKey,
            message: "Split point boundaries are not ordered"
        )
        guard let wireChunkSize = Int64(exactly: chunkSize),
            wireChunkSize > 0
        else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Split point chunk size must be positive"
            )
        }
        let journal = try journalForAccess(operation: .rangeRead)
        let transactionView = try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .rangeRead)
            return (
                readVersion: state.observedReadVersion,
                hasPendingMutations: !state.mutations.isEmpty
            )
        }
        if journal != nil || transactionView.hasPendingMutations {
            return try await StorageRangeMetrics.splitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize,
                maximumPointCount: limits.maxSplitPoints,
                rows: getRange(
                    from: .firstGreaterOrEqual(beginKey),
                    to: .firstGreaterOrEqual(endKey),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .wantAll
                )
            )
        }
        let response = try await performHostCall(operation: .rangeRead) { [self] in
            try await self.client.rangeSplitPoints(
                StorageWireRangeSplitPointsRequest(
                    partitionIdentity: self.partitionIdentity,
                    begin: beginKey,
                    end: endKey,
                    chunkSize: wireChunkSize,
                    expectedReadVersion: transactionView.readVersion
                )
            )
        }
        try acceptReadVersion(response.currentCommitVersion)
        guard response.splitPoints.count <= limits.maxSplitPoints else {
            throw invalidSplitPoints()
        }
        let points = response.splitPoints
        try validateSplitPoints(
            points,
            beginKey: beginKey,
            endKey: endKey
        )
        return points
    }

    public func getCommittedVersion() throws -> Int64 {
        try state.withLock { state in
            guard case .committed = state.phase,
                let committedVersion = state.committedVersion
            else {
                throw Self.phaseError(state.phase, operation: .read)
            }
            return committedVersion
        }
    }

    public func requestVersionstamp() -> any PendingTransactionVersionstamp {
        state.withLock { $0.hasAdmittedAccess = true }
        let failure = state.withLock { state -> (any Error)? in
            guard case .open = state.phase else {
                return Self.phaseError(state.phase, operation: .read)
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

    private func value(
        for key: ByteString,
        committed: ByteString?,
        applying mutations: [StorageWireWriteOperation]
    ) throws -> ByteString? {
        var value = committed
        for mutation in mutations {
            switch mutation {
            case .set(let opKey, let opValue)
            where opKey == key:
                value = opValue
            case .clear(let opKey) where opKey == key:
                value = nil
            case .clearRange(let begin, let end)
            where CloudflareDurableObjectByteOrdering.compare(
                key,
                begin
            ) >= 0
                && CloudflareDurableObjectByteOrdering.compare(
                    key,
                    end
                ) < 0:
                value = nil
            case .atomic(let opKey, let param, let mutationType)
            where opKey == key:
                switch try mutationType.mutationType.apply(
                    to: value,
                    param: param
                ) {
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

    private func validateWriteConflictSuppression(_ suppress: Bool) throws {
        guard !suppress else {
            throw StorageError.unsupportedOperation(
                "Durable Object Wire v1 does not support suppressing write conflicts",
                operation: .write, backend: .cloudflareDurableObject)
        }
    }

    private func validate(
        _ mutations: [StorageWireWriteOperation]
    ) throws {
        guard mutations.count <= limits.maxMutationsPerCommit else {
            throw StorageError(
                code: .invalidOperation,
                operation: .commit,
                backend: .cloudflareDurableObject,
                message: "Mutation batch exceeds configured limit"
            )
        }
        for mutation in mutations {
            switch mutation {
            case .set(let key, let value):
                try validateKey(key)
                try validateValue(value)
                try validateStoredPair(
                    keyBytes: key.count,
                    valueBytes: value.count
                )
            case .clear(let key):
                try validateKey(key)
            case .clearRange(let begin, let end):
                try validateBoundary(begin)
                try validateBoundary(end)
            case .atomic(let key, let param, let mutationType):
                switch mutationType {
                case .setVersionstampedKey:
                    try validateBytes(
                        key,
                        maximum: limits.maxKeyBytes + 4,
                        message: "Versionstamped key operand exceeds configured byte limit"
                    )
                    try validateValue(param)
                    if key.count >= 4 {
                        try validateStoredPair(
                            keyBytes: key.count - 4,
                            valueBytes: param.count
                        )
                    }
                case .setVersionstampedValue:
                    try validateKey(key)
                    try validateBytes(
                        param,
                        maximum: limits.maxValueBytes + 4,
                        message: "Versionstamped value operand exceeds configured byte limit"
                    )
                    if param.count >= 4 {
                        try validateStoredPair(
                            keyBytes: key.count,
                            valueBytes: param.count - 4
                        )
                    }
                default:
                    try validateKey(key)
                    try validateValue(param)
                    switch mutationType {
                    case .compareAndClear:
                        break
                    default:
                        try validateStoredPair(
                            keyBytes: key.count,
                            valueBytes: param.count
                        )
                    }
                }
            }
        }
    }

    private func validateKey(_ key: ByteString) throws {
        guard key.count <= limits.maxKeyBytes else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .cloudflareDurableObject,
                message: "Key exceeds configured byte limit"
            )
        }
    }

    private func validateValue(_ value: ByteString) throws {
        try validateBytes(
            value,
            maximum: limits.maxValueBytes,
            message: "Value exceeds configured byte limit"
        )
    }

    private func validateBytes(
        _ bytes: ByteString,
        maximum: Int,
        message: String
    ) throws {
        guard bytes.count <= maximum else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .cloudflareDurableObject,
                message: message
            )
        }
    }

    private func validateStoredPair(
        keyBytes: Int,
        valueBytes: Int
    ) throws {
        let total = keyBytes.addingReportingOverflow(valueBytes)
        guard !total.overflow,
              total.partialValue <= limits.maxStoredKeyValueBytes else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .cloudflareDurableObject,
                message: "Stored key and value exceed configured combined byte limit"
            )
        }
    }

    private func validateStoredRow(
        key: ByteString,
        value: ByteString
    ) throws {
        try validateKey(key)
        try validateValue(value)
        try validateStoredPair(keyBytes: key.count, valueBytes: value.count)
    }

    private func validateBoundary(
        _ boundary: ByteString,
        operation: StorageOperation = .write
    ) throws {
        guard boundary.count <= limits.maxBoundaryBytes else {
            throw StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Conflict boundary exceeds configured byte limit"
            )
        }
    }

    private func validateOrderedRange(
        beginKey: ByteString,
        endKey: ByteString,
        message: String
    ) throws {
        try validateBoundary(beginKey, operation: .rangeRead)
        try validateBoundary(endKey, operation: .rangeRead)
        guard
            CloudflareDurableObjectByteOrdering.compare(
                beginKey,
                endKey
            ) <= 0
        else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: message
            )
        }
    }

    private func validateSplitPoints(
        _ points: [ByteString],
        beginKey: ByteString,
        endKey: ByteString
    ) throws {
        let expectedMinimumCount = beginKey == endKey ? 1 : 2
        guard points.count >= expectedMinimumCount,
            points.count <= limits.maxSplitPoints,
            points.first == beginKey,
            points.last == endKey
        else {
            throw invalidSplitPoints()
        }
        for point in points {
            try validateBoundary(point, operation: .rangeRead)
        }
        for index in 1..<points.count {
            guard
                CloudflareDurableObjectByteOrdering.compare(
                    points[index - 1],
                    points[index]
                ) < 0
            else {
                throw invalidSplitPoints()
            }
        }
    }

    private func invalidSplitPoints() -> StorageError {
        StorageError(
            code: .dataCorruption,
            operation: .rangeRead,
            backend: .cloudflareDurableObject,
            message: "Host returned invalid range split points"
        )
    }

    private func acceptReadVersion(_ version: Int64) throws {
        try reader.acceptReadVersion(version)
    }

    private func recordReadConflictRange(_ range: StorageWireKeyRange) throws {
        try reader.recordReadConflictRange(range)
    }

    static func merging(
        _ ranges: [StorageWireKeyRange],
        with range: StorageWireKeyRange
    ) -> [StorageWireKeyRange] {
        let sorted = (ranges + [range]).sorted {
            compareBegin($0.begin, $1.begin) < 0
        }
        var merged: [StorageWireKeyRange] = []
        merged.reserveCapacity(sorted.count)
        for candidate in sorted {
            guard let previous = merged.last,
                overlapsOrTouches(previous, candidate)
            else {
                merged.append(candidate)
                continue
            }
            merged[merged.count - 1] = StorageWireKeyRange(
                begin: previous.begin,
                end: maximumEnd(previous.end, candidate.end)
            )
        }
        return merged
    }

    private static func compareBegin(
        _ left: ByteString?,
        _ right: ByteString?
    ) -> Int {
        guard let left else {
            return right == nil ? 0 : -1
        }
        guard let right else {
            return 1
        }
        return CloudflareDurableObjectByteOrdering.compare(
            left,
            right
        )
    }

    private static func overlapsOrTouches(
        _ left: StorageWireKeyRange,
        _ right: StorageWireKeyRange
    ) -> Bool {
        guard let leftEnd = left.end, let rightBegin = right.begin else {
            return true
        }
        return CloudflareDurableObjectByteOrdering.compare(
            rightBegin,
            leftEnd
        ) <= 0
    }

    private static func maximumEnd(
        _ left: ByteString?,
        _ right: ByteString?
    ) -> ByteString? {
        guard let left, let right else {
            return nil
        }
        return CloudflareDurableObjectByteOrdering.compare(
            left,
            right
        ) >= 0 ? left : right
    }

    static func ensureHostAccess(
        _ phase: CloudflareDurableObjectTransactionPhase,
        allowingBarrier: Bool,
        operation: StorageOperation
    ) throws {
        if !allowingBarrier {
            try ensureOpen(phase, operation: operation)
            return
        }
        switch phase {
        case .open, .sealing, .cancelling: return
        default: throw phaseError(phase, operation: operation)
        }
    }

    static func ensureOpen(
        _ phase: CloudflareDurableObjectTransactionPhase,
        operation: StorageOperation
    ) throws {
        guard case .open = phase else {
            throw phaseError(phase, operation: operation)
        }
    }

    static func phaseError(
        _ phase: CloudflareDurableObjectTransactionPhase,
        operation: StorageOperation
    ) -> any Error {
        switch phase {
        case .open:
            return StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Transaction is open"
            )
        case .sealing:
            return lifecycleError("Transaction is sealing checkpoint access", operation: operation)
        case .committing:
            return lifecycleError("Transaction is committing", operation: operation)
        case .committed:
            return lifecycleError("Transaction already committed", operation: operation)
        case .cancelling:
            return lifecycleError("Transaction is cancelling", operation: operation)
        case .cancelled:
            return lifecycleError("Transaction cancelled", operation: operation)
        case .failed(let error): return error
        case .commitUnknown(let error): return error
        }
    }

    private static func lifecycleError(
        _ message: String,
        operation: StorageOperation
    ) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: .cloudflareDurableObject,
            message: message
        )
    }

    private static func storageError(
        _ error: any Error,
        operation: StorageOperation
    ) -> StorageError {
        if let storageError = error as? StorageError {
            return storageError
        }
        return StorageError(
            code: .backendFailure,
            operation: operation,
            backend: .cloudflareDurableObject,
            message: "Cloudflare Durable Object transaction failed",
            underlyingDescription: underlyingStorageErrorDescription(error)
        )
    }

    static func clearPendingState(
        _ state: inout CloudflareDurableObjectTransactionState
    ) {
        state.mutations.removeAll(keepingCapacity: false)
        state.readConflictRanges.removeAll(keepingCapacity: false)
        state.writeConflictRanges.removeAll(keepingCapacity: false)
    }

    private func prepareMutation(
        state: inout CloudflareDurableObjectTransactionState
    ) throws {
        try Self.ensureOpen(state.phase, operation: .write)
        guard state.mutations.count < limits.maxMutationsPerCommit else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .cloudflareDurableObject,
                message: "Mutation batch exceeds configured limit"
            )
        }
    }

    private func configureTimeout(milliseconds: Int) throws {
        let journal = try journalForAccess(operation: .execute)
        guard milliseconds >= 0,
            let wireMilliseconds = Int64(exactly: milliseconds)
        else {
            throw invalidOptionValue(
                "Timeout milliseconds must be a nonnegative Int64 value"
            )
        }
        let deadline =
            wireMilliseconds == 0
            ? nil
            : monotonicClock.now.advanced(
                by: .milliseconds(wireMilliseconds)
            )
        let update = {
            try self.state.withLock { state in
                try Self.ensureHostAccess(state.phase,
                    allowingBarrier: state.checkpointEnabled, operation: .execute)
                state.deadline = deadline
            }
        }
        if let journal { try journal.performSynchronousOperation(update) }
        else { try update() }
    }

    private func invalidOptionValue(_ message: String) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: .execute,
            backend: .cloudflareDurableObject,
            message: message
        )
    }

    private func performHostCall<T: Sendable>(
        operation: StorageOperation,
        _ body: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        try await reader.performHostCall(operation: operation, body)
    }

    private func ensureDeadlineNotExpired(
        _ deadline: StorageInstant?,
        operation: StorageOperation
    ) throws {
        guard let deadline else { return }
        guard monotonicClock.now < deadline else {
            throw StorageError(
                code: .transactionTimedOut,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object transaction timed out"
            )
        }
    }

    private func commitUnknownError(
        underlyingDescription: String
    ) -> StorageError {
        StorageError(
            code: .commitUnknownResult,
            operation: .commit,
            backend: .cloudflareDurableObject,
            message: "Cloudflare Durable Object commit result is unknown",
            underlyingDescription: underlyingDescription
        )
    }

}

extension CloudflareDurableObjectStorageTransaction {
    public func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        if let journal = try journalForAccess(operation: .rangeRead) {
            let reader = reader
            return try await TransactionOperationExecutor(access: self).withOperation {
                try reader.validateAccess(operation: .rangeRead)
                let key = try await journal.getKey(selector: selector, snapshot: snapshot, using: reader)
                try reader.validateAccess(operation: .rangeRead)
                return key
            }
        }
        return try await TransactionKeySelection.resolve(
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
