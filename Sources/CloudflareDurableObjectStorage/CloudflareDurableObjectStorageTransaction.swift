import StorageKit
import Synchronization

/// Transaction facade for one Cloudflare Durable Object scope.
public final class CloudflareDurableObjectStorageTransaction: Transaction, Sendable {
    public typealias RangeResult = CloudflareDurableObjectRangeResult

    public static let declaredCapabilities = TransactionCapabilities(
            transactionTimeout: true,
            readVersion: true,
            committedVersion: true,
            explicitConflictRanges: true,
            committedVersionstamp: true,
            versionstampedMutations: true
    )

    public var capabilities: TransactionCapabilities { Self.declaredCapabilities }
    public var mutationByteLimit: Int? { mutationByteMeter.maximumBytes }
    public let transactionDomain: StorageTransactionDomain

    private let scope: CloudflareDurableObjectStorageScope
    private let client: any CloudflareDurableObjectStorageClient
    private let limits: CloudflareDurableObjectLimits
    private let monotonicClock: any StorageMonotonicClock
    private let state = Mutex(CloudflareDurableObjectTransactionState())
    private let mutationByteMeter = TransactionMutationByteMeter()
    private let versionstampCompletion = TransactionVersionstampCompletion()

    public func configureMutationByteLimit(maximumBytes: Int?) throws {
        try mutationByteMeter.configure(maximumBytes: maximumBytes)
    }

    private struct CommitPayload: Sendable {
        let observedReadVersion: Int64?
        let writeBuffer: [CloudflareDurableObjectWriteOp]
        let readConflictRanges: [CloudflareDurableObjectConflictRange]
        let writeConflictRanges: [CloudflareDurableObjectConflictRange]
    }

    init(
        scope: CloudflareDurableObjectStorageScope,
        client: any CloudflareDurableObjectStorageClient,
        limits: CloudflareDurableObjectLimits,
        monotonicClock: any StorageMonotonicClock,
        transactionDomain: StorageTransactionDomain = StorageTransactionDomain()
    ) {
        self.scope = scope
        self.client = client
        self.limits = limits
        self.monotonicClock = monotonicClock
        self.transactionDomain = transactionDomain
    }

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        try validateKey(key)
        let (phase, writeBuffer, observedReadVersion) = state.withLock {
            ($0.phase, $0.writeBuffer, $0.observedReadVersion)
        }
        try Self.ensureOpen(phase, operation: .read)

        let response = try await performHostCall(operation: .read) { [self] in
            try await self.client.read(
                CloudflareDurableObjectReadRequest(
                    scope: self.scope,
                    key: CloudflareDurableObjectBytes(key),
                    snapshot: snapshot,
                    expectedReadVersion: observedReadVersion
                )
            )
        }
        try acceptReadVersion(response.currentCommitVersion)
        if !snapshot {
            try recordReadConflictRange(
                .singleKey(CloudflareDurableObjectBytes(key))
            )
        }

        return try value(for: key, committed: response.value?.rawValue, applying: writeBuffer)
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> CloudflareDurableObjectRangeResult {
        CloudflareDurableObjectRangeResult { [self] in
            let (writeBuffer, observedReadVersion) = state.withLock {
                ($0.writeBuffer, $0.observedReadVersion)
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
                scope: scope,
                begin: begin,
                end: end,
                snapshot: snapshot,
                initialExpectedReadVersion: observedReadVersion,
                pageLimit: limits.maxRangeLimit,
                maxSelectorResolutionSteps: limits.maxSelectorResolutionSteps,
                userLimit: limit,
                reverse: reverse,
                writeBuffer: writeBuffer,
                ensureOpen: { [self] in
                    let currentPhase = state.withLock { $0.phase }
                    try Self.ensureOpen(currentPhase, operation: .rangeRead)
                },
                recordReadVersion: { [self] version in
                    try acceptReadVersion(version)
                },
                recordReadConflictRange: { [self] range in
                    try recordReadConflictRange(range)
                }
            )
        }
    }

    public func setValue(_ value: Bytes, for key: Bytes) throws {
        try validateKey(key)
        try validateValue(value)
        try state.withLock { state in
            try prepareMutation(state: &state)
            try mutationByteMeter.recordSet(
                key: key,
                value: value
            )
            state.writeBuffer.append(.set(key: key, value: value))
        }
    }

    public func clear(key: Bytes) throws {
        try validateKey(key)
        try state.withLock { state in
            try prepareMutation(state: &state)
            try mutationByteMeter.recordClear(key: key)
            state.writeBuffer.append(.clear(key: key))
        }
    }

    public func clearRange(beginKey: Bytes, endKey: Bytes) throws {
        try validateBoundary(beginKey)
        try validateBoundary(endKey)
        guard CloudflareDurableObjectByteOrdering.compare(
            beginKey,
            endKey
        ) <= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .deleteRange,
                backend: .cloudflareDurableObject,
                message: "Clear range boundaries are not ordered"
            )
        }
        try state.withLock { state in
            try prepareMutation(state: &state)
            try mutationByteMeter.recordClearRange(
                beginKey: beginKey,
                endKey: endKey
            )
            state.writeBuffer.append(.clearRange(begin: beginKey, end: endKey))
        }
    }

    public func atomicOp(
        key: Bytes,
        param: Bytes,
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
        case .setVersionstampedValue:
            try validateKey(key)
            try validateBytes(
                param,
                maximum: limits.maxValueBytes + 4,
                message: "Versionstamped value operand exceeds configured byte limit"
            )
        default:
            try validateKey(key)
            try validateValue(param)
        }
        try state.withLock { state in
            try prepareMutation(state: &state)
            try mutationByteMeter.recordAtomic(
                key: key,
                parameter: param
            )
            state.writeBuffer.append(.atomic(key: key, param: param, mutationType: mutationType))
        }
    }

    public func commit() async throws {
        enum Start {
            case leader(TransactionOperationCompletion, CommitPayload)
            case waitForCommit(TransactionOperationCompletion)
            case waitForCancellation(TransactionOperationCompletion)
            case committed
            case cancelled
            case failed(StorageError)
        }

        let start = state.withLock { state -> Start in
            switch state.phase {
            case .open:
                do {
                    try ensureDeadlineNotExpired(
                        state.deadline,
                        operation: .commit
                    )
                    try validate(state.writeBuffer)
                    guard state.readConflictRanges.count <=
                            limits.maxConflictRangesPerCommit,
                          state.writeConflictRanges.count <=
                            limits.maxConflictRangesPerCommit else {
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
                let payload = CommitPayload(
                    observedReadVersion: state.observedReadVersion,
                    writeBuffer: state.writeBuffer,
                    readConflictRanges: state.readConflictRanges,
                    writeConflictRanges: state.writeConflictRanges
                )
                state.phase = .committing(completion)
                Self.clearPendingState(&state)
                return .leader(completion, payload)
            case .committing(let completion):
                return .waitForCommit(completion)
            case .committed:
                return .committed
            case .cancelling(let completion):
                return .waitForCancellation(completion)
            case .cancelled:
                return .cancelled
            case .failed(let error), .commitUnknown(let error):
                return .failed(error)
            }
        }

        switch start {
        case .leader(let completion, let payload):
            do {
                try await performCommit(payload)
                completion.resolve(.success(()))
            } catch {
                completion.resolve(.failure(error))
                throw error
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
            versionstampCompletion.resolveIfPending(.failure(error))
            throw error
        }
    }

    private func performCommit(
        _ payload: CommitPayload
    ) async throws(StorageError) {
        do {
            let response = try await performHostCall(operation: .commit) { [self] in
                try await self.client.commit(
                    CloudflareDurableObjectCommitRequest(
                        scope: self.scope,
                        observedReadVersion: payload.observedReadVersion,
                        mutations: payload.writeBuffer.map(\.mutation),
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

            let versionstamp: TransactionVersionstamp
            do {
                versionstamp = try TransactionVersionstamp(
                    committedVersion: response.committedVersion
                )
            } catch let error as StorageError {
                throw error
            } catch {
                throw StorageError(
                    code: .backendContractViolation,
                    operation: .read,
                    backend: .cloudflareDurableObject,
                    message: "Unable to encode the committed versionstamp",
                    underlyingDescription: String(describing: error)
                )
            }
            state.withLock { state in
                state.phase = .committed
                state.committedVersion = response.committedVersion
            }
            versionstampCompletion.resolveIfPending(.success(versionstamp))
        } catch is CancellationError {
            let error = commitUnknownError(
                underlyingDescription: "Commit task was cancelled after dispatch"
            )
            state.withLock { state in
                state.phase = .commitUnknown(error)
                Self.clearPendingState(&state)
            }
            versionstampCompletion.resolveIfPending(.failure(error))
            throw error
        } catch let error as StorageError
            where error.code == .transactionTimedOut {
            let unknown = commitUnknownError(
                underlyingDescription: error.description
            )
            state.withLock { state in
                state.phase = .commitUnknown(unknown)
                Self.clearPendingState(&state)
            }
            versionstampCompletion.resolveIfPending(.failure(unknown))
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
            versionstampCompletion.resolveIfPending(.failure(storageError))
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
            case .commitUnknown(let error):
                return .commitUnknown(error)
            }
        }

        switch start {
        case .leader(let completion):
            state.withLock { $0.phase = .cancelled }
            versionstampCompletion.resolveIfPending(
                .failure(Self.phaseError(.cancelled, operation: .read))
            )
            completion.resolve(.success(()))
        case .waitForCancellation(let completion):
            try await completion.wait()
        case .waitForCommit(let completion):
            do {
                try await completion.wait()
            } catch {
                if error.code == .commitUnknownResult {
                    throw error
                }
                return
            }
            throw Self.phaseError(.committed, operation: .cancel)
        case .cancelled, .failed:
            return
        case .committed:
            throw Self.phaseError(.committed, operation: .cancel)
        case .commitUnknown(let error):
            throw error
        }
    }

    public func setReadVersion(_ version: Int64) throws {
        guard version >= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .read,
                backend: .cloudflareDurableObject,
                message: "Transaction read version must be non-negative"
            )
        }
        try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .read)
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

    public func getReadVersion() async throws -> Int64 {
        let (phase, readVersion) = state.withLock { ($0.phase, $0.observedReadVersion) }
        try Self.ensureOpen(phase, operation: .read)
        if let readVersion {
            return readVersion
        }
        let response = try await performHostCall(operation: .read) { [self] in
            try await self.client.readiness(
                CloudflareDurableObjectReadinessRequest(scope: self.scope)
            )
        }
        try acceptReadVersion(response.commitVersion)
        return response.commitVersion
    }

    public func setOption(forOption option: TransactionOption) throws {
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
        to value: Bytes?,
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
        beginKey: Bytes,
        endKey: Bytes,
        type: ConflictRangeType
    ) throws {
        try validateBoundary(beginKey)
        try validateBoundary(endKey)
        guard CloudflareDurableObjectByteOrdering.compare(
            beginKey,
            endKey
        ) < 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .cloudflareDurableObject,
                message: "Conflict range must be non-empty and ordered"
            )
        }
        let range = CloudflareDurableObjectConflictRange(
            begin: CloudflareDurableObjectBytes(beginKey),
            end: CloudflareDurableObjectBytes(endKey)
        ).detached()
        try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .write)
            switch type {
            case .read:
                state.readConflictRanges = Self.merging(
                    state.readConflictRanges,
                    with: range
                )
            case .write:
                state.writeConflictRanges = Self.merging(
                    state.writeConflictRanges,
                    with: range
                )
            }
        }
    }

    public func getEstimatedRangeSizeBytes(
        beginKey: Bytes,
        endKey: Bytes
    ) async throws -> Int {
        try validateOrderedRange(
            beginKey: beginKey,
            endKey: endKey,
            message: "Range size boundaries are not ordered"
        )
        let transactionView = try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .rangeRead)
            return (
                readVersion: state.observedReadVersion,
                hasPendingMutations: !state.writeBuffer.isEmpty
            )
        }
        if transactionView.hasPendingMutations {
            return try await StorageRangeMetrics.exactSize(
                getRange(
                    begin: beginKey,
                    end: endKey,
                    snapshot: true,
                    streamingMode: .wantAll
                )
            )
        }
        let response = try await performHostCall(operation: .rangeRead) { [self] in
            try await self.client.rangeSize(
                CloudflareDurableObjectRangeSizeRequest(
                    scope: self.scope,
                    begin: CloudflareDurableObjectBytes(beginKey),
                    end: CloudflareDurableObjectBytes(endKey),
                    expectedReadVersion: transactionView.readVersion
                )
            )
        }
        try acceptReadVersion(response.currentCommitVersion)
        guard response.byteCount >= 0,
              let result = Int(exactly: response.byteCount) else {
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
        beginKey: Bytes,
        endKey: Bytes,
        chunkSize: Int
    ) async throws -> [Bytes] {
        try validateOrderedRange(
            beginKey: beginKey,
            endKey: endKey,
            message: "Split point boundaries are not ordered"
        )
        guard let wireChunkSize = Int64(exactly: chunkSize),
              wireChunkSize > 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Split point chunk size must be positive"
            )
        }
        let transactionView = try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .rangeRead)
            return (
                readVersion: state.observedReadVersion,
                hasPendingMutations: !state.writeBuffer.isEmpty
            )
        }
        if transactionView.hasPendingMutations {
            return try await StorageRangeMetrics.splitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize,
                maximumPointCount: limits.maxSplitPoints,
                rows: getRange(
                    begin: beginKey,
                    end: endKey,
                    snapshot: true,
                    streamingMode: .wantAll
                )
            )
        }
        let response = try await performHostCall(operation: .rangeRead) { [self] in
            try await self.client.rangeSplitPoints(
                CloudflareDurableObjectRangeSplitPointsRequest(
                    scope: self.scope,
                    begin: CloudflareDurableObjectBytes(beginKey),
                    end: CloudflareDurableObjectBytes(endKey),
                    chunkSize: wireChunkSize,
                    expectedReadVersion: transactionView.readVersion
                )
            )
        }
        try acceptReadVersion(response.currentCommitVersion)
        guard response.splitPoints.count <= limits.maxSplitPoints else {
            throw invalidSplitPoints()
        }
        let points = response.splitPoints.map(\.rawValue)
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
                  let committedVersion = state.committedVersion else {
                throw Self.phaseError(state.phase, operation: .read)
            }
            return committedVersion
        }
    }

    public func requestVersionstamp() -> any PendingTransactionVersionstamp {
        let failure = state.withLock { state -> StorageError? in
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
        for key: Bytes,
        committed: Bytes?,
        applying writeBuffer: [CloudflareDurableObjectWriteOp]
    ) throws -> Bytes? {
        var value = committed
        for op in writeBuffer {
            switch op {
            case .set(let opKey, let opValue) where opKey == key:
                value = opValue
            case .clear(let opKey) where opKey == key:
                value = nil
            case .clearRange(let begin, let end)
                where CloudflareDurableObjectByteOrdering.compare(key, begin) >= 0
                    && CloudflareDurableObjectByteOrdering.compare(key, end) < 0:
                value = nil
            case .atomic(let opKey, let param, let mutationType) where opKey == key:
                switch try mutationType.apply(to: value, param: param) {
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

    private func validate(_ writeBuffer: [CloudflareDurableObjectWriteOp]) throws {
        guard writeBuffer.count <= limits.maxMutationsPerCommit else {
            throw StorageError(
                code: .invalidOperation,
                operation: .commit,
                backend: .cloudflareDurableObject,
                message: "Mutation batch exceeds configured limit"
            )
        }
        for op in writeBuffer {
            switch op {
            case .set(let key, let value):
                try validateKey(key)
                try validateValue(value)
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
                case .setVersionstampedValue:
                    try validateKey(key)
                    try validateBytes(
                        param,
                        maximum: limits.maxValueBytes + 4,
                        message: "Versionstamped value operand exceeds configured byte limit"
                    )
                default:
                    try validateKey(key)
                    try validateValue(param)
                }
            }
        }
    }

    private func validateKey(_ key: Bytes) throws {
        guard key.count <= limits.maxKeyBytes else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .cloudflareDurableObject,
                message: "Key exceeds configured byte limit"
            )
        }
    }

    private func validateValue(_ value: Bytes) throws {
        try validateBytes(
            value,
            maximum: limits.maxValueBytes,
            message: "Value exceeds configured byte limit"
        )
    }

    private func validateBytes(
        _ bytes: Bytes,
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

    private func validateBoundary(
        _ boundary: Bytes,
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
        beginKey: Bytes,
        endKey: Bytes,
        message: String
    ) throws {
        try validateBoundary(beginKey, operation: .rangeRead)
        try validateBoundary(endKey, operation: .rangeRead)
        guard CloudflareDurableObjectByteOrdering.compare(
            beginKey,
            endKey
        ) <= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: message
            )
        }
    }

    private func validateSplitPoints(
        _ points: [Bytes],
        beginKey: Bytes,
        endKey: Bytes
    ) throws {
        let expectedMinimumCount = beginKey == endKey ? 1 : 2
        guard points.count >= expectedMinimumCount,
              points.count <= limits.maxSplitPoints,
              points.first == beginKey,
              points.last == endKey else {
            throw invalidSplitPoints()
        }
        for point in points {
            try validateBoundary(point, operation: .rangeRead)
        }
        for index in 1..<points.count {
            guard CloudflareDurableObjectByteOrdering.compare(
                points[index - 1],
                points[index]
            ) < 0 else {
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
        guard version >= 0 else {
            throw StorageError(
                code: .backendContractViolation,
                operation: .read,
                backend: .cloudflareDurableObject,
                message: "Host returned a negative transaction read version"
            )
        }
        try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .read)
            if let observed = state.observedReadVersion {
                guard observed == version else {
                    throw StorageError(
                        code: .transactionConflict,
                        operation: .read,
                        backend: .cloudflareDurableObject,
                        message: "Transaction read version changed"
                    )
                }
            } else {
                state.observedReadVersion = version
            }
        }
    }

    private func recordReadConflictRange(
        _ range: CloudflareDurableObjectConflictRange
    ) throws {
        try validateConflictRange(range)
        let detachedRange = range.detached()
        try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .rangeRead)
            state.readConflictRanges = Self.merging(
                state.readConflictRanges,
                with: detachedRange
            )
        }
    }

    private func validateConflictRange(
        _ range: CloudflareDurableObjectConflictRange
    ) throws {
        if let begin = range.begin {
            try validateBoundary(begin.rawValue, operation: .rangeRead)
        }
        if let end = range.end {
            try validateBoundary(end.rawValue, operation: .rangeRead)
        }
        if let begin = range.begin, let end = range.end {
            guard CloudflareDurableObjectByteOrdering.compare(
                begin.rawValue,
                end.rawValue
            ) < 0 else {
                throw StorageError(
                    code: .dataCorruption,
                    operation: .rangeRead,
                    backend: .cloudflareDurableObject,
                    message: "Host returned an invalid read conflict range"
                )
            }
        }
    }

    private static func merging(
        _ ranges: [CloudflareDurableObjectConflictRange],
        with range: CloudflareDurableObjectConflictRange
    ) -> [CloudflareDurableObjectConflictRange] {
        let sorted = (ranges + [range]).sorted {
            compareBegin($0.begin, $1.begin) < 0
        }
        var merged: [CloudflareDurableObjectConflictRange] = []
        merged.reserveCapacity(sorted.count)
        for candidate in sorted {
            guard let previous = merged.last,
                  overlapsOrTouches(previous, candidate) else {
                merged.append(candidate)
                continue
            }
            merged[merged.count - 1] = CloudflareDurableObjectConflictRange(
                begin: previous.begin,
                end: maximumEnd(previous.end, candidate.end)
            )
        }
        return merged
    }

    private static func compareBegin(
        _ left: CloudflareDurableObjectBytes?,
        _ right: CloudflareDurableObjectBytes?
    ) -> Int {
        guard let left else {
            return right == nil ? 0 : -1
        }
        guard let right else {
            return 1
        }
        return CloudflareDurableObjectByteOrdering.compare(
            left.rawValue,
            right.rawValue
        )
    }

    private static func overlapsOrTouches(
        _ left: CloudflareDurableObjectConflictRange,
        _ right: CloudflareDurableObjectConflictRange
    ) -> Bool {
        guard let leftEnd = left.end, let rightBegin = right.begin else {
            return true
        }
        return CloudflareDurableObjectByteOrdering.compare(
            rightBegin.rawValue,
            leftEnd.rawValue
        ) <= 0
    }

    private static func maximumEnd(
        _ left: CloudflareDurableObjectBytes?,
        _ right: CloudflareDurableObjectBytes?
    ) -> CloudflareDurableObjectBytes? {
        guard let left, let right else {
            return nil
        }
        return CloudflareDurableObjectByteOrdering.compare(
            left.rawValue,
            right.rawValue
        ) >= 0 ? left : right
    }

    private static func ensureOpen(
        _ phase: CloudflareDurableObjectTransactionPhase,
        operation: StorageOperation
    ) throws {
        guard case .open = phase else {
            throw phaseError(phase, operation: operation)
        }
    }

    private static func phaseError(
        _ phase: CloudflareDurableObjectTransactionPhase,
        operation: StorageOperation
    ) -> StorageError {
        switch phase {
        case .open:
            return StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Transaction is open"
            )
        case .committing:
            return lifecycleError("Transaction is committing", operation: operation)
        case .committed:
            return lifecycleError("Transaction already committed", operation: operation)
        case .cancelling:
            return lifecycleError("Transaction is cancelling", operation: operation)
        case .cancelled:
            return lifecycleError("Transaction cancelled", operation: operation)
        case .failed(let error), .commitUnknown(let error):
            return error
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
            underlyingDescription: String(describing: error)
        )
    }

    private static func clearPendingState(
        _ state: inout CloudflareDurableObjectTransactionState
    ) {
        state.writeBuffer.removeAll(keepingCapacity: false)
        state.readConflictRanges.removeAll(keepingCapacity: false)
        state.writeConflictRanges.removeAll(keepingCapacity: false)
    }

    private func prepareMutation(
        state: inout CloudflareDurableObjectTransactionState
    ) throws {
        try Self.ensureOpen(state.phase, operation: .write)
        guard state.writeBuffer.count < limits.maxMutationsPerCommit else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .cloudflareDurableObject,
                message: "Mutation batch exceeds configured limit"
            )
        }
    }

    private func configureTimeout(milliseconds: Int) throws {
        guard milliseconds >= 0,
              let wireMilliseconds = Int64(exactly: milliseconds) else {
            throw invalidOptionValue(
                "Timeout milliseconds must be a nonnegative Int64 value"
            )
        }
        let deadline = wireMilliseconds == 0
            ? nil
            : monotonicClock.now.advanced(
                by: .milliseconds(wireMilliseconds)
            )
        try state.withLock { state in
            try Self.ensureOpen(state.phase, operation: .execute)
            state.deadline = deadline
        }
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
        let deadline = state.withLock { $0.deadline }
        do {
            return try await Self.mapHostError(operation: operation) {
                guard let deadline else {
                    return try await body()
                }
                try self.ensureDeadlineNotExpired(
                    deadline,
                    operation: operation
                )
                switch self.client.callExecution {
                case .synchronous:
                    let result = try await body()
                    try self.ensureDeadlineNotExpired(
                        deadline,
                        operation: operation
                    )
                    return result
                case .suspending:
                    return try await CloudflareDurableObjectTimedCall<T>().execute(
                        until: deadline,
                        clock: self.monotonicClock,
                        timeoutError: {
                            Self.timeoutError(operation: operation)
                        },
                        operation: body
                    )
                }
            }
        } catch let error as StorageError
            where error.code == .transactionTimedOut {
            let transitioned = state.withLock { state -> Bool in
                guard case .open = state.phase else { return false }
                state.phase = .failed(error)
                Self.clearPendingState(&state)
                return true
            }
            if transitioned {
                versionstampCompletion.resolveIfPending(.failure(error))
            }
            throw error
        }
    }

    private static func mapHostError<T: Sendable>(
        operation: StorageOperation,
        _ body: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        do {
            return try await body()
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as StorageError {
            throw error
        } catch {
            throw StorageError(
                code: .backendFailure,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object client operation failed",
                underlyingDescription: String(describing: error)
            )
        }
    }

    private func ensureDeadlineNotExpired(
        _ deadline: ContinuousClock.Instant?,
        operation: StorageOperation
    ) throws {
        guard let deadline else { return }
        guard monotonicClock.now < deadline else {
            throw Self.timeoutError(operation: operation)
        }
    }

    private static func timeoutError(
        operation: StorageOperation
    ) -> StorageError {
        StorageError(
            code: .transactionTimedOut,
            operation: operation,
            backend: .cloudflareDurableObject,
            message: "Cloudflare Durable Object transaction timed out"
        )
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
