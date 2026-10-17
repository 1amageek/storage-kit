import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit

/// Raw snapshot access retains host state without retaining its transaction facade.
struct CloudflareDurableObjectTransactionReader: TransactionJournalBackendAccess {
    let partitionIdentity: StoragePartitionIdentity
    let client: any CloudflareDurableObjectStorageClient
    let limits: CloudflareDurableObjectLimits
    let monotonicClock: any StorageMonotonicClock
    let state: CloudflareDurableObjectTransactionStateOwner
    let versionstampCompletion: TransactionVersionstampCompletion

    func validateAccess(operation: StorageOperation) throws {
        let deadline = try state.withLock { state in
            try CloudflareDurableObjectStorageTransaction.ensureHostAccess(
                state.phase, allowingBarrier: state.checkpointEnabled, operation: operation)
            return state.deadline
        }
        do {
            try ensureDeadlineNotExpired(deadline, operation: operation)
        } catch let error as StorageError {
            failTimeout(error)
            throw error
        }
    }

    private func failTimeout(_ error: StorageError) {
        let transitioned = state.withLock { state -> Bool in
            switch state.phase {
            case .open, .sealing, .cancelling: break
            default: return false
            }
            state.phase = .failed(error)
            CloudflareDurableObjectStorageTransaction.clearPendingState(&state)
            return true
        }
        if transitioned { versionstampCompletion.fail(error) }
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        let readVersion = try state.withLock { state in
            try CloudflareDurableObjectStorageTransaction.ensureHostAccess(state.phase, allowingBarrier: state.checkpointEnabled, operation: .read)
            return state.observedReadVersion
        }
        let response = try await performHostCall(operation: .read) {
            try await client.read(StorageWireReadRequest(
                partitionIdentity: partitionIdentity,
                key: key,
                snapshot: snapshot,
                expectedReadVersion: readVersion
            ))
        }
        if let value = response.value { try validateStoredRow(key: key, value: value) }
        try acceptReadVersion(response.currentCommitVersion)
        if !snapshot { try recordReadConflictRange(.singleKey(key)) }
        if let value = response.value, value.count > maximumByteCount {
            throw StorageError.pointReadValueTooLarge(
                observedByteCount: value.count,
                maximumByteCount: maximumByteCount,
                backend: .cloudflareDurableObject
            )
        }
        return response.value
    }

    func rangeCursor(
        in range: TransactionJournalRange,
        reverse: Bool,
        snapshot: Bool,
        pageSize: Int,
        maximumByteCount: Int
    ) -> KeyValueCursor {
        let bounds = (
            begin: wireBoundary(range.lower, lower: true),
            end: wireBoundary(range.upper, lower: false)
        )
        let result = CloudflareDurableObjectRangeResult {
            CloudflareDurableObjectRangeScan(
                read: { request in
                    try await performHostCall(operation: .read) {
                        try await client.read(request)
                    }
                },
                range: { request in
                    let response = try await performHostCall(operation: .rangeRead) {
                        try await client.range(request)
                    }
                    guard response.rows.count <= request.limit else {
                        throw StorageError(code: .backendContractViolation, operation: .rangeRead,
                            backend: .cloudflareDurableObject,
                            message: "Host returned more rows than the requested page limit")
                    }
                    return response
                },
                partitionIdentity: partitionIdentity,
                begin: .firstGreaterOrEqual(ByteString()),
                end: .firstGreaterOrEqual(ByteString()),
                snapshot: snapshot,
                initialExpectedReadVersion: state.withLock { $0.observedReadVersion },
                pageLimit: min(pageSize, limits.maxRangeLimit),
                maxSelectorResolutionSteps: limits.maxSelectorResolutionSteps,
                userLimit: 0,
                reverse: reverse,
                mutations: [],
                ensureOpen: {
                    try state.withLock {
                        try CloudflareDurableObjectStorageTransaction.ensureHostAccess($0.phase, allowingBarrier: $0.checkpointEnabled, operation: .rangeRead)
                    }
                },
                recordReadVersion: { try acceptReadVersion($0) },
                recordReadConflictRange: { try recordReadConflictRange($0) },
                validateStoredRow: { try validateStoredRow(key: $0, value: $1) },
                rawBounds: bounds,
                maximumTransferByteCount: maximumByteCount
            )
        }
        return KeyValueCursor(consuming: result)
    }

    private func wireBoundary(
        _ boundary: TransactionJournalBoundary,
        lower: Bool
    ) -> StorageWireRangeBoundary {
        switch boundary {
        case .unbounded: return .unbounded
        case .inclusive(let key):
            return .selector(StorageWireKeySelector(lower ? .firstGreaterOrEqual(key) : .firstGreaterThan(key)))
        case .exclusive(let key):
            return .selector(StorageWireKeySelector(lower ? .firstGreaterThan(key) : .firstGreaterOrEqual(key)))
        }
    }

    private func validateStoredRow(key: ByteString, value: ByteString) throws {
        let total = key.count.addingReportingOverflow(value.count)
        guard key.count <= limits.maxKeyBytes, value.count <= limits.maxValueBytes,
              !total.overflow, total.partialValue <= limits.maxStoredKeyValueBytes else {
            throw StorageError(code: .invalidOperation, operation: .read,
                backend: .cloudflareDurableObject,
                message: "Host row exceeds configured key/value byte limits")
        }
    }

    private func validateBoundary(_ boundary: ByteString) throws {
        guard boundary.count <= limits.maxBoundaryBytes else {
            throw StorageError(code: .invalidOperation, operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Conflict boundary exceeds configured byte limit")
        }
    }

    func acceptReadVersion(_ version: Int64) throws {
        guard version >= 0 else {
            throw StorageError(
                code: .backendContractViolation,
                operation: .read,
                backend: .cloudflareDurableObject,
                message: "Host returned a negative transaction read version"
            )
        }
        try state.withLock { state in
            try CloudflareDurableObjectStorageTransaction.ensureHostAccess(state.phase, allowingBarrier: state.checkpointEnabled, operation: .read)
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

    func recordReadConflictRange(
        _ range: StorageWireKeyRange
    ) throws {
        try validateConflictRange(range)
        // Conflict metadata outlives the decoded response. Copy only its small
        // boundaries so the transaction does not retain an entire wire frame.
        let detachedRange = range.detached()
        try state.withLock { state in
            try CloudflareDurableObjectStorageTransaction.ensureHostAccess(state.phase, allowingBarrier: state.checkpointEnabled, operation: .rangeRead)
            state.readConflictRanges = CloudflareDurableObjectStorageTransaction.merging(
                state.readConflictRanges,
                with: detachedRange
            )
        }
    }

    private func validateConflictRange(
        _ range: StorageWireKeyRange
    ) throws {
        if let begin = range.begin {
            try validateBoundary(begin)
        }
        if let end = range.end {
            try validateBoundary(end)
        }
        if let begin = range.begin, let end = range.end {
            guard
                CloudflareDurableObjectByteOrdering.compare(
                    begin,
                    end
                ) < 0
            else {
                throw StorageError(
                    code: .dataCorruption,
                    operation: .rangeRead,
                    backend: .cloudflareDurableObject,
                    message: "Host returned an invalid read conflict range"
                )
            }
        }
    }

    func performHostCall<T: Sendable>(
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
            where error.code == .transactionTimedOut
        {
            failTimeout(error)
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
                underlyingDescription: underlyingStorageErrorDescription(error)
            )
        }
    }

    private func ensureDeadlineNotExpired(
        _ deadline: StorageInstant?,
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

}
