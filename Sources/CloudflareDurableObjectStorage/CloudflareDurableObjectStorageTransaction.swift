import StorageKit
import Synchronization

/// Transaction facade for one Cloudflare Durable Object scope.
public final class CloudflareDurableObjectStorageTransaction: Transaction, Sendable {
    public typealias RangeResult = CloudflareDurableObjectRangeResult

    private let scope: CloudflareDurableObjectStorageScope
    private let client: any CloudflareDurableObjectStorageClient
    private let limits: CloudflareDurableObjectLimits
    private let state = Mutex(CloudflareDurableObjectTransactionState())

    init(
        scope: CloudflareDurableObjectStorageScope,
        client: any CloudflareDurableObjectStorageClient,
        limits: CloudflareDurableObjectLimits
    ) {
        self.scope = scope
        self.client = client
        self.limits = limits
    }

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        try validateKey(key)
        let (phase, writeBuffer, observedReadVersion) = state.withLock {
            ($0.phase, $0.writeBuffer, $0.observedReadVersion)
        }
        try Self.ensureOpen(phase, operation: .read)

        let response = try await mapHostError(operation: .read) {
            try await client.read(
                CloudflareDurableObjectReadRequest(
                    scope: scope,
                    key: CloudflareDurableObjectBytes(key),
                    snapshot: snapshot,
                    expectedReadVersion: snapshot ? nil : observedReadVersion
                )
            )
        }
        if !snapshot {
            recordReadVersion(response.currentCommitVersion)
            recordReadConflictRange(.singleKey(CloudflareDurableObjectBytes(key)))
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
                client: client,
                scope: scope,
                begin: begin,
                end: end,
                snapshot: snapshot,
                initialExpectedReadVersion: snapshot ? nil : observedReadVersion,
                pageLimit: limits.maxRangeLimit,
                userLimit: limit,
                reverse: reverse,
                writeBuffer: writeBuffer,
                ensureOpen: { [self] in
                    let currentPhase = state.withLock { $0.phase }
                    try Self.ensureOpen(currentPhase, operation: .rangeRead)
                },
                recordReadVersion: { [self] version in
                    recordReadVersion(version)
                },
                recordReadConflictRange: { [self] range in
                    recordReadConflictRange(range)
                }
            )
        }
    }

    public func setValue(_ value: Bytes, for key: Bytes) {
        state.withLock { state in
            guard state.phase == .open else { return }
            state.writeBuffer.append(.set(key: key, value: value))
        }
    }

    public func clear(key: Bytes) {
        state.withLock { state in
            guard state.phase == .open else { return }
            state.writeBuffer.append(.clear(key: key))
        }
    }

    public func clearRange(beginKey: Bytes, endKey: Bytes) {
        state.withLock { state in
            guard state.phase == .open else { return }
            state.writeBuffer.append(.clearRange(begin: beginKey, end: endKey))
        }
    }

    public func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) {
        state.withLock { state in
            guard state.phase == .open else { return }
            state.writeBuffer.append(.atomic(key: key, param: param, mutationType: mutationType))
        }
    }

    public func commit() async throws {
        let payload = try state.withLock {
            state -> (
                observedReadVersion: Int64?,
                writeBuffer: [CloudflareDurableObjectWriteOp],
                readConflictRanges: [CloudflareDurableObjectConflictRange]
            )? in
            switch state.phase {
            case .open:
                try validate(state.writeBuffer)
                guard !state.writeBuffer.isEmpty else {
                    state.phase = .committed
                    state.committedVersion = state.observedReadVersion ?? 0
                    return nil
                }
                let payload = (state.observedReadVersion, state.writeBuffer, state.readConflictRanges)
                state.phase = .committing
                state.writeBuffer.removeAll()
                return payload
            case .committed:
                return nil
            case .committing:
                throw Self.phaseError(.committing, operation: .commit)
            case .commitUnknown:
                throw Self.phaseError(.commitUnknown, operation: .commit)
            case .cancelled:
                throw Self.phaseError(.cancelled, operation: .commit)
            }
        }
        guard let payload else {
            return
        }

        do {
            let response = try await mapHostError(operation: .commit) {
                try await client.commit(
                    CloudflareDurableObjectCommitRequest(
                        scope: scope,
                        observedReadVersion: payload.observedReadVersion,
                        mutations: payload.writeBuffer.map(\.mutation),
                        readConflictRanges: payload.readConflictRanges
                    )
                )
            }

            state.withLock { state in
                state.phase = .committed
                state.committedVersion = response.committedVersion
            }
        } catch {
            state.withLock { state in
                if let storageError = error as? StorageError, storageError.code == .commitUnknownResult {
                    state.phase = .commitUnknown
                } else {
                    state.phase = .cancelled
                }
                state.writeBuffer.removeAll()
                state.readConflictRanges.removeAll()
            }
            throw error
        }
    }

    public func cancel() {
        state.withLock { state in
            state.phase = .cancelled
            state.writeBuffer.removeAll()
            state.readConflictRanges.removeAll()
        }
    }

    public func getReadVersion() async throws -> Int64 {
        let (phase, readVersion) = state.withLock { ($0.phase, $0.observedReadVersion) }
        try Self.ensureOpen(phase, operation: .read)
        if let readVersion {
            return readVersion
        }
        let response = try await mapHostError(operation: .read) {
            try await client.readiness(CloudflareDurableObjectReadinessRequest(scope: scope))
        }
        recordReadVersion(response.commitVersion)
        return response.commitVersion
    }

    public func getCommittedVersion() throws -> Int64 {
        state.withLock { $0.committedVersion ?? 0 }
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
                try validateKey(begin)
                try validateKey(end)
            case .atomic(let key, let param, _):
                try validateKey(key)
                try validateValue(param)
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
        guard value.count <= limits.maxValueBytes else {
            throw StorageError(
                code: .invalidOperation,
                operation: .write,
                backend: .cloudflareDurableObject,
                message: "Value exceeds configured byte limit"
            )
        }
    }

    private func recordReadVersion(_ version: Int64) {
        state.withLock { state in
            guard state.phase == .open else { return }
            state.observedReadVersion = max(state.observedReadVersion ?? version, version)
        }
    }

    private func recordReadConflictRange(_ range: CloudflareDurableObjectConflictRange) {
        state.withLock { state in
            guard state.phase == .open else { return }
            guard !state.readConflictRanges.contains(range) else { return }
            state.readConflictRanges.append(range)
        }
    }

    private static func ensureOpen(
        _ phase: CloudflareDurableObjectTransactionPhase,
        operation: StorageOperation
    ) throws {
        guard phase == .open else {
            throw phaseError(phase, operation: operation)
        }
    }

    private static func phaseError(
        _ phase: CloudflareDurableObjectTransactionPhase,
        operation: StorageOperation
    ) -> StorageError {
        let message: String
        switch phase {
        case .open:
            message = "Transaction is open"
        case .committing:
            message = "Transaction is committing"
        case .committed:
            message = "Transaction already committed"
        case .commitUnknown:
            message = "Transaction commit result is unknown"
        case .cancelled:
            message = "Transaction cancelled"
        }
        return StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: .cloudflareDurableObject,
            message: message
        )
    }

    private func mapHostError<T>(
        operation: StorageOperation,
        _ body: () async throws -> T
    ) async throws -> T {
        try await Self.mapHostError(operation: operation, body)
    }

    private static func mapHostError<T>(
        operation: StorageOperation,
        _ body: () async throws -> T
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

}
