import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageEmbedded
import StorageKit
import StorageKitEmbeddedCore
import Synchronization

final class FakeCloudflareDurableObjectBinaryTransport: CloudflareDurableObjectBinaryTransport, Sendable {
    private struct State: Sendable {
        var rowsByScope: [CloudflareDurableObjectEmbeddedScope: [Bytes: Bytes]] = [:]
        var versionsByScope: [CloudflareDurableObjectEmbeddedScope: Int64] = [:]
        var conflictsByScope: [CloudflareDurableObjectEmbeddedScope: [ConflictEntry]] = [:]
    }

    private struct ConflictEntry: Sendable {
        let version: Int64
        let begin: Bytes
        let end: Bytes
    }

    private let state = Mutex(State())

    func send(_ requestBytes: [UInt8]) async throws -> [UInt8] {
        do {
            let request = try CloudflareDurableObjectEmbeddedRuntime.decodeRequest(requestBytes)
            return try state.withLock { state in
                try CloudflareDurableObjectEmbeddedRuntime.encode(handle(request, state: &state))
            }
        } catch let error as StorageError {
            return try CloudflareDurableObjectEmbeddedRuntime.encode(
                .failure(status: statusCode(for: error), message: error.message)
            )
        } catch {
            return try CloudflareDurableObjectEmbeddedRuntime.encode(
                .failure(status: .invalidOperation, message: String(describing: error))
            )
        }
    }

    private func handle(
        _ request: CloudflareDurableObjectEmbeddedRequest,
        state: inout State
    ) throws -> CloudflareDurableObjectEmbeddedResponse {
        switch request {
        case .readiness(let request):
            return .readiness(
                CloudflareDurableObjectEmbeddedReadinessResponse(
                    schemaVersion: 1,
                    commitVersion: state.versionsByScope[request.scope] ?? 0,
                    metadataInitialized: state.rowsByScope[request.scope] != nil
                )
            )
        case .read(let request):
            if !request.snapshot {
                try verifyReadVersion(request.expectedReadVersion, scope: request.scope, state: state)
            }
            return .read(
                CloudflareDurableObjectEmbeddedReadResponse(
                    value: state.rowsByScope[request.scope]?[request.key],
                    currentCommitVersion: state.versionsByScope[request.scope] ?? 0
                )
            )
        case .range(let request):
            if !request.snapshot {
                try verifyReadVersion(request.expectedReadVersion, scope: request.scope, state: state)
            }
            let rows = try pageRows(for: request, state: state)
            return .range(
                CloudflareDurableObjectEmbeddedRangeResponse(
                    rows: rows.page,
                    nextCursor: rows.nextCursor,
                    currentCommitVersion: state.versionsByScope[request.scope] ?? 0,
                    conflictRange: conflictRange(for: request)
                )
            )
        case .commit(let request):
            try verifyReadConflicts(
                readVersion: request.observedReadVersion,
                readConflictRanges: request.readConflictRanges,
                scope: request.scope,
                state: state
            )
            var rows = state.rowsByScope[request.scope] ?? [:]
            let committedVersion = (state.versionsByScope[request.scope] ?? 0) + 1
            for mutation in request.mutations {
                recordWriteConflict(mutation, version: committedVersion, scope: request.scope, state: &state)
                try apply(mutation, to: &rows)
            }
            state.rowsByScope[request.scope] = rows
            state.versionsByScope[request.scope] = committedVersion
            return .commit(CloudflareDurableObjectEmbeddedCommitResponse(committedVersion: committedVersion))
        }
    }

    private func pageRows(
        for request: CloudflareDurableObjectEmbeddedRangeRequest,
        state: State
    ) throws -> (page: [EmbeddedKeyValue], nextCursor: String?) {
        let committedRows = (state.rowsByScope[request.scope] ?? [:]).map {
            EmbeddedKeyValue(key: $0.key, value: $0.value)
        }
        let selected = try EmbeddedRangeOverlay.overlay(
            committedRows: committedRows,
            writes: [],
            begin: request.begin,
            end: request.end,
            reverse: request.reverse,
            limit: 0
        )
        let offset = try decodedCursor(request.cursor)
        let pageLimit = request.limit > 0 ? request.limit : selected.count
        let page = Array(selected.dropFirst(offset).prefix(pageLimit))
        let nextOffset = offset + page.count
        let nextCursor = nextOffset < selected.count ? String(nextOffset) : nil
        return (page, nextCursor)
    }

    private func apply(_ mutation: EmbeddedWriteOperation, to rows: inout [Bytes: Bytes]) throws {
        switch mutation {
        case .set(let key, let value):
            rows[key] = value
        case .clear(let key):
            rows.removeValue(forKey: key)
        case .clearRange(let begin, let end):
            for key in Array(rows.keys)
                where EmbeddedByteOrdering.compare(key, begin) >= 0
                    && EmbeddedByteOrdering.compare(key, end) < 0 {
                rows.removeValue(forKey: key)
            }
        case .atomic(let key, let param, let mutationType):
            switch try mutationType.apply(to: rows[key], param: param) {
            case .set(let value):
                rows[key] = value
            case .clear:
                rows.removeValue(forKey: key)
            case .unchanged:
                break
            }
        }
    }

    private func verifyReadVersion(
        _ expectedReadVersion: Int64?,
        scope: CloudflareDurableObjectEmbeddedScope,
        state: State
    ) throws {
        guard let expectedReadVersion else { return }
        let currentVersion = state.versionsByScope[scope] ?? 0
        guard currentVersion == expectedReadVersion else {
            throw StorageError(
                code: .transactionConflict,
                operation: .commit,
                backend: .cloudflareDurableObject,
                message: "Observed read version does not match current committed version"
            )
        }
    }

    private func verifyReadConflicts(
        readVersion: Int64?,
        readConflictRanges: [EmbeddedKeyRange],
        scope: CloudflareDurableObjectEmbeddedScope,
        state: State
    ) throws {
        guard let readVersion else { return }
        let conflicts = state.conflictsByScope[scope] ?? []
        for readRange in readConflictRanges {
            for conflict in conflicts where conflict.version > readVersion && overlaps(conflict, readRange) {
                throw StorageError(
                    code: .transactionConflict,
                    operation: .commit,
                    backend: .cloudflareDurableObject,
                    message: "Read conflict range was modified after the transaction read version"
                )
            }
        }
    }

    private func recordWriteConflict(
        _ mutation: EmbeddedWriteOperation,
        version: Int64,
        scope: CloudflareDurableObjectEmbeddedScope,
        state: inout State
    ) {
        guard let range = writeConflictRange(for: mutation) else {
            return
        }
        state.conflictsByScope[scope, default: []].append(
            ConflictEntry(version: version, begin: range.begin, end: range.end)
        )
    }

    private func writeConflictRange(for mutation: EmbeddedWriteOperation) -> (begin: Bytes, end: Bytes)? {
        switch mutation {
        case .set(let key, _), .clear(let key), .atomic(let key, _, _):
            return singleKeyRange(key)
        case .clearRange(let begin, let end):
            guard EmbeddedByteOrdering.compare(begin, end) < 0 else {
                return nil
            }
            return (begin, end)
        }
    }

    private func singleKeyRange(_ key: Bytes) -> (begin: Bytes, end: Bytes) {
        (key, key + [0x00])
    }

    private func overlaps(_ conflict: ConflictEntry, _ readRange: EmbeddedKeyRange) -> Bool {
        if let readEnd = readRange.end, EmbeddedByteOrdering.compare(conflict.begin, readEnd) >= 0 {
            return false
        }
        if let readBegin = readRange.begin, EmbeddedByteOrdering.compare(conflict.end, readBegin) <= 0 {
            return false
        }
        return true
    }

    private func conflictRange(for request: CloudflareDurableObjectEmbeddedRangeRequest) -> EmbeddedKeyRange {
        EmbeddedKeyRange(begin: request.begin.key, end: request.end.key)
    }

    private func decodedCursor(_ cursor: String?) throws -> Int {
        guard let cursor else { return 0 }
        guard let offset = Int(cursor), offset >= 0 else {
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                backend: .cloudflareDurableObject,
                message: "Invalid range cursor"
            )
        }
        return offset
    }

    private func statusCode(for error: StorageError) -> CloudflareDurableObjectEmbeddedStatusCode {
        switch error.code {
        case .transactionConflict:
            return .transactionConflict
        case .invalidOperation:
            return .invalidOperation
        case .resourceUnavailable:
            return .resourceUnavailable
        default:
            return .backendFailure
        }
    }
}
