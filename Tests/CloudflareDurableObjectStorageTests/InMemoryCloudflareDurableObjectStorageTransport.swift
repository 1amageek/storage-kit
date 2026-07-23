import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageEmbedded
import StorageKit
import StorageKitEmbeddedCore
import Synchronization

final class InMemoryCloudflareDurableObjectStorageTransport: CloudflareDurableObjectStorageTransport, Sendable {
    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    private struct State: Sendable {
        var rowsByScope: [
            CloudflareDurableObjectEmbeddedScope: [EmbeddedBytes: EmbeddedBytes]
        ] = [:]
        var versionsByScope: [CloudflareDurableObjectEmbeddedScope: Int64] = [:]
        var conflictsByScope: [CloudflareDurableObjectEmbeddedScope: [ConflictEntry]] = [:]
    }

    private struct ConflictEntry: Sendable {
        let version: Int64
        let begin: EmbeddedBytes
        let end: EmbeddedBytes
    }

    private let state = Mutex(State())

    func send(_ requestBytes: EmbeddedBytes) async throws -> EmbeddedBytes {
        do {
            let request = try CloudflareDurableObjectStorageWireCodec.decodeRequest(requestBytes)
            return try state.withLock { state in
                try CloudflareDurableObjectStorageWireCodec.encode(handle(request, state: &state))
            }
        } catch let error as StorageError {
            return try CloudflareDurableObjectStorageWireCodec.encode(
                .failure(status: statusCode(for: error), message: error.message)
            )
        } catch {
            return try CloudflareDurableObjectStorageWireCodec.encode(
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
                    metadataInitialized: true
                )
            )
        case .read(let request):
            try verifyReadVersion(
                request.expectedReadVersion,
                scope: request.scope,
                state: state
            )
            return .read(
                CloudflareDurableObjectEmbeddedReadResponse(
                    value: state.rowsByScope[request.scope]?[request.key],
                    currentCommitVersion: state.versionsByScope[request.scope] ?? 0
                )
            )
        case .range(let request):
            try verifyReadVersion(
                request.expectedReadVersion,
                scope: request.scope,
                state: state
            )
            let rows = try pageRows(for: request, state: state)
            return .range(
                CloudflareDurableObjectEmbeddedRangeResponse(
                    rows: rows.page,
                    hasMore: rows.hasMore,
                    currentCommitVersion: state.versionsByScope[request.scope] ?? 0,
                    readConflictRanges: [conflictRange(for: request, rows: rows.page)]
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
            let currentVersion = state.versionsByScope[request.scope] ?? 0
            let committedVersion = currentVersion + 1
            for mutation in request.mutations {
                let materializedMutation = try materialized(
                    mutation,
                    committedVersion: committedVersion
                )
                recordWriteConflict(
                    materializedMutation,
                    version: committedVersion,
                    scope: request.scope,
                    state: &state
                )
                try apply(materializedMutation, to: &rows)
            }
            for range in request.writeConflictRanges {
                recordWriteConflict(
                    range,
                    version: committedVersion,
                    scope: request.scope,
                    state: &state
                )
            }
            state.rowsByScope[request.scope] = rows
            state.versionsByScope[request.scope] = committedVersion
            return .commit(CloudflareDurableObjectEmbeddedCommitResponse(committedVersion: committedVersion))
        case .rangeSize(let request):
            try verifyReadVersion(
                request.expectedReadVersion,
                scope: request.scope,
                state: state
            )
            var total: Int64 = 0
            for (key, value) in state.rowsByScope[request.scope] ?? [:]
                where EmbeddedByteOrdering.compare(key, request.begin) >= 0
                    && EmbeddedByteOrdering.compare(key, request.end) < 0 {
                total += Int64(key.count + value.count)
            }
            return .rangeSize(
                CloudflareDurableObjectEmbeddedRangeSizeResponse(
                    byteCount: total,
                    currentCommitVersion: state.versionsByScope[request.scope] ?? 0
                )
            )
        case .rangeSplitPoints(let request):
            try verifyReadVersion(
                request.expectedReadVersion,
                scope: request.scope,
                state: state
            )
            let rows = (state.rowsByScope[request.scope] ?? [:])
                .filter {
                    EmbeddedByteOrdering.compare($0.key, request.begin) >= 0
                        && EmbeddedByteOrdering.compare($0.key, request.end) < 0
                }
                .sorted {
                    EmbeddedByteOrdering.compare($0.key, $1.key) < 0
                }
            var points = [request.begin]
            var chunkBytes: Int64 = 0
            for row in rows {
                let rowSize = Int64(row.key.count + row.value.count)
                if chunkBytes > 0,
                   rowSize > request.chunkSize - min(
                    chunkBytes,
                    request.chunkSize
                   ) {
                    points.append(row.key)
                    chunkBytes = 0
                }
                chunkBytes += rowSize
            }
            if request.begin != request.end {
                points.append(request.end)
            }
            guard points.count <= EmbeddedLimits.cloudflareDurableObject.maxSplitPoints else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    backend: .cloudflareDurableObject,
                    message: "Split point result exceeds the protocol limit"
                )
            }
            return .rangeSplitPoints(
                CloudflareDurableObjectEmbeddedRangeSplitPointsResponse(
                    splitPoints: points,
                    currentCommitVersion: state.versionsByScope[request.scope] ?? 0
                )
            )
        }
    }

    private func pageRows(
        for request: CloudflareDurableObjectEmbeddedRangeRequest,
        state: State
    ) throws -> (page: [EmbeddedKeyValue], hasMore: Bool) {
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
        var remaining = selected
        if let cursorKey = request.cursorKey {
            remaining = selected.filter {
                let ordering = EmbeddedByteOrdering.compare($0.key, cursorKey)
                return request.reverse ? ordering < 0 : ordering > 0
            }
        }
        let pageLimit = request.limit > 0 ? request.limit : selected.count
        let page = Array(remaining.prefix(pageLimit))
        return (page, page.count < remaining.count)
    }

    private func apply(
        _ mutation: EmbeddedWriteOperation,
        to rows: inout [EmbeddedBytes: EmbeddedBytes]
    ) throws {
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

    private func materialized(
        _ mutation: EmbeddedWriteOperation,
        committedVersion: Int64
    ) throws -> EmbeddedWriteOperation {
        guard case .atomic(let key, let param, let mutationType) = mutation else {
            return mutation
        }
        switch mutationType {
        case .setVersionstampedKey:
            return .set(
                key: try materializedVersionstampOperand(
                    key,
                    committedVersion: committedVersion,
                    maximumResultBytes: 1_024
                ),
                value: param
            )
        case .setVersionstampedValue:
            return .set(
                key: key,
                value: try materializedVersionstampOperand(
                    param,
                    committedVersion: committedVersion,
                    maximumResultBytes: 1_048_576
                )
            )
        default:
            return mutation
        }
    }

    private func materializedVersionstampOperand(
        _ operand: EmbeddedBytes,
        committedVersion: Int64,
        maximumResultBytes: Int
    ) throws -> EmbeddedBytes {
        guard operand.count >= 14 else {
            throw StorageError.invalidOperation(
                "Versionstamp operand is shorter than fourteen bytes"
            )
        }
        let payloadCount = operand.count - 4
        guard payloadCount <= maximumResultBytes else {
            throw StorageError.invalidOperation(
                "Materialized versionstamp exceeds the byte limit"
            )
        }
        let offset = Int(operand[payloadCount])
            | (Int(operand[payloadCount + 1]) << 8)
            | (Int(operand[payloadCount + 2]) << 16)
            | (Int(operand[payloadCount + 3]) << 24)
        guard offset <= payloadCount - 10 else {
            throw StorageError.invalidOperation(
                "Versionstamp offset does not identify ten payload bytes"
            )
        }
        var result = operand.slice(0..<payloadCount).contiguousArray()
        result.replaceSubrange(
            offset..<(offset + 10),
            with: versionstamp(for: committedVersion)
        )
        return EmbeddedBytes(result)
    }

    private func versionstamp(
        for committedVersion: Int64
    ) -> EmbeddedBytes {
        let version = UInt64(committedVersion)
        return [
            UInt8(truncatingIfNeeded: version >> 56),
            UInt8(truncatingIfNeeded: version >> 48),
            UInt8(truncatingIfNeeded: version >> 40),
            UInt8(truncatingIfNeeded: version >> 32),
            UInt8(truncatingIfNeeded: version >> 24),
            UInt8(truncatingIfNeeded: version >> 16),
            UInt8(truncatingIfNeeded: version >> 8),
            UInt8(truncatingIfNeeded: version),
            0,
            0,
        ]
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

    private func recordWriteConflict(
        _ range: EmbeddedKeyRange,
        version: Int64,
        scope: CloudflareDurableObjectEmbeddedScope,
        state: inout State
    ) {
        guard let begin = range.begin,
              let end = range.end,
              EmbeddedByteOrdering.compare(begin, end) < 0 else {
            return
        }
        state.conflictsByScope[scope, default: []].append(
            ConflictEntry(version: version, begin: begin, end: end)
        )
    }

    private func writeConflictRange(
        for mutation: EmbeddedWriteOperation
    ) -> (begin: EmbeddedBytes, end: EmbeddedBytes)? {
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

    private func singleKeyRange(
        _ key: EmbeddedBytes
    ) -> (begin: EmbeddedBytes, end: EmbeddedBytes) {
        (key, key.appending(0x00))
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

    private func conflictRange(
        for request: CloudflareDurableObjectEmbeddedRangeRequest,
        rows: [EmbeddedKeyValue]
    ) -> EmbeddedKeyRange {
        let requestedBegin = boundaryKey(request.begin)
        let requestedEnd = boundaryKey(request.end)
        if let requestedBegin,
           let requestedEnd,
           EmbeddedByteOrdering.compare(requestedBegin, requestedEnd) < 0 {
            return EmbeddedKeyRange(
                begin: requestedBegin,
                end: requestedEnd
            )
        }

        let orderedKeys = rows.map(\.key).sorted {
            EmbeddedByteOrdering.compare($0, $1) < 0
        }
        let begin = minimumKey(requestedBegin, orderedKeys.first)
        let end = maximumKey(
            requestedEnd.map(keySuccessor),
            orderedKeys.last.map(keySuccessor)
        )
        return EmbeddedKeyRange(begin: begin, end: end)
    }

    private func boundaryKey(
        _ boundary: EmbeddedRangeBoundary
    ) -> EmbeddedBytes? {
        switch boundary {
        case .unbounded:
            return nil
        case .selector(let selector):
            return selector.key
        }
    }

    private func minimumKey(
        _ left: EmbeddedBytes?,
        _ right: EmbeddedBytes?
    ) -> EmbeddedBytes? {
        guard let left else { return right }
        guard let right else { return left }
        return EmbeddedByteOrdering.compare(left, right) <= 0 ? left : right
    }

    private func maximumKey(
        _ left: EmbeddedBytes?,
        _ right: EmbeddedBytes?
    ) -> EmbeddedBytes? {
        guard let left else { return right }
        guard let right else { return left }
        return EmbeddedByteOrdering.compare(left, right) >= 0 ? left : right
    }

    private func keySuccessor(_ key: EmbeddedBytes) -> EmbeddedBytes {
        key.appending(0x00)
    }

    private func statusCode(
        for error: StorageError
    ) -> CloudflareDurableObjectEmbeddedFailureStatus {
        switch error.code {
        case .transactionConflict:
            return .transactionConflict
        case .invalidOperation:
            return .invalidOperation
        case .resourceUnavailable:
            return .resourceUnavailable
        case .backendContractViolation:
            return .backendContractViolation
        default:
            return .backendFailure
        }
    }
}
