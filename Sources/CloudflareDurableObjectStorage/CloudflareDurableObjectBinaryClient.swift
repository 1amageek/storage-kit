import CloudflareDurableObjectStorageEmbedded
import StorageKit
import StorageKitEmbeddedCore

/// Typed StorageKit client backed by the fixed Cloudflare Durable Object binary protocol.
public struct CloudflareDurableObjectBinaryClient: CloudflareDurableObjectStorageClient {
    public let transport: any CloudflareDurableObjectBinaryTransport

    public init(transport: any CloudflareDurableObjectBinaryTransport) {
        self.transport = transport
    }

    public func read(_ request: CloudflareDurableObjectReadRequest) async throws -> CloudflareDurableObjectReadResponse {
        let response = try await send(
            .read(
                CloudflareDurableObjectEmbeddedReadRequest(
                    scope: try embeddedScope(request.scope, operation: .read),
                    key: request.key.rawValue,
                    snapshot: request.snapshot,
                    expectedReadVersion: request.expectedReadVersion
                )
            ),
            operation: .read
        )
        guard case .read(let readResponse) = response else {
            throw unexpectedResponse(operation: .read)
        }
        return CloudflareDurableObjectReadResponse(
            value: readResponse.value.map(CloudflareDurableObjectBytes.init),
            currentCommitVersion: readResponse.currentCommitVersion
        )
    }

    public func range(_ request: CloudflareDurableObjectRangeRequest) async throws -> CloudflareDurableObjectRangeResponse {
        let response = try await send(
            .range(
                CloudflareDurableObjectEmbeddedRangeRequest(
                    scope: try embeddedScope(request.scope, operation: .rangeRead),
                    begin: try embeddedSelector(request.begin.storageKitSelector, operation: .rangeRead),
                    end: try embeddedSelector(request.end.storageKitSelector, operation: .rangeRead),
                    limit: request.limit,
                    reverse: request.reverse,
                    snapshot: request.snapshot,
                    expectedReadVersion: request.expectedReadVersion,
                    cursor: request.cursor
                )
            ),
            operation: .rangeRead
        )
        guard case .range(let rangeResponse) = response else {
            throw unexpectedResponse(operation: .rangeRead)
        }
        return CloudflareDurableObjectRangeResponse(
            rows: rangeResponse.rows.map {
                CloudflareDurableObjectKeyValue(
                    key: CloudflareDurableObjectBytes($0.key),
                    value: CloudflareDurableObjectBytes($0.value)
                )
            },
            nextCursor: rangeResponse.nextCursor,
            currentCommitVersion: rangeResponse.currentCommitVersion,
            conflictRange: rangeResponse.conflictRange.map(regularConflictRange)
        )
    }

    public func commit(_ request: CloudflareDurableObjectCommitRequest) async throws -> CloudflareDurableObjectCommitResponse {
        let response = try await send(
            .commit(
                CloudflareDurableObjectEmbeddedCommitRequest(
                    scope: try embeddedScope(request.scope, operation: .commit),
                    observedReadVersion: request.observedReadVersion,
                    mutations: try request.mutations.map { try embeddedMutation($0, operation: .commit) },
                    readConflictRanges: request.readConflictRanges.map(embeddedConflictRange)
                )
            ),
            operation: .commit
        )
        guard case .commit(let commitResponse) = response else {
            throw unexpectedResponse(operation: .commit)
        }
        return CloudflareDurableObjectCommitResponse(committedVersion: commitResponse.committedVersion)
    }

    public func readiness(
        _ request: CloudflareDurableObjectReadinessRequest
    ) async throws -> CloudflareDurableObjectReadinessResponse {
        let response = try await send(
            .readiness(
                CloudflareDurableObjectEmbeddedReadinessRequest(
                    scope: try embeddedScope(request.scope, operation: .initialize)
                )
            ),
            operation: .initialize
        )
        guard case .readiness(let readinessResponse) = response else {
            throw unexpectedResponse(operation: .initialize)
        }
        return CloudflareDurableObjectReadinessResponse(
            schemaVersion: Int(readinessResponse.schemaVersion),
            commitVersion: readinessResponse.commitVersion,
            metadataInitialized: readinessResponse.metadataInitialized
        )
    }

    private func send(
        _ request: CloudflareDurableObjectEmbeddedRequest,
        operation: StorageOperation
    ) async throws -> CloudflareDurableObjectEmbeddedResponse {
        let requestBytes: [UInt8]
        do {
            requestBytes = try CloudflareDurableObjectEmbeddedRuntime.encode(request)
        } catch {
            throw StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object binary protocol failed",
                underlyingDescription: String(describing: error)
            )
        }

        let responseBytes: [UInt8]
        do {
            responseBytes = try await transport.send(requestBytes)
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as StorageError {
            throw transportError(error, operation: operation)
        } catch {
            throw transportError(
                StorageError(
                    code: .connectionFailure,
                    operation: operation,
                    backend: .cloudflareDurableObject,
                    message: "Cloudflare Durable Object binary transport failed",
                    underlyingDescription: String(describing: error)
                ),
                operation: operation
            )
        }

        do {
            let response = try CloudflareDurableObjectEmbeddedRuntime.decodeResponse(responseBytes)
            if case .failure(let status, let message) = response {
                throw storageError(status: status, message: message, operation: operation)
            }
            return response
        } catch let error as StorageError {
            throw error
        } catch let error as CloudflareDurableObjectEmbeddedError {
            throw responseDecodeError(
                storageError(from: error, operation: operation, code: .dataCorruption),
                operation: operation
            )
        } catch {
            throw responseDecodeError(
                StorageError(
                    code: .dataCorruption,
                    operation: operation,
                    backend: .cloudflareDurableObject,
                    message: "Cloudflare Durable Object binary protocol failed",
                    underlyingDescription: String(describing: error)
                ),
                operation: operation
            )
        }
    }

    private func embeddedScope(
        _ scope: CloudflareDurableObjectStorageScope,
        operation: StorageOperation
    ) throws -> CloudflareDurableObjectEmbeddedScope {
        do {
            return try CloudflareDurableObjectEmbeddedScope(
                databaseID: scope.databaseID,
                tenantID: scope.tenantID,
                workspaceID: scope.workspaceID
            )
        } catch {
            throw storageError(from: error, operation: operation, code: .invalidOperation)
        }
    }

    private func embeddedSelector(
        _ selector: KeySelector,
        operation: StorageOperation
    ) throws -> EmbeddedKeySelector {
        switch (selector.orEqual, selector.offset) {
        case (false, 1):
            return EmbeddedKeySelector(key: selector.key, kind: .firstGreaterOrEqual)
        case (true, 1):
            return EmbeddedKeySelector(key: selector.key, kind: .firstGreaterThan)
        case (true, 0):
            return EmbeddedKeySelector(key: selector.key, kind: .lastLessOrEqual)
        case (false, 0):
            return EmbeddedKeySelector(key: selector.key, kind: .lastLessThan)
        default:
            throw StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Unsupported KeySelector(orEqual: \(selector.orEqual), offset: \(selector.offset))"
            )
        }
    }

    private func embeddedMutation(
        _ mutation: CloudflareDurableObjectMutation,
        operation: StorageOperation
    ) throws -> EmbeddedWriteOperation {
        switch mutation {
        case .set(let key, let value):
            return .set(key: key.rawValue, value: value.rawValue)
        case .clear(let key):
            return .clear(key: key.rawValue)
        case .clearRange(let begin, let end):
            return .clearRange(begin: begin.rawValue, end: end.rawValue)
        case .atomic(let key, let param, let mutationType):
            return .atomic(
                key: key.rawValue,
                param: param.rawValue,
                mutationType: try embeddedMutationType(mutationType, operation: operation)
            )
        }
    }

    private func embeddedMutationType(
        _ mutationType: CloudflareDurableObjectMutationTypeCode,
        operation: StorageOperation
    ) throws -> EmbeddedMutationType {
        switch mutationType {
        case .add:
            return .add
        case .bitOr:
            return .bitOr
        case .bitAnd:
            return .bitAnd
        case .bitXor:
            return .bitXor
        case .max:
            return .max
        case .min:
            return .min
        case .compareAndClear:
            return .compareAndClear
        case .setVersionstampedKey:
            return .setVersionstampedKey
        case .setVersionstampedValue:
            return .setVersionstampedValue
        }
    }

    private func embeddedConflictRange(
        _ range: CloudflareDurableObjectConflictRange
    ) -> EmbeddedKeyRange {
        EmbeddedKeyRange(
            begin: range.begin?.rawValue,
            end: range.end?.rawValue
        )
    }

    private func regularConflictRange(
        _ range: EmbeddedKeyRange
    ) -> CloudflareDurableObjectConflictRange {
        CloudflareDurableObjectConflictRange(
            begin: range.begin.map(CloudflareDurableObjectBytes.init),
            end: range.end.map(CloudflareDurableObjectBytes.init)
        )
    }

    private func transportError(
        _ error: StorageError,
        operation: StorageOperation
    ) -> StorageError {
        guard operation == .commit, error.code == .connectionFailure else {
            return error
        }
        return StorageError(
            code: .commitUnknownResult,
            operation: .commit,
            backend: .cloudflareDurableObject,
            message: "Cloudflare Durable Object commit result is unknown",
            underlyingDescription: error.description
        )
    }

    private func responseDecodeError(
        _ error: StorageError,
        operation: StorageOperation
    ) -> StorageError {
        guard operation == .commit else {
            return error
        }
        return StorageError(
            code: .commitUnknownResult,
            operation: .commit,
            backend: .cloudflareDurableObject,
            message: "Cloudflare Durable Object commit response could not be decoded",
            underlyingDescription: error.description
        )
    }

    private func storageError(
        status: CloudflareDurableObjectEmbeddedStatusCode,
        message: String,
        operation: StorageOperation
    ) -> StorageError {
        let code: StorageError.Code
        switch status {
        case .ok:
            code = .backendFailure
        case .transactionConflict:
            code = .transactionConflict
        case .invalidOperation:
            code = .invalidOperation
        case .backendFailure:
            code = .backendFailure
        case .resourceUnavailable:
            code = .resourceUnavailable
        }
        return StorageError(
            code: code,
            operation: operation,
            backend: .cloudflareDurableObject,
            message: message
        )
    }

    private func storageError(
        from error: CloudflareDurableObjectEmbeddedError,
        operation: StorageOperation,
        code: StorageError.Code
    ) -> StorageError {
        StorageError(
            code: code,
            operation: operation,
            backend: .cloudflareDurableObject,
            message: "Cloudflare Durable Object binary protocol failed",
            underlyingDescription: String(describing: error)
        )
    }

    private func unexpectedResponse(operation: StorageOperation) -> StorageError {
        StorageError(
            code: .dataCorruption,
            operation: operation,
            backend: .cloudflareDurableObject,
            message: "Cloudflare Durable Object returned a response for a different operation"
        )
    }
}
