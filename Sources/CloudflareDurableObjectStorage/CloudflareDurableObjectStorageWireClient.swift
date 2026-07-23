import CloudflareDurableObjectStorageEmbedded
import StorageKit
import StorageKitEmbeddedCore

/// Typed StorageKit client backed by the fixed Cloudflare Durable Object StorageKit Wire.
public struct CloudflareDurableObjectStorageWireClient: CloudflareDurableObjectStorageClient {
    public let transport: any CloudflareDurableObjectStorageTransport

    public var callExecution: CloudflareDurableObjectCallExecution {
        transport.callExecution
    }

    public init(transport: any CloudflareDurableObjectStorageTransport) {
        self.transport = transport
    }

    public func read(_ request: CloudflareDurableObjectReadRequest) async throws -> CloudflareDurableObjectReadResponse {
        let response = try await send(
            .read(
                CloudflareDurableObjectEmbeddedReadRequest(
                    scope: try embeddedScope(request.scope, operation: .read),
                    key: request.key.rawValue.embeddedBytes,
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
            value: readResponse.value.map {
                CloudflareDurableObjectBytes(Bytes($0))
            },
            currentCommitVersion: readResponse.currentCommitVersion
        )
    }

    public func range(_ request: CloudflareDurableObjectRangeRequest) async throws -> CloudflareDurableObjectRangeResponse {
        let response = try await send(
            .range(
                CloudflareDurableObjectEmbeddedRangeRequest(
                    scope: try embeddedScope(request.scope, operation: .rangeRead),
                    begin: try embeddedBoundary(
                        request.begin,
                        operation: .rangeRead
                    ),
                    end: try embeddedBoundary(
                        request.end,
                        operation: .rangeRead
                    ),
                    limit: request.limit,
                    reverse: request.reverse,
                    snapshot: request.snapshot,
                    expectedReadVersion: request.expectedReadVersion,
                    cursorKey: request.cursorKey?.rawValue.embeddedBytes
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
                    key: CloudflareDurableObjectBytes(Bytes($0.key)),
                    value: CloudflareDurableObjectBytes(Bytes($0.value))
                )
            },
            hasMore: rangeResponse.hasMore,
            currentCommitVersion: rangeResponse.currentCommitVersion,
            readConflictRanges: rangeResponse.readConflictRanges.map(storageConflictRange)
        )
    }

    public func commit(_ request: CloudflareDurableObjectCommitRequest) async throws -> CloudflareDurableObjectCommitResponse {
        let response = try await send(
            .commit(
                CloudflareDurableObjectEmbeddedCommitRequest(
                    scope: try embeddedScope(request.scope, operation: .commit),
                    observedReadVersion: request.observedReadVersion,
                    mutations: try request.mutations.map { try embeddedMutation($0, operation: .commit) },
                    readConflictRanges: request.readConflictRanges.map(embeddedConflictRange),
                    writeConflictRanges: request.writeConflictRanges.map(
                        embeddedConflictRange
                    )
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

    public func rangeSize(
        _ request: CloudflareDurableObjectRangeSizeRequest
    ) async throws -> CloudflareDurableObjectRangeSizeResponse {
        let response = try await send(
            .rangeSize(
                CloudflareDurableObjectEmbeddedRangeSizeRequest(
                    scope: try embeddedScope(
                        request.scope,
                        operation: .rangeRead
                    ),
                    begin: request.begin.rawValue.embeddedBytes,
                    end: request.end.rawValue.embeddedBytes,
                    expectedReadVersion: request.expectedReadVersion
                )
            ),
            operation: .rangeRead
        )
        guard case .rangeSize(let sizeResponse) = response else {
            throw unexpectedResponse(operation: .rangeRead)
        }
        return CloudflareDurableObjectRangeSizeResponse(
            byteCount: sizeResponse.byteCount,
            currentCommitVersion: sizeResponse.currentCommitVersion
        )
    }

    public func rangeSplitPoints(
        _ request: CloudflareDurableObjectRangeSplitPointsRequest
    ) async throws -> CloudflareDurableObjectRangeSplitPointsResponse {
        let response = try await send(
            .rangeSplitPoints(
                CloudflareDurableObjectEmbeddedRangeSplitPointsRequest(
                    scope: try embeddedScope(
                        request.scope,
                        operation: .rangeRead
                    ),
                    begin: request.begin.rawValue.embeddedBytes,
                    end: request.end.rawValue.embeddedBytes,
                    chunkSize: request.chunkSize,
                    expectedReadVersion: request.expectedReadVersion
                )
            ),
            operation: .rangeRead
        )
        guard case .rangeSplitPoints(let splitResponse) = response else {
            throw unexpectedResponse(operation: .rangeRead)
        }
        return CloudflareDurableObjectRangeSplitPointsResponse(
            splitPoints: splitResponse.splitPoints.map {
                CloudflareDurableObjectBytes(Bytes($0))
            },
            currentCommitVersion: splitResponse.currentCommitVersion
        )
    }

    private func send(
        _ request: CloudflareDurableObjectEmbeddedRequest,
        operation: StorageOperation
    ) async throws -> CloudflareDurableObjectEmbeddedResponse {
        let requestBytes: EmbeddedBytes
        do {
            requestBytes = try CloudflareDurableObjectStorageWireCodec.encode(request)
        } catch {
            throw StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object StorageKit Wire failed",
                underlyingDescription: String(describing: error)
            )
        }

        let responseBytes: EmbeddedBytes
        do {
            responseBytes = try await transport.send(requestBytes)
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as any CloudflareDurableObjectStorageTransportFailure {
            throw transportFailureError(error, operation: operation)
        } catch let error as StorageError {
            throw transportError(error, operation: operation)
        } catch {
            throw transportError(
                StorageError(
                    code: .connectionFailure,
                    operation: operation,
                    backend: .cloudflareDurableObject,
                    message: "Cloudflare Durable Object storage transport failed",
                    underlyingDescription: String(describing: error)
                ),
                operation: operation
            )
        }

        do {
            let response = try CloudflareDurableObjectStorageWireCodec.decodeResponse(responseBytes)
            if case .failure(let status, let message) = response {
                throw storageError(status: status, message: message, operation: operation)
            }
            guard Self.matches(
                response,
                requestOperation: request.operation
            ) else {
                throw responseDecodeError(
                    unexpectedResponse(operation: operation),
                    operation: operation
                )
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
                    message: "Cloudflare Durable Object StorageKit Wire failed",
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
        _ = operation
        return EmbeddedKeySelector(
            key: selector.key.embeddedBytes,
            orEqual: selector.orEqual,
            offset: selector.offset
        )
    }

    private func embeddedBoundary(
        _ boundary: CloudflareDurableObjectRangeBoundary,
        operation: StorageOperation
    ) throws -> EmbeddedRangeBoundary {
        switch boundary {
        case .unbounded:
            return .unbounded
        case .selector(let selector):
            return .selector(
                try embeddedSelector(
                    selector.storageKitSelector,
                    operation: operation
                )
            )
        }
    }

    private func embeddedMutation(
        _ mutation: CloudflareDurableObjectMutation,
        operation: StorageOperation
    ) throws -> EmbeddedWriteOperation {
        switch mutation {
        case .set(let key, let value):
            return .set(
                key: key.rawValue.embeddedBytes,
                value: value.rawValue.embeddedBytes
            )
        case .clear(let key):
            return .clear(key: key.rawValue.embeddedBytes)
        case .clearRange(let begin, let end):
            return .clearRange(
                begin: begin.rawValue.embeddedBytes,
                end: end.rawValue.embeddedBytes
            )
        case .atomic(let key, let param, let mutationType):
            return .atomic(
                key: key.rawValue.embeddedBytes,
                param: param.rawValue.embeddedBytes,
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
            begin: range.begin.map { $0.rawValue.embeddedBytes },
            end: range.end.map { $0.rawValue.embeddedBytes }
        )
    }

    private func storageConflictRange(
        _ range: EmbeddedKeyRange
    ) -> CloudflareDurableObjectConflictRange {
        CloudflareDurableObjectConflictRange(
            begin: range.begin.map {
                CloudflareDurableObjectBytes(Bytes($0))
            },
            end: range.end.map {
                CloudflareDurableObjectBytes(Bytes($0))
            }
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

    private func transportFailureError(
        _ error: any CloudflareDurableObjectStorageTransportFailure,
        operation: StorageOperation
    ) -> StorageError {
        let code: StorageError.Code
        switch error.failureStage {
        case .localValidation:
            code = .invalidOperation
        case .unavailable:
            code = .resourceUnavailable
        case .afterDispatch:
            code = .connectionFailure
        }
        return transportError(
            StorageError(
                code: code,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object storage transport failed",
                underlyingDescription: String(describing: error)
            ),
            operation: operation
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
        status: CloudflareDurableObjectEmbeddedFailureStatus,
        message: String,
        operation: StorageOperation
    ) -> StorageError {
        let code: StorageError.Code
        switch status {
        case .transactionConflict:
            code = .transactionConflict
        case .invalidOperation:
            code = .invalidOperation
        case .backendFailure:
            code = .backendFailure
        case .resourceUnavailable:
            code = .resourceUnavailable
        case .backendContractViolation:
            code = .backendContractViolation
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
            message: "Cloudflare Durable Object StorageKit Wire failed",
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

    private static func matches(
        _ response: CloudflareDurableObjectEmbeddedResponse,
        requestOperation: CloudflareDurableObjectEmbeddedOperation
    ) -> Bool {
        switch (response, requestOperation) {
        case (.readiness, .readiness),
             (.read, .read),
             (.range, .range),
             (.commit, .commit),
             (.rangeSize, .rangeSize),
             (.rangeSplitPoints, .rangeSplitPoints):
            return true
        case (.failure, _):
            return false
        default:
            return false
        }
    }
}
