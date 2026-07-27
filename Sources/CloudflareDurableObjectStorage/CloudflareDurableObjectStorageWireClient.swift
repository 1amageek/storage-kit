import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit

/// Typed StorageKit client backed by the fixed Cloudflare Durable Object StorageKit Wire.
public struct CloudflareDurableObjectStorageWireClient: CloudflareDurableObjectStorageClient {
    public let transport: any CloudflareDurableObjectStorageTransport

    public var callExecution: CloudflareDurableObjectCallExecution {
        transport.callExecution
    }

    public init(transport: any CloudflareDurableObjectStorageTransport) {
        self.transport = transport
    }

    public func read(
        _ request: StorageWireReadRequest
    ) async throws -> StorageWireReadResponse {
        let response = try await send(.read(request), operation: .read)
        guard case .read(let readResponse) = response else {
            throw unexpectedResponse(operation: .read)
        }
        return readResponse
    }

    public func range(
        _ request: StorageWireRangeRequest
    ) async throws -> StorageWireRangeResponse {
        let response = try await send(.range(request), operation: .rangeRead)
        guard case .range(let rangeResponse) = response else {
            throw unexpectedResponse(operation: .rangeRead)
        }
        return rangeResponse
    }

    public func commit(
        _ request: StorageWireCommitRequest
    ) async throws -> StorageWireCommitResponse {
        let response = try await send(.commit(request), operation: .commit)
        guard case .commit(let commitResponse) = response else {
            throw unexpectedResponse(operation: .commit)
        }
        return commitResponse
    }

    public func readiness(
        _ request: StorageWireReadinessRequest
    ) async throws -> StorageWireReadinessResponse {
        let response = try await send(.readiness(request), operation: .initialize)
        guard case .readiness(let readinessResponse) = response else {
            throw unexpectedResponse(operation: .initialize)
        }
        return readinessResponse
    }

    public func rangeSize(
        _ request: StorageWireRangeSizeRequest
    ) async throws -> StorageWireRangeSizeResponse {
        let response = try await send(.rangeSize(request), operation: .rangeRead)
        guard case .rangeSize(let sizeResponse) = response else {
            throw unexpectedResponse(operation: .rangeRead)
        }
        return sizeResponse
    }

    public func rangeSplitPoints(
        _ request: StorageWireRangeSplitPointsRequest
    ) async throws -> StorageWireRangeSplitPointsResponse {
        let response = try await send(
            .rangeSplitPoints(request),
            operation: .rangeRead
        )
        guard case .rangeSplitPoints(let splitResponse) = response else {
            throw unexpectedResponse(operation: .rangeRead)
        }
        return splitResponse
    }

    private func send(
        _ request: StorageWireRequest,
        operation: StorageOperation
    ) async throws -> StorageWireResponse {
        let requestBytes: ByteString
        do {
            requestBytes = try StorageWire.encode(request)
        } catch {
            throw StorageError(
                code: .invalidOperation,
                operation: operation,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object StorageKit Wire failed",
                underlyingDescription: String(describing: error)
            )
        }

        let responseBytes: ByteString
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
            let response = try StorageWire.decodeResponse(responseBytes)
            if case .failure(let status, let message) = response {
                throw storageError(status: status, message: message, operation: operation)
            }
            guard
                Self.matches(
                    response,
                    requestOperation: request.operation
                )
            else {
                throw responseDecodeError(
                    unexpectedResponse(operation: operation),
                    operation: operation
                )
            }
            return response
        } catch let error as StorageError {
            throw error
        } catch let error as StorageWireProtocolError {
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
        status: StorageWireFailureStatus,
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
        from error: StorageWireProtocolError,
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
        _ response: StorageWireResponse,
        requestOperation: StorageWireOperation
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
