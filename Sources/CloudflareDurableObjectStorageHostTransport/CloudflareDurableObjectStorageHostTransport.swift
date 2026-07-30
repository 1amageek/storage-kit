import DatabaseTypes
import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire

public struct CloudflareDurableObjectStorageHostTransport:
    CloudflareDurableObjectStorageTransport {
    public static let defaultMaximumFrameBytes = 16 * 1_024 * 1_024

    public let dispatcher: any StorageHostDispatching
    public let maximumRequestBytes: Int
    public let maximumResponseBytes: Int

    public var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    public init(
        dispatcher: any StorageHostDispatching = StorageHostDispatcher(),
        maximumRequestBytes: Int = Self.defaultMaximumFrameBytes,
        maximumResponseBytes: Int = Self.defaultMaximumFrameBytes
    ) throws {
        guard maximumRequestBytes > 0, maximumResponseBytes > 0 else {
            throw StorageHostTransportError.invalidLimit
        }
        guard maximumRequestBytes <= Self.defaultMaximumFrameBytes else {
            throw StorageHostTransportError
                .limitExceedsProtocolMaximum(
                    actual: maximumRequestBytes,
                    maximum: Self.defaultMaximumFrameBytes
                )
        }
        guard maximumResponseBytes <= Self.defaultMaximumFrameBytes else {
            throw StorageHostTransportError
                .limitExceedsProtocolMaximum(
                    actual: maximumResponseBytes,
                    maximum: Self.defaultMaximumFrameBytes
                )
        }
        self.dispatcher = dispatcher
        self.maximumRequestBytes = maximumRequestBytes
        self.maximumResponseBytes = maximumResponseBytes
    }

    public func send(
        _ requestBytes: ByteString
    ) async throws(StorageTransportError) -> ByteString {
        guard requestBytes.count <= maximumRequestBytes else {
            throw .rejected(
                stage: StorageHostTransportError.requestTooLarge(
                    actual: requestBytes.count,
                    maximum: maximumRequestBytes
                ).failureStage
            )
        }
        // The synchronous WASM host import must begin from a fresh task turn.
        // This keeps the host frame budget independent of the framework call
        // depth that produced the StorageKit Wire request.
        await Task.yield()
        let responseBytes: ByteString
        do {
            responseBytes = try dispatcher.dispatch(
                requestBytes,
                maximumResponseBytes: maximumResponseBytes
            )
        } catch {
            throw .rejected(stage: error.failureStage)
        }
        guard responseBytes.count <= maximumResponseBytes else {
            throw .rejected(
                stage: StorageHostTransportError.responseTooLarge(
                    actual: responseBytes.count,
                    maximum: maximumResponseBytes
                ).failureStage
            )
        }
        return responseBytes
    }
}
