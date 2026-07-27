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
    ) async throws -> ByteString {
        guard requestBytes.count <= maximumRequestBytes else {
            throw StorageHostTransportError.requestTooLarge(
                actual: requestBytes.count,
                maximum: maximumRequestBytes
            )
        }
        let responseBytes = try dispatcher.dispatch(
            requestBytes,
            maximumResponseBytes: maximumResponseBytes
        )
        guard responseBytes.count <= maximumResponseBytes else {
            throw StorageHostTransportError.responseTooLarge(
                actual: responseBytes.count,
                maximum: maximumResponseBytes
            )
        }
        return responseBytes
    }
}
