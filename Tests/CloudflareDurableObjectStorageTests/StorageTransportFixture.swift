import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit

func decodeStorageTransportRequest(
    _ requestBytes: ByteString
) throws(StorageTransportError) -> StorageWireRequest {
    do {
        return try StorageWire.decodeRequest(requestBytes)
    } catch {
        throw .storage(
            StorageError(
                code: .backendFailure,
                operation: .read,
                backend: .cloudflareDurableObject,
                message: "Test transport could not decode its request",
                underlyingDescription: String(describing: error)
            )
        )
    }
}

func encodeStorageTransportResponse(
    _ response: StorageWireResponse
) throws(StorageTransportError) -> ByteString {
    do {
        return try StorageWire.encode(response)
    } catch {
        throw .storage(
            StorageError(
                code: .backendFailure,
                operation: .read,
                backend: .cloudflareDurableObject,
                message: "Test transport could not encode its response",
                underlyingDescription: String(describing: error)
            )
        )
    }
}
