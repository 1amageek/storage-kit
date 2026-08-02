import DatabaseTypes
import CloudflareDurableObjectStorageWire

#if arch(wasm32)
@_extern(wasm, module: "storage_host", name: "dispatch")
private func dispatchStorageRequest(
    _ pointer: UInt32,
    _ length: UInt32
) -> UInt32

@_extern(wasm, module: "storage_host", name: "receive")
private func receiveStorageResponse(
    _ pointer: UInt32,
    _ length: UInt32
)

@_extern(wasm, module: "storage_host", name: "discard")
private func discardStorageResponse()
#endif

public struct StorageHostDispatcher:
    StorageHostDispatching {
    public init() {}

    public func dispatch(
        _ requestBytes: ByteString,
        maximumResponseBytes: Int
    ) throws(StorageHostTransportError) -> ByteString {
        guard maximumResponseBytes > 0 else {
            throw StorageHostTransportError.invalidLimit
        }
#if arch(wasm32)
        guard let requestLength = UInt32(exactly: requestBytes.count) else {
            throw StorageHostTransportError.requestLengthOverflow
        }
        let responseByteCount = requestBytes.withUnsafeBytes { buffer in
            let pointer = buffer.baseAddress.map {
                UInt32(truncatingIfNeeded: UInt(bitPattern: $0))
            } ?? 0
            return dispatchStorageRequest(pointer, requestLength)
        }
        guard responseByteCount > 0 else {
            throw StorageHostTransportError.hostReturnedNoResponse
        }
        let addressableResponseByteCount = try StorageHostResponse
            .addressableByteCount(
                responseByteCount,
                discard: {
                    discardStorageResponse()
                }
            )
        return try StorageHostResponse.receive(
            byteCount: addressableResponseByteCount,
            maximumResponseBytes: maximumResponseBytes,
            discard: {
                discardStorageResponse()
            },
            copyInto: { destination in
                guard let baseAddress = destination.baseAddress else {
                    preconditionFailure(
                        "A nonempty storage response requires destination storage"
                    )
                }
                let destinationAddress = UInt(bitPattern: baseAddress)
                guard let pointer = UInt32(exactly: destinationAddress) else {
                    preconditionFailure(
                        "Storage response destination exceeded the guest address space"
                    )
                }
                receiveStorageResponse(pointer, responseByteCount)
            }
        )
#else
        _ = requestBytes
        throw StorageHostTransportError.unavailable
#endif
    }
}
