import DatabaseTypes
import StorageKitEmbeddedCore

#if arch(wasm32)
@_extern(wasm, module: "storage_host", name: "dispatch")
private func requestStorageResponseFrame(
    _ pointer: UInt32,
    _ length: UInt32
) -> UInt32

@_extern(c, "database_dealloc")
private func releaseStorageResponseFrame(
    _ pointer: UInt32,
    _ length: UInt32
)
#endif

public struct StorageHostDispatcher:
    StorageHostDispatching {
    public init() {}

    public func dispatch(
        _ requestBytes: ByteString,
        maximumResponseBytes: Int
    ) throws -> ByteString {
        guard maximumResponseBytes > 0 else {
            throw StorageHostTransportError.invalidLimit
        }
#if arch(wasm32)
        guard let requestLength = UInt32(exactly: requestBytes.count) else {
            throw StorageHostTransportError.requestLengthOverflow
        }
        let framePointer = requestBytes.withUnsafeBytes { buffer in
            let pointer = buffer.baseAddress.map {
                UInt32(truncatingIfNeeded: UInt(bitPattern: $0))
            } ?? 0
            return requestStorageResponseFrame(pointer, requestLength)
        }
        guard framePointer != 0 else {
            throw StorageHostTransportError.hostReturnedNoResponse
        }
        return try StorageHostResponseFrame.adopt(
            unsafeAddress: UInt(framePointer),
            maximumResponseBytes: maximumResponseBytes,
            deallocator: { address, count in
                guard let pointer = UInt32(exactly: address),
                      let byteCount = UInt32(exactly: count) else {
                    preconditionFailure(
                        "Storage response ownership exceeded the guest address space"
                    )
                }
                releaseStorageResponseFrame(pointer, byteCount)
            }
        )
#else
        _ = requestBytes
        throw StorageHostTransportError.unavailable
#endif
    }
}
