import CloudflareDurableObjectStorageEmbedded
import StorageKitEmbeddedCore

enum CloudflareDurableObjectWasmRequestDispatcher {
    static func dispatch(pointer: UInt32, length: UInt32) -> UInt32 {
        guard let requestBytes = CloudflareDurableObjectWasmMemory.readBytes(pointer: pointer, length: length) else {
            return failureFrame(message: "Invalid request memory")
        }

        do {
            _ = try CloudflareDurableObjectEmbeddedRuntime.decodeRequest(requestBytes)
        } catch {
            return failureFrame(message: message(for: error))
        }

        let hostFramePointer = storagekit_host_dispatch(pointer, length)
        guard hostFramePointer != 0 else {
            return failureFrame(message: "Host dispatch returned no response")
        }
        guard let frame = CloudflareDurableObjectWasmMemory.readFrame(pointer: hostFramePointer) else {
            return failureFrame(message: "Host dispatch returned an invalid response frame")
        }

        do {
            _ = try CloudflareDurableObjectEmbeddedRuntime.decodeResponse(frame.payload)
            return hostFramePointer
        } catch {
            CloudflareDurableObjectWasmMemory.deallocate(
                pointer: hostFramePointer,
                byteCount: frame.byteCount
            )
            return failureFrame(message: message(for: error))
        }
    }

    static func failureFrame(message: String) -> UInt32 {
        let response = CloudflareDurableObjectEmbeddedResponse.failure(
            status: .invalidOperation,
            message: message
        )
        do {
            let bytes = try CloudflareDurableObjectEmbeddedRuntime.encode(response)
            return CloudflareDurableObjectWasmMemory.makeFrame(payload: bytes)
        } catch {
            return 0
        }
    }

    private static func message(for error: CloudflareDurableObjectEmbeddedError) -> String {
        switch error {
        case .wire(let wireError):
            return "Wire error: \(message(for: wireError))"
        case .rangeOverlay:
            return "Range overlay error"
        case .unsupportedProtocolVersion(let version):
            return "Unsupported protocol version: \(version)"
        case .unknownOperation(let operation):
            return "Unknown operation: \(operation)"
        case .unknownStatus(let status):
            return "Unknown status: \(status)"
        case .invalidScope:
            return "Invalid scope"
        case .invalidName:
            return "Invalid Durable Object name"
        }
    }

    private static func message(for error: EmbeddedWireError) -> String {
        switch error {
        case .truncated:
            return "truncated input"
        case .trailingBytes:
            return "trailing bytes"
        case .byteCountOverflow:
            return "byte count overflow"
        case .invalidCursor:
            return "invalid cursor"
        case .unknownMutationType(let value):
            return "unknown mutation type \(value)"
        case .unknownOperation(let value):
            return "unknown operation \(value)"
        case .unknownWriteOperation(let value):
            return "unknown write operation \(value)"
        case .unknownKeySelector(let value):
            return "unknown key selector \(value)"
        case .invalidBool(let value):
            return "invalid bool \(value)"
        case .invalidUTF8:
            return "invalid UTF-8"
        case .invalidRangeLimit:
            return "invalid range limit"
        }
    }
}
