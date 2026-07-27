import DatabaseTypes
import StorageKitEmbeddedCore

/// Adopts the storage response frame allocated in guest memory by the host.
enum StorageHostResponseFrame {
    static func adopt(
        unsafeAddress address: UInt,
        maximumResponseBytes: Int,
        deallocator: @escaping @Sendable (UInt, Int) -> Void
    ) throws(StorageHostTransportError) -> ByteString {
        guard maximumResponseBytes > 0 else {
            throw .invalidLimit
        }
        guard let frame = UnsafeRawPointer(bitPattern: address) else {
            throw .hostReturnedNoResponse
        }

        let header = frame.bindMemory(to: UInt8.self, capacity: 4)
        let payloadLength = UInt32(header[0])
            | (UInt32(header[1]) << 8)
            | (UInt32(header[2]) << 16)
            | (UInt32(header[3]) << 24)
        let payloadCount = Int(payloadLength)
        let frameByteCountResult = payloadCount.addingReportingOverflow(4)
        guard !frameByteCountResult.overflow,
              let frameByteCount32 = UInt32(
                exactly: frameByteCountResult.partialValue
              ) else {
            preconditionFailure(
                "Storage host returned a frame length that cannot describe its allocation"
            )
        }
        let frameByteCount = Int(frameByteCount32)
        let allocation = StorageHostResponseAllocation(
            unsafeAddress: address,
            count: frameByteCount,
            deallocator: deallocator
        )
        guard payloadCount <= maximumResponseBytes else {
            throw .responseTooLarge(
                actual: payloadCount,
                maximum: maximumResponseBytes
            )
        }
        let frameBytes = ByteString(retaining: allocation)
        return frameBytes[4..<frameByteCount]
    }
}
