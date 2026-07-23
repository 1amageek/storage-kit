import FoundationDB
import StorageKit

/// A scoped pointer view used only while the originating StorageKit borrow is
/// active. The type is internal to this module so it cannot become a stored API.
struct ScopedByteInput: FDB.ByteInput {
    private let address: UInt
    let byteCount: Int

    init(_ bytes: UnsafeRawBufferPointer) {
        precondition(bytes.count == 0 || bytes.baseAddress != nil)
        self.address = bytes.baseAddress.map(UInt.init(bitPattern:)) ?? 0
        self.byteCount = bytes.count
    }

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        if byteCount == 0 {
            return try body(UnsafeRawBufferPointer(start: nil, count: 0))
        }
        guard let pointer = UnsafeRawPointer(bitPattern: address) else {
            preconditionFailure("Scoped byte input address is invalid")
        }
        return try body(UnsafeRawBufferPointer(start: pointer, count: byteCount))
    }
}
