import FoundationDB
import StorageKit

/// A scoped pointer view used only while the originating StorageKit borrow is
/// active. The type is internal to this module so it cannot become a stored API.
struct BorrowedFoundationDBByteSource: FDB.ByteSource {
    private let address: UInt
    let count: Int

    init(_ bytes: UnsafeRawBufferPointer) {
        precondition(bytes.count == 0 || bytes.baseAddress != nil)
        self.address = bytes.baseAddress.map(UInt.init(bitPattern:)) ?? 0
        self.count = bytes.count
    }

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        if count == 0 {
            return try body(UnsafeRawBufferPointer(start: nil, count: 0))
        }
        guard let pointer = UnsafeRawPointer(bitPattern: address) else {
            preconditionFailure("Borrowed FoundationDB byte address is invalid")
        }
        return try body(UnsafeRawBufferPointer(start: pointer, count: count))
    }
}
