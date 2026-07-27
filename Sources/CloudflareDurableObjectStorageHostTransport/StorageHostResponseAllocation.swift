import DatabaseTypes

/// Owns a response frame allocated by the storage host until every borrowed
/// payload view has been released.
///
/// The host transfers one initialized byte allocation whose exact size is
/// `count`. This owner calls `deallocator` exactly once from `deinit`. Borrows
/// retain this owner, remain synchronous, and never expose the pointer outside
/// the closure. Bytes are immutable after transfer, require no typed alignment,
/// and are never rebound or mutated. Concurrent borrows are safe because the
/// host cannot reclaim or modify the allocation while this owner is alive.
final class StorageHostResponseAllocation: ByteStringOwner {
    let count: Int

    private let address: UInt
    private let deallocator: @Sendable (UInt, Int) -> Void

    init(
        unsafeAddress address: UInt,
        count: Int,
        deallocator: @escaping @Sendable (UInt, Int) -> Void
    ) {
        precondition(count >= 0)
        precondition(count == 0 || address != 0)
        self.address = address
        self.count = count
        self.deallocator = deallocator
    }

    deinit {
        deallocator(address, count)
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        guard count > 0 else {
            return try body(
                UnsafeRawBufferPointer(start: nil, count: 0)
            )
        }
        guard let pointer = UnsafeRawPointer(bitPattern: address) else {
            preconditionFailure("Storage host response address is invalid")
        }
        try body(
            UnsafeRawBufferPointer(start: pointer, count: count)
        )
    }
}
