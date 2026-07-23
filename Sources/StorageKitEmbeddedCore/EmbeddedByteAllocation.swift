/// Owns an immutable contiguous byte allocation without storing a raw pointer.
public final class EmbeddedByteAllocation: Sendable {
    public let count: Int

    private let address: UInt
    private let deallocator: @Sendable (UInt, Int) -> Void

    public init(
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

    /// The stable allocation address. It remains valid only while this owner
    /// or another owner that explicitly retains it is alive.
    public var unsafeAddress: UInt { address }

    deinit {
        deallocator(address, count)
    }

    public func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        guard count > 0 else {
            return try body(UnsafeRawBufferPointer(start: nil, count: 0))
        }
        guard let pointer = UnsafeRawPointer(bitPattern: address) else {
            preconditionFailure("Owned byte allocation has an invalid address")
        }
        return try body(
            UnsafeRawBufferPointer(start: pointer, count: count)
        )
    }
}
