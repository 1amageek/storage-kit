/// Immutable owned bytes with constant-time zero-copy slicing.
public struct EmbeddedBytes:
    Sendable,
    Hashable,
    RandomAccessCollection,
    ExpressibleByArrayLiteral {
    public typealias Element = UInt8
    public typealias Index = Int
    public typealias ArrayLiteralElement = UInt8

    public enum SharedStorage: Sendable {
        case array([UInt8], Range<Int>)
        case allocation(EmbeddedByteAllocation, Range<Int>)
        case owner(any EmbeddedByteOwner, Range<Int>)
    }

    private enum Storage: Sendable {
        case array([UInt8])
        case exactArray([UInt8])
        case allocation(EmbeddedByteAllocation)
        case owner(any EmbeddedByteOwner)
    }

    private let storage: Storage
    private let storageRange: Range<Int>

    public init(_ bytes: [UInt8]) {
        self.storage = .array(bytes)
        self.storageRange = 0..<bytes.count
    }

    /// Takes ownership of an array that is the complete intended byte value.
    ///
    /// This preserves the array as the canonical detached representation and
    /// prevents a later full-range `detached()` call from materializing it again.
    public init(owningExact bytes: [UInt8]) {
        self.storage = .exactArray(bytes)
        self.storageRange = 0..<bytes.count
    }

    public init() {
        self.storage = .exactArray([])
        self.storageRange = 0..<0
    }

    public init(allocation: EmbeddedByteAllocation) {
        self.storage = .allocation(allocation)
        self.storageRange = 0..<allocation.count
    }

    /// Retains an external immutable owner without copying its bytes.
    public init(retaining owner: any EmbeddedByteOwner) {
        precondition(owner.count >= 0)
        self.storage = .owner(owner)
        self.storageRange = 0..<owner.count
    }

    public init(sharing bytes: [UInt8], storageRange: Range<Int>) {
        precondition(
            storageRange.lowerBound >= bytes.startIndex
                && storageRange.upperBound <= bytes.endIndex
        )
        self.storage = .array(bytes)
        self.storageRange = storageRange
    }

    public init(
        sharing allocation: EmbeddedByteAllocation,
        storageRange: Range<Int>
    ) {
        precondition(
            storageRange.lowerBound >= 0
                && storageRange.upperBound <= allocation.count
        )
        self.storage = .allocation(allocation)
        self.storageRange = storageRange
    }

    public init(arrayLiteral elements: UInt8...) {
        if elements.isEmpty {
            self.init()
        } else {
            self.init(elements)
        }
    }

    /// Allocates final storage once and initializes it through a synchronous borrow.
    public static func copying(
        count: Int,
        _ initialize: (UnsafeMutableRawBufferPointer) -> Void
    ) -> EmbeddedBytes {
        precondition(count >= 0)
        guard count > 0 else {
            return EmbeddedBytes()
        }
        let bytes = [UInt8](unsafeUninitializedCapacity: count) {
            buffer,
            initializedCount in
            initialize(UnsafeMutableRawBufferPointer(buffer))
            initializedCount = count
        }
        return EmbeddedBytes(exactBytes: bytes)
    }

    init(exactBytes: [UInt8]) {
        self.storage = .exactArray(exactBytes)
        self.storageRange = 0..<exactBytes.count
    }

    private init(storage: Storage, storageRange: Range<Int>) {
        self.storage = storage
        self.storageRange = storageRange
    }

    public var startIndex: Int { 0 }
    public var endIndex: Int { storageRange.count }

    public var sharedStorage: SharedStorage {
        switch storage {
        case .array(let bytes), .exactArray(let bytes):
            return .array(bytes, storageRange)
        case .allocation(let allocation):
            return .allocation(allocation, storageRange)
        case .owner(let owner):
            return .owner(owner, storageRange)
        }
    }

    public subscript(position: Int) -> UInt8 {
        precondition(position >= startIndex && position < endIndex)
        return withUnsafeBytes { $0[position] }
    }

    public func slice(_ range: Range<Int>) -> EmbeddedBytes {
        precondition(range.lowerBound >= 0 && range.upperBound <= count)
        return EmbeddedBytes(
            storage: storage,
            storageRange: (storageRange.lowerBound + range.lowerBound)..<(
                storageRange.lowerBound + range.upperBound
            )
        )
    }

    public func appending(_ byte: UInt8) -> EmbeddedBytes {
        var bytes = contiguousArray()
        bytes.reserveCapacity(count + 1)
        bytes.append(byte)
        return EmbeddedBytes(bytes)
    }

    public func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        switch storage {
        case .array(let bytes), .exactArray(let bytes):
            return try bytes.withUnsafeBytes { storageBytes in
                let start = storageBytes.baseAddress?.advanced(
                    by: storageRange.lowerBound
                )
                return try body(
                    UnsafeRawBufferPointer(
                        start: start,
                        count: storageRange.count
                    )
                )
            }
        case .allocation(let allocation):
            return try allocation.withUnsafeBytes { storageBytes in
                let start = storageBytes.baseAddress?.advanced(
                    by: storageRange.lowerBound
                )
                return try body(
                    UnsafeRawBufferPointer(
                        start: start,
                        count: storageRange.count
                    )
                )
            }
        case .owner(let owner):
            var outcome: EmbeddedByteBorrowOutcome<Result> = .missing
            let ownerCount = owner.count
            try owner.borrowBytes { storageBytes in
                precondition(storageBytes.count == ownerCount)
                precondition(
                    storageBytes.count == 0
                        || storageBytes.baseAddress != nil
                )
                guard case .missing = outcome else {
                    preconditionFailure(
                        "EmbeddedByteOwner invoked its borrow callback more than once"
                    )
                }
                let start = storageBytes.baseAddress?.advanced(
                    by: storageRange.lowerBound
                )
                outcome = .value(
                    try body(
                        UnsafeRawBufferPointer(
                            start: start,
                            count: storageRange.count
                        )
                    )
                )
            }
            switch outcome {
            case .value(let result):
                return result
            case .missing:
                preconditionFailure(
                    "EmbeddedByteOwner did not invoke its borrow callback"
                )
            }
        }
    }

    /// Exposes contiguous storage to generic collection algorithms.
    ///
    /// The pointer is a synchronous borrow and must not escape `body`.
    public func withContiguousStorageIfAvailable<Result>(
        _ body: (UnsafeBufferPointer<UInt8>) throws -> Result
    ) rethrows -> Result? {
        try withUnsafeBytes { bytes in
            try body(bytes.bindMemory(to: UInt8.self))
        }
    }

    public func contiguousArray() -> [UInt8] {
        switch storage {
        case .array(let bytes) where storageRange == bytes.indices:
            return bytes
        case .exactArray(let bytes) where storageRange == bytes.indices:
            return bytes
        case .array, .exactArray, .allocation, .owner:
            return withUnsafeBytes { Array($0) }
        }
    }

    /// Returns exact independent storage that cannot retain a larger backing
    /// allocation or an owner with unknown retained capacity.
    public func detached() -> EmbeddedBytes {
        guard !isEmpty else {
            return EmbeddedBytes()
        }
        switch storage {
        case .exactArray(let bytes) where storageRange == bytes.indices:
            return self
        case .array, .exactArray, .allocation, .owner:
            break
        }
        return EmbeddedBytes.copying(count: count) { destination in
            withUnsafeBytes { source in
                destination.copyMemory(from: source)
            }
        }
    }

    public func encode(
        into writer: inout EmbeddedWireWriter
    ) throws(EmbeddedWireError) {
        try writer.writeBytes(self)
    }

    public init(
        from reader: inout EmbeddedWireReader
    ) throws(EmbeddedWireError) {
        self = try reader.readByteRegion()
    }

    public static func == (lhs: EmbeddedBytes, rhs: EmbeddedBytes) -> Bool {
        guard lhs.count == rhs.count else {
            return false
        }
        return lhs.withUnsafeBytes { lhsBytes in
            rhs.withUnsafeBytes { rhsBytes in
                lhsBytes.elementsEqual(rhsBytes)
            }
        }
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(count)
        withUnsafeBytes { bytes in
            for byte in bytes {
                hasher.combine(byte)
            }
        }
    }
}

private enum EmbeddedByteBorrowOutcome<Value> {
    case missing
    case value(Value)
}
