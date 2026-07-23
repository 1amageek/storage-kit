import StorageKitEmbeddedCore

/// Immutable-by-default byte value with constant-time owned slicing.
public struct Bytes:
    Sendable,
    Hashable,
    RandomAccessCollection,
    MutableCollection,
    RangeReplaceableCollection,
    ExpressibleByArrayLiteral,
    Codable {
    public typealias Element = UInt8
    public typealias Index = Int
    public typealias SubSequence = Bytes
    public typealias ArrayLiteralElement = UInt8

    private var storage: EmbeddedBytes

    public init() {
        self.storage = []
    }

    public init(_ bytes: [UInt8]) {
        self.storage = EmbeddedBytes(bytes)
    }

    /// Takes ownership of an array that is the complete intended byte value.
    public init(owningExact bytes: [UInt8]) {
        self.storage = EmbeddedBytes(owningExact: bytes)
    }

    public init(_ bytes: EmbeddedBytes) {
        self.storage = bytes
    }

    /// Retains an external immutable owner without copying its bytes.
    public init(retaining owner: any BytesOwner) {
        self.storage = EmbeddedBytes(
            retaining: RetainedStorageBytesOwner(owner: owner)
        )
    }

    public init<S: Sequence>(_ elements: S) where S.Element == UInt8 {
        self.init(Array(elements))
    }

    public init(repeating repeatedValue: UInt8, count: Int) {
        self.init([UInt8](repeating: repeatedValue, count: count))
    }

    public init(arrayLiteral elements: UInt8...) {
        self.init(elements)
    }

    /// Allocates final storage once and initializes it through a synchronous borrow.
    public static func copying(
        count: Int,
        _ initialize: (UnsafeMutableRawBufferPointer) -> Void
    ) -> Bytes {
        precondition(count >= 0)
        return Bytes(
            EmbeddedBytes.copying(count: count, initialize)
        )
    }

    /// Allocates final storage once and propagates a typed initialization error.
    public static func copying<Failure: Error>(
        count: Int,
        _ initialize: (
            UnsafeMutableRawBufferPointer
        ) throws(Failure) -> Void
    ) throws(Failure) -> Bytes {
        precondition(count >= 0)
        guard count > 0 else {
            return Bytes()
        }
        var initializationError: Failure?
        let bytes = [UInt8](unsafeUninitializedCapacity: count) {
            buffer,
            initializedCount in
            do {
                try initialize(UnsafeMutableRawBufferPointer(buffer))
                initializedCount = count
            } catch let error as Failure {
                initializationError = error
                initializedCount = 0
            } catch {
                preconditionFailure(
                    "Byte initialization threw an unexpected error type"
                )
            }
        }
        if let initializationError {
            throw initializationError
        }
        return Bytes(owningExact: bytes)
    }

    public var startIndex: Int { 0 }
    public var endIndex: Int { storage.count }

    public subscript(position: Int) -> UInt8 {
        get {
            precondition(indices.contains(position))
            return storage[position]
        }
        set {
            precondition(indices.contains(position))
            mutateStorage { $0[position] = newValue }
        }
    }

    public subscript(bounds: Range<Int>) -> Bytes {
        get {
            precondition(
                bounds.lowerBound >= startIndex
                    && bounds.upperBound <= endIndex
            )
            return Bytes(storage.slice(bounds))
        }
        set {
            replaceSubrange(bounds, with: newValue)
        }
    }

    public var embeddedBytes: EmbeddedBytes {
        storage
    }

    public func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        try storage.withUnsafeBytes(body)
    }

    /// Exposes contiguous storage to generic collection algorithms.
    ///
    /// The pointer is a synchronous borrow and must not escape `body`.
    public func withContiguousStorageIfAvailable<Result>(
        _ body: (UnsafeBufferPointer<UInt8>) throws -> Result
    ) rethrows -> Result? {
        try storage.withContiguousStorageIfAvailable(body)
    }

    public func withUnsafeBufferPointer<Result>(
        _ body: (UnsafeBufferPointer<UInt8>) throws -> Result
    ) rethrows -> Result {
        try storage.withUnsafeBytes { bytes in
            try body(bytes.bindMemory(to: UInt8.self))
        }
    }

    public func contiguousArray() -> [UInt8] {
        storage.contiguousArray()
    }

    public func copyBytes() -> [UInt8] {
        storage.withUnsafeBytes { Array($0) }
    }

    /// Returns exact independent storage for cursors and progress values that
    /// outlive the response frame from which they were decoded.
    public func detached() -> Bytes {
        Bytes(storage.detached())
    }

    public mutating func reserveCapacity(_ minimumCapacity: Int) {
        precondition(minimumCapacity >= 0)
        mutateStorage { $0.reserveCapacity(minimumCapacity) }
    }

    public mutating func replaceSubrange<C: Collection>(
        _ subrange: Range<Int>,
        with newElements: C
    ) where C.Element == UInt8 {
        precondition(
            subrange.lowerBound >= startIndex
                && subrange.upperBound <= endIndex
        )
        mutateStorage {
            $0.replaceSubrange(subrange, with: newElements)
        }
    }

    public mutating func append(_ newElement: UInt8) {
        mutateStorage { $0.append(newElement) }
    }

    public mutating func append(contentsOf newElements: Bytes) {
        guard !newElements.isEmpty else {
            return
        }
        self = self + newElements
    }

    public mutating func append(contentsOf newElements: [UInt8]) {
        guard !newElements.isEmpty else {
            return
        }
        self = self + newElements
    }

    public mutating func append<S: Sequence>(contentsOf newElements: S)
    where S.Element == UInt8 {
        // A single-pass sequence has no stable final count. Materialize it once
        // before allocating the immutable result buffer at its exact size.
        append(contentsOf: Array(newElements))
    }

    public mutating func removeAll(keepingCapacity keepCapacity: Bool = false) {
        mutateStorage { $0.removeAll(keepingCapacity: keepCapacity) }
    }

    public init(from decoder: any Decoder) throws {
        var container = try decoder.unkeyedContainer()
        var bytes: [UInt8] = []
        bytes.reserveCapacity(container.count ?? 0)
        while !container.isAtEnd {
            bytes.append(try container.decode(UInt8.self))
        }
        self.init(bytes)
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.unkeyedContainer()
        for byte in self {
            try container.encode(byte)
        }
    }

    private mutating func mutateStorage(
        _ mutation: (inout [UInt8]) -> Void
    ) {
        var bytes = storage.contiguousArray()
        storage = []
        mutation(&bytes)
        storage = EmbeddedBytes(bytes)
    }
}

public func + (lhs: Bytes, rhs: Bytes) -> Bytes {
    let (count, overflow) = lhs.count.addingReportingOverflow(rhs.count)
    precondition(!overflow)
    return Bytes.copying(count: count) { output in
        lhs.withUnsafeBytes { source in
            destination(output, offset: 0, count: lhs.count)
                .copyMemory(from: source)
        }
        rhs.withUnsafeBytes { source in
            destination(output, offset: lhs.count, count: rhs.count)
                .copyMemory(from: source)
        }
    }
}

public func + (lhs: Bytes, rhs: [UInt8]) -> Bytes {
    let (count, overflow) = lhs.count.addingReportingOverflow(rhs.count)
    precondition(!overflow)
    return Bytes.copying(count: count) { output in
        lhs.withUnsafeBytes { source in
            destination(output, offset: 0, count: lhs.count)
                .copyMemory(from: source)
        }
        rhs.withUnsafeBytes { source in
            destination(output, offset: lhs.count, count: rhs.count)
                .copyMemory(from: source)
        }
    }
}

public func + (lhs: [UInt8], rhs: Bytes) -> Bytes {
    let (count, overflow) = lhs.count.addingReportingOverflow(rhs.count)
    precondition(!overflow)
    return Bytes.copying(count: count) { output in
        lhs.withUnsafeBytes { source in
            destination(output, offset: 0, count: lhs.count)
                .copyMemory(from: source)
        }
        rhs.withUnsafeBytes { source in
            destination(output, offset: lhs.count, count: rhs.count)
                .copyMemory(from: source)
        }
    }
}

private func destination(
    _ buffer: UnsafeMutableRawBufferPointer,
    offset: Int,
    count: Int
) -> UnsafeMutableRawBufferPointer {
    UnsafeMutableRawBufferPointer(
        start: buffer.baseAddress?.advanced(by: offset),
        count: count
    )
}

public func == (lhs: Bytes, rhs: [UInt8]) -> Bool {
    lhs.elementsEqual(rhs)
}

public func == (lhs: [UInt8], rhs: Bytes) -> Bool {
    rhs.elementsEqual(lhs)
}
