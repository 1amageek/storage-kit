public struct TupleEncodingSink {
    private enum Destination {
        case measuring
        case buffer(UnsafeMutableRawBufferPointer)
    }

    private let destination: Destination
    private var escapeDepth: Int
    public private(set) var byteCount: Int

    package init(measuringFrom offset: Int = 0) {
        self.destination = .measuring
        self.escapeDepth = 0
        self.byteCount = offset
    }

    package init(
        buffer: UnsafeMutableRawBufferPointer,
        startingAt offset: Int = 0
    ) {
        precondition(offset >= 0 && offset <= buffer.count)
        self.destination = .buffer(buffer)
        self.escapeDepth = 0
        self.byteCount = offset
    }

    public mutating func writeByte(_ byte: UInt8) {
        writeUnescapedByte(byte)
        if byte == 0 {
            for _ in 0..<escapeDepth {
                writeUnescapedByte(0xff)
            }
        }
    }

    public mutating func writeBytes<S: Sequence>(_ bytes: S)
    where S.Element == UInt8 {
        for byte in bytes {
            writeByte(byte)
        }
    }

    /// Writes one contiguous borrow without per-byte dispatch when no nested
    /// tuple escaping is active.
    public mutating func writeBytes(_ bytes: UnsafeRawBufferPointer) {
        guard escapeDepth == 0 else {
            for byte in bytes {
                writeByte(byte)
            }
            return
        }
        switch destination {
        case .measuring:
            break
        case .buffer(let buffer):
            precondition(byteCount <= buffer.count - bytes.count)
            let target = UnsafeMutableRawBufferPointer(
                start: buffer.baseAddress?.advanced(by: byteCount),
                count: bytes.count
            )
            target.copyMemory(from: bytes)
        }
        let (next, overflow) = byteCount.addingReportingOverflow(bytes.count)
        precondition(!overflow, "Tuple byte count overflow")
        byteCount = next
    }

    public mutating func withNullEscaping(
        _ body: (inout TupleEncodingSink) -> Void
    ) {
        escapeDepth += 1
        body(&self)
        escapeDepth -= 1
    }

    package func validateFinalByteCount(_ expected: Int) {
        precondition(
            byteCount == expected,
            "Tuple encoder measured \(expected) bytes but wrote \(byteCount)"
        )
    }

    private mutating func writeUnescapedByte(_ byte: UInt8) {
        switch destination {
        case .measuring:
            break
        case .buffer(let buffer):
            precondition(byteCount < buffer.count)
            buffer[byteCount] = byte
        }
        let (next, overflow) = byteCount.addingReportingOverflow(1)
        precondition(!overflow, "Tuple byte count overflow")
        byteCount = next
    }
}
