enum CloudflareDurableObjectWasmMemory {
    static func allocate(byteCount: UInt32) -> UInt32 {
        let pointer = UnsafeMutableRawPointer.allocate(
            byteCount: Int(byteCount),
            alignment: MemoryLayout<UInt8>.alignment
        )
        return UInt32(truncatingIfNeeded: UInt(bitPattern: pointer))
    }

    static func deallocate(pointer: UInt32, byteCount: UInt32) {
        guard byteCount > 0 else {
            return
        }
        guard let rawPointer = UnsafeMutableRawPointer(bitPattern: Int(pointer)) else {
            return
        }
        rawPointer.deallocate()
    }

    static func readBytes(pointer: UInt32, length: UInt32) -> [UInt8]? {
        guard length > 0 else {
            return []
        }
        guard let rawPointer = UnsafeRawPointer(bitPattern: Int(pointer)) else {
            return nil
        }
        let buffer = rawPointer.bindMemory(to: UInt8.self, capacity: Int(length))
        var bytes: [UInt8] = []
        bytes.reserveCapacity(Int(length))
        var index = 0
        while index < Int(length) {
            bytes.append(buffer[index])
            index += 1
        }
        return bytes
    }

    static func makeFrame(payload: [UInt8]) -> UInt32 {
        let frameLength = payload.count + 4
        let pointer = allocate(byteCount: UInt32(frameLength))
        guard let rawPointer = UnsafeMutableRawPointer(bitPattern: Int(pointer)) else {
            return 0
        }
        let buffer = rawPointer.bindMemory(to: UInt8.self, capacity: frameLength)
        let payloadLength = UInt32(payload.count)
        buffer[0] = UInt8(truncatingIfNeeded: payloadLength)
        buffer[1] = UInt8(truncatingIfNeeded: payloadLength >> 8)
        buffer[2] = UInt8(truncatingIfNeeded: payloadLength >> 16)
        buffer[3] = UInt8(truncatingIfNeeded: payloadLength >> 24)
        var index = 0
        while index < payload.count {
            buffer[index + 4] = payload[index]
            index += 1
        }
        return pointer
    }

    static func readFrame(pointer: UInt32) -> (payload: [UInt8], byteCount: UInt32)? {
        guard let rawPointer = UnsafeRawPointer(bitPattern: Int(pointer)) else {
            return nil
        }
        let header = rawPointer.bindMemory(to: UInt8.self, capacity: 4)
        let payloadLength = UInt32(header[0])
            | (UInt32(header[1]) << 8)
            | (UInt32(header[2]) << 16)
            | (UInt32(header[3]) << 24)
        guard payloadLength <= UInt32.max - 4 else {
            return nil
        }
        let byteCount = payloadLength + 4
        guard let payloadPointer = UnsafeRawPointer(bitPattern: Int(pointer) + 4) else {
            return nil
        }
        let payloadBuffer = payloadPointer.bindMemory(to: UInt8.self, capacity: Int(payloadLength))
        var payload: [UInt8] = []
        payload.reserveCapacity(Int(payloadLength))
        var index = 0
        while index < Int(payloadLength) {
            payload.append(payloadBuffer[index])
            index += 1
        }
        return (payload, byteCount)
    }
}
