import StorageKitEmbeddedCore

enum CloudflareDurableObjectWasmMutationApplier {
    static func apply(
        hasExisting: UInt32,
        existingPointer: UInt32,
        existingLength: UInt32,
        paramPointer: UInt32,
        paramLength: UInt32,
        mutationTypeRaw: UInt32
    ) -> UInt32 {
        guard mutationTypeRaw <= UInt32(UInt8.max) else {
            return failureFrame()
        }
        guard let mutationType = EmbeddedMutationType(rawValue: UInt8(mutationTypeRaw)) else {
            return failureFrame()
        }
        let existing: [UInt8]?
        if hasExisting == 0 {
            existing = nil
        } else {
            guard let bytes = CloudflareDurableObjectWasmMemory.readBytes(
                pointer: existingPointer,
                length: existingLength
            ) else {
                return failureFrame()
            }
            existing = bytes
        }
        guard let param = CloudflareDurableObjectWasmMemory.readBytes(
            pointer: paramPointer,
            length: paramLength
        ) else {
            return failureFrame()
        }

        do {
            let result = try mutationType.apply(to: existing, param: param)
            return successFrame(result)
        } catch {
            return failureFrame()
        }
    }

    private static func successFrame(_ result: EmbeddedAtomicMutationResult) -> UInt32 {
        var payload: [UInt8] = [0]
        switch result {
        case .set(let value):
            payload.append(1)
            appendUInt32(UInt32(value.count), to: &payload)
            payload.append(contentsOf: value)
        case .clear:
            payload.append(2)
        case .unchanged:
            payload.append(3)
        }
        return CloudflareDurableObjectWasmMemory.makeFrame(payload: payload)
    }

    private static func failureFrame() -> UInt32 {
        CloudflareDurableObjectWasmMemory.makeFrame(payload: [1])
    }

    private static func appendUInt32(_ value: UInt32, to payload: inout [UInt8]) {
        payload.append(UInt8(truncatingIfNeeded: value))
        payload.append(UInt8(truncatingIfNeeded: value >> 8))
        payload.append(UInt8(truncatingIfNeeded: value >> 16))
        payload.append(UInt8(truncatingIfNeeded: value >> 24))
    }
}
