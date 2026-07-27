import DatabaseTypes

/// Errors for the Cloudflare Durable Object storage protocol.
public enum StorageWireProtocolError: Error, Sendable, Equatable {
    case wire(StorageWireError)
    case unsupportedProtocolVersion(UInt8)
    case unknownOperation(UInt8)
    case unknownStatus(UInt8)
    case invalidScope
    case invalidVersion(Int64)
}

extension StorageWireProtocolError {
    static func readUInt8(from reader: inout StorageWireReader) throws(StorageWireProtocolError) -> UInt8 {
        do {
            return try reader.readUInt8()
        } catch {
            throw .wire(error)
        }
    }

    static func readBool(from reader: inout StorageWireReader) throws(StorageWireProtocolError) -> Bool {
        do {
            return try reader.readBool()
        } catch {
            throw .wire(error)
        }
    }

    static func readUInt32(from reader: inout StorageWireReader) throws(StorageWireProtocolError) -> UInt32 {
        do {
            return try reader.readUInt32()
        } catch {
            throw .wire(error)
        }
    }

    static func readUInt64(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> UInt64 {
        do {
            return try reader.readUInt64()
        } catch {
            throw .wire(error)
        }
    }

    static func readCount(from reader: inout StorageWireReader) throws(StorageWireProtocolError) -> Int {
        do {
            return try reader.readCount()
        } catch {
            throw .wire(error)
        }
    }

    static func readCount(
        from reader: inout StorageWireReader,
        maximum: Int
    ) throws(StorageWireProtocolError) -> Int {
        let count = try readCount(from: &reader)
        guard count <= maximum else {
            throw .wire(
                .collectionCountExceedsLimit(
                    count: count,
                    maximum: maximum
                )
            )
        }
        return count
    }

    static func readInt32(from reader: inout StorageWireReader) throws(StorageWireProtocolError) -> Int32 {
        do {
            return try reader.readInt32()
        } catch {
            throw .wire(error)
        }
    }

    static func readInt64(from reader: inout StorageWireReader) throws(StorageWireProtocolError) -> Int64 {
        do {
            return try reader.readInt64()
        } catch {
            throw .wire(error)
        }
    }

    static func readBytes(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> ByteString {
        try readBytes(from: &reader, maximum: StorageWireLimits.cloudflareDurableObject.maxValueBytes)
    }

    static func readBytes(
        from reader: inout StorageWireReader,
        maximum: Int
    ) throws(StorageWireProtocolError) -> ByteString {
        do {
            return try reader.readByteRegion(maximum: maximum)
        } catch {
            throw .wire(error)
        }
    }

    static func readString(from reader: inout StorageWireReader) throws(StorageWireProtocolError) -> String {
        try readString(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxScopeComponentBytes
        )
    }

    static func readString(
        from reader: inout StorageWireReader,
        maximum: Int
    ) throws(StorageWireProtocolError) -> String {
        do {
            return try reader.readString(maximum: maximum)
        } catch {
            throw .wire(error)
        }
    }

    static func ensureFullyRead(_ reader: StorageWireReader) throws(StorageWireProtocolError) {
        do {
            try reader.ensureFullyRead()
        } catch {
            throw .wire(error)
        }
    }

    static func writeBytes(
        _ value: ByteString,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        try writeBytes(
            value,
            maximum: StorageWireLimits.cloudflareDurableObject.maxValueBytes,
            into: &writer
        )
    }

    static func writeBytes(
        _ value: ByteString,
        maximum: Int,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        guard value.count <= maximum else {
            throw .wire(.byteCountExceedsLimit(count: value.count, maximum: maximum))
        }
        do {
            try writer.writeBytes(value)
        } catch {
            throw .wire(error)
        }
    }

    static func writeCount(
        _ count: Int,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        do {
            try writer.writeCount(count)
        } catch {
            throw .wire(error)
        }
    }

    static func writeCount(
        _ count: Int,
        maximum: Int,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        guard count <= maximum else {
            throw .wire(
                .collectionCountExceedsLimit(
                    count: count,
                    maximum: maximum
                )
            )
        }
        try writeCount(count, into: &writer)
    }

    static func writeString(
        _ value: String,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        try writeString(
            value,
            maximum: StorageWireLimits.cloudflareDurableObject.maxScopeComponentBytes,
            into: &writer
        )
    }

    static func writeString(
        _ value: String,
        maximum: Int,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        let count = value.utf8.count
        guard count <= maximum else {
            throw .wire(.byteCountExceedsLimit(count: count, maximum: maximum))
        }
        do {
            try writer.writeString(value)
        } catch {
            throw .wire(error)
        }
    }

    static func encode(
        _ selector: StorageWireKeySelector,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        let limits = StorageWireLimits.cloudflareDurableObject
        try writeBytes(selector.key, maximum: limits.maxKeyBytes, into: &writer)
        writer.writeBool(selector.orEqual)
        guard let offset = Int64(exactly: selector.offset) else {
            throw .wire(.keySelectorOffsetOverflow)
        }
        guard absOffset(offset) <= UInt64(limits.maxSelectorResolutionSteps) else {
            throw .wire(
                .keySelectorOffsetExceedsLimit(
                    offset: offset,
                    maximum: limits.maxSelectorResolutionSteps
                )
            )
        }
        writer.writeInt64(offset)
    }

    static func encode(
        _ boundary: StorageWireRangeBoundary,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        switch boundary {
        case .unbounded:
            writer.writeUInt8(0)
        case .selector(let selector):
            writer.writeUInt8(1)
            try encode(selector, into: &writer)
        }
    }

    static func encode(
        _ row: StorageWireKeyValue,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        let limits = StorageWireLimits.cloudflareDurableObject
        try writeBytes(row.key, maximum: limits.maxKeyBytes, into: &writer)
        try writeBytes(row.value, maximum: limits.maxValueBytes, into: &writer)
    }

    static func encode(
        _ range: StorageWireKeyRange,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        try writeOptionalBoundary(range.begin, into: &writer)
        try writeOptionalBoundary(range.end, into: &writer)
    }

    static func encode(
        _ operation: StorageWireWriteOperation,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        let limits = StorageWireLimits.cloudflareDurableObject
        switch operation {
        case .set(let key, let value):
            writer.writeUInt8(1)
            try writeBytes(key, maximum: limits.maxKeyBytes, into: &writer)
            try writeBytes(value, maximum: limits.maxValueBytes, into: &writer)
        case .clear(let key):
            writer.writeUInt8(2)
            try writeBytes(key, maximum: limits.maxKeyBytes, into: &writer)
        case .clearRange(let begin, let end):
            writer.writeUInt8(3)
            try writeBytes(
                begin,
                maximum: limits.maxBoundaryBytes,
                into: &writer
            )
            try writeBytes(
                end,
                maximum: limits.maxBoundaryBytes,
                into: &writer
            )
        case .atomic(let key, let param, let mutationType):
            writer.writeUInt8(4)
            try validateAtomicOperands(
                key: key,
                param: param,
                mutationType: mutationType
            )
            try writeBytes(
                key,
                maximum: mutationType == .setVersionstampedKey
                    ? limits.maxVersionstampedKeyOperandBytes
                    : limits.maxKeyBytes,
                into: &writer
            )
            try writeBytes(
                param,
                maximum: mutationType == .setVersionstampedValue
                    ? limits.maxVersionstampedValueOperandBytes
                    : limits.maxValueBytes,
                into: &writer
            )
            mutationType.encode(into: &writer)
        }
    }

    static func readKeySelector(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> StorageWireKeySelector {
        let limits = StorageWireLimits.cloudflareDurableObject
        let key = try readBytes(from: &reader, maximum: limits.maxKeyBytes)
        let orEqual = try readBool(from: &reader)
        let offset = try readInt64(from: &reader)
        guard absOffset(offset) <= UInt64(limits.maxSelectorResolutionSteps) else {
            throw .wire(
                .keySelectorOffsetExceedsLimit(
                    offset: offset,
                    maximum: limits.maxSelectorResolutionSteps
                )
            )
        }
        guard let selectorOffset = Int(exactly: offset) else {
            throw .wire(.keySelectorOffsetOverflow)
        }
        return StorageWireKeySelector(key: key, orEqual: orEqual, offset: selectorOffset)
    }

    static func readRangeBoundary(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> StorageWireRangeBoundary {
        switch try readUInt8(from: &reader) {
        case 0:
            return .unbounded
        case 1:
            return .selector(try readKeySelector(from: &reader))
        case let tag:
            throw .wire(.unknownRangeBoundary(tag))
        }
    }

    static func readKeyValue(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> StorageWireKeyValue {
        let limits = StorageWireLimits.cloudflareDurableObject
        return StorageWireKeyValue(
            key: try readBytes(from: &reader, maximum: limits.maxKeyBytes),
            value: try readBytes(from: &reader, maximum: limits.maxValueBytes)
        )
    }

    static func readKeyRange(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> StorageWireKeyRange {
        StorageWireKeyRange(
            begin: try readOptionalBoundary(from: &reader),
            end: try readOptionalBoundary(from: &reader)
        )
    }

    static func readWriteOperation(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> StorageWireWriteOperation {
        let limits = StorageWireLimits.cloudflareDurableObject
        switch try readUInt8(from: &reader) {
        case 1:
            return .set(
                key: try readBytes(from: &reader, maximum: limits.maxKeyBytes),
                value: try readBytes(from: &reader, maximum: limits.maxValueBytes)
            )
        case 2:
            return .clear(
                key: try readBytes(from: &reader, maximum: limits.maxKeyBytes)
            )
        case 3:
            return .clearRange(
                begin: try readBytes(
                    from: &reader,
                    maximum: limits.maxBoundaryBytes
                ),
                end: try readBytes(
                    from: &reader,
                    maximum: limits.maxBoundaryBytes
                )
            )
        case 4:
            let key = try readBytes(
                from: &reader,
                maximum: limits.maxVersionstampedKeyOperandBytes
            )
            let param = try readBytes(
                from: &reader,
                maximum: limits.maxVersionstampedValueOperandBytes
            )
            let mutationType: StorageWireMutationType
            do {
                mutationType = try StorageWireMutationType(from: &reader)
            } catch {
                throw .wire(error)
            }
            try validateAtomicOperands(
                key: key,
                param: param,
                mutationType: mutationType
            )
            return .atomic(key: key, param: param, mutationType: mutationType)
        case let tag:
            throw .wire(.unknownWriteOperation(tag))
        }
    }

    private static func validateAtomicOperands(
        key: ByteString,
        param: ByteString,
        mutationType: StorageWireMutationType
    ) throws(StorageWireProtocolError) {
        let limits = StorageWireLimits.cloudflareDurableObject
        let maximumKey = mutationType == .setVersionstampedKey
            ? limits.maxVersionstampedKeyOperandBytes
            : limits.maxKeyBytes
        let maximumParam = mutationType == .setVersionstampedValue
            ? limits.maxVersionstampedValueOperandBytes
            : limits.maxValueBytes
        guard key.count <= maximumKey else {
            throw .wire(
                .byteCountExceedsLimit(
                    count: key.count,
                    maximum: maximumKey
                )
            )
        }
        guard param.count <= maximumParam else {
            throw .wire(
                .byteCountExceedsLimit(
                    count: param.count,
                    maximum: maximumParam
                )
            )
        }
    }

    private static func writeOptionalBoundary(
        _ value: ByteString?,
        into writer: inout StorageWireWriter
    ) throws(StorageWireProtocolError) {
        guard let value else {
            writer.writeBool(false)
            return
        }
        writer.writeBool(true)
        try writeBytes(
            value,
            maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes,
            into: &writer
        )
    }

    private static func readOptionalBoundary(
        from reader: inout StorageWireReader
    ) throws(StorageWireProtocolError) -> ByteString? {
        guard try readBool(from: &reader) else {
            return nil
        }
        return try readBytes(
            from: &reader,
            maximum: StorageWireLimits.cloudflareDurableObject.maxBoundaryBytes
        )
    }

    private static func absOffset(_ value: Int64) -> UInt64 {
        value >= 0 ? UInt64(value) : UInt64(bitPattern: ~value) + 1
    }
}
