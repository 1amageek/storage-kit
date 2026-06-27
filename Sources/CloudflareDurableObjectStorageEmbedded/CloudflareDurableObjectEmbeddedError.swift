import StorageKitEmbeddedCore

/// Errors for the Cloudflare Durable Object Embedded wire layer.
public enum CloudflareDurableObjectEmbeddedError: Error, Sendable, Equatable {
    case wire(EmbeddedWireError)
    case rangeOverlay(EmbeddedRangeOverlayError)
    case unsupportedProtocolVersion(UInt8)
    case unknownOperation(UInt8)
    case unknownStatus(UInt8)
    case invalidScope
    case invalidName
}

extension CloudflareDurableObjectEmbeddedError {
    static func readUInt8(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) -> UInt8 {
        do {
            return try reader.readUInt8()
        } catch {
            throw .wire(error)
        }
    }

    static func readBool(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) -> Bool {
        do {
            return try reader.readBool()
        } catch {
            throw .wire(error)
        }
    }

    static func readUInt32(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) -> UInt32 {
        do {
            return try reader.readUInt32()
        } catch {
            throw .wire(error)
        }
    }

    static func readCount(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) -> Int {
        do {
            return try reader.readCount()
        } catch {
            throw .wire(error)
        }
    }

    static func readInt32(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) -> Int32 {
        do {
            return try reader.readInt32()
        } catch {
            throw .wire(error)
        }
    }

    static func readInt64(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) -> Int64 {
        do {
            return try reader.readInt64()
        } catch {
            throw .wire(error)
        }
    }

    static func readBytes(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) -> [UInt8] {
        do {
            return try reader.readBytes()
        } catch {
            throw .wire(error)
        }
    }

    static func readString(from reader: inout EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) -> String {
        do {
            return try reader.readString()
        } catch {
            throw .wire(error)
        }
    }

    static func ensureFullyRead(_ reader: EmbeddedBinaryReader) throws(CloudflareDurableObjectEmbeddedError) {
        do {
            try reader.ensureFullyRead()
        } catch {
            throw .wire(error)
        }
    }

    static func writeBytes(
        _ value: [UInt8],
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        do {
            try writer.writeBytes(value)
        } catch {
            throw .wire(error)
        }
    }

    static func writeCount(
        _ count: Int,
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        do {
            try writer.writeCount(count)
        } catch {
            throw .wire(error)
        }
    }

    static func writeString(
        _ value: String,
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        do {
            try writer.writeString(value)
        } catch {
            throw .wire(error)
        }
    }

    static func encode(
        _ selector: EmbeddedKeySelector,
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        do {
            try selector.encode(into: &writer)
        } catch {
            throw .wire(error)
        }
    }

    static func encode(
        _ row: EmbeddedKeyValue,
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        do {
            try row.encode(into: &writer)
        } catch {
            throw .wire(error)
        }
    }

    static func encode(
        _ range: EmbeddedKeyRange,
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        do {
            try range.encode(into: &writer)
        } catch {
            throw .wire(error)
        }
    }

    static func encode(
        _ operation: EmbeddedWriteOperation,
        into writer: inout EmbeddedBinaryWriter
    ) throws(CloudflareDurableObjectEmbeddedError) {
        do {
            try operation.encode(into: &writer)
        } catch {
            throw .wire(error)
        }
    }

    static func readKeySelector(
        from reader: inout EmbeddedBinaryReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> EmbeddedKeySelector {
        do {
            return try EmbeddedKeySelector(from: &reader)
        } catch {
            throw .wire(error)
        }
    }

    static func readKeyValue(
        from reader: inout EmbeddedBinaryReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> EmbeddedKeyValue {
        do {
            return try EmbeddedKeyValue(from: &reader)
        } catch {
            throw .wire(error)
        }
    }

    static func readKeyRange(
        from reader: inout EmbeddedBinaryReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> EmbeddedKeyRange {
        do {
            return try EmbeddedKeyRange(from: &reader)
        } catch {
            throw .wire(error)
        }
    }

    static func readWriteOperation(
        from reader: inout EmbeddedBinaryReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> EmbeddedWriteOperation {
        do {
            return try EmbeddedWriteOperation(from: &reader)
        } catch {
            throw .wire(error)
        }
    }

    static func validateMutationRoundTrip(
        _ mutationType: EmbeddedMutationType,
        reader: inout EmbeddedBinaryReader
    ) throws(CloudflareDurableObjectEmbeddedError) -> EmbeddedMutationType {
        do {
            return try EmbeddedMutationType(from: &reader)
        } catch {
            throw .wire(error)
        }
    }

    static func overlay(
        committedRows: [EmbeddedKeyValue],
        writes: [EmbeddedWriteOperation],
        begin: EmbeddedKeySelector,
        end: EmbeddedKeySelector,
        reverse: Bool,
        limit: Int
    ) throws(CloudflareDurableObjectEmbeddedError) -> [EmbeddedKeyValue] {
        do {
            return try EmbeddedRangeOverlay.overlay(
                committedRows: committedRows,
                writes: writes,
                begin: begin,
                end: end,
                reverse: reverse,
                limit: limit
            )
        } catch {
            throw .rangeOverlay(error)
        }
    }
}
