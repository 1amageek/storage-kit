import DatabaseTypes
import DatabaseTypesFoundation
import StorageKit
#if canImport(FoundationEssentials)
import FoundationEssentials

extension FoundationEssentials.UUID: TupleElement {
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        DatabaseTypes.UUID(self).encodeTuple(to: &sink)
    }

    public static func decodeTuple(
        from bytes: ByteString,
        at offset: inout Int
    ) throws -> FoundationEssentials.UUID {
        FoundationEssentials.UUID(
            try DatabaseTypes.UUID.decodeTuple(from: bytes, at: &offset)
        )
    }
}
#else
import Foundation

extension Foundation.UUID: TupleElement {
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        DatabaseTypes.UUID(self).encodeTuple(to: &sink)
    }

    public static func decodeTuple(
        from bytes: ByteString,
        at offset: inout Int
    ) throws -> Foundation.UUID {
        Foundation.UUID(
            try DatabaseTypes.UUID.decodeTuple(from: bytes, at: &offset)
        )
    }
}
#endif
