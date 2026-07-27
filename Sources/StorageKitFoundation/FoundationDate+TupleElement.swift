import DatabaseTypes
import DatabaseTypesFoundation
import StorageKit
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

extension Date: TupleElement {
    public func encodeTuple(to sink: inout TupleEncodingSink) {
        timeIntervalSince1970.encodeTuple(to: &sink)
    }

    public static func decodeTuple(
        from bytes: ByteString,
        at offset: inout Int
    ) throws -> Date {
        let interval = try Double.decodeTuple(from: bytes, at: &offset)
        return Date(timeIntervalSince1970: interval)
    }
}
