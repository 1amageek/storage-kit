/// Errors raised by the embedded binary wire codec.
public enum EmbeddedWireError: Error, Sendable, Equatable {
    case truncated
    case byteCountOverflow
    case invalidBool(UInt8)
    case invalidUTF8
    case trailingBytes
    case invalidCursor
    case unknownMutationType(UInt8)
    case unknownOperation(UInt8)
    case unknownKeySelector(UInt8)
    case unknownWriteOperation(UInt8)
    case invalidRangeLimit
}
