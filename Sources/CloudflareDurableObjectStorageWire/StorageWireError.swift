/// Errors raised while reading or writing primitive storage wire values.
public enum StorageWireError: Error, Sendable, Equatable {
    case truncated
    case byteCountOverflow
    case byteCountExceedsLimit(count: Int, maximum: Int)
    case invalidBool(UInt8)
    case invalidUTF8
    case trailingBytes
    case invalidCursor
    case unknownMutationType(UInt8)
    case unknownOperation(UInt8)
    case unknownRangeBoundary(UInt8)
    case keySelectorOffsetOverflow
    case keySelectorOffsetExceedsLimit(offset: Int64, maximum: Int)
    case collectionCountExceedsLimit(count: Int, maximum: Int)
    case unknownWriteOperation(UInt8)
    case invalidRangeLimit
    case invalidRangeContinuation
    case invalidRangeBoundaries
    case invalidChunkSize(Int64)
    case invalidRangeByteCount(Int64)
    case invalidSplitPoints
}
