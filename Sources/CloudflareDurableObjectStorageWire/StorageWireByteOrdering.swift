/// Lexicographic byte ordering used by StorageKit keys.
public enum StorageWireByteOrdering {
    public static func compare<LHS: Collection, RHS: Collection>(
        _ lhs: LHS,
        _ rhs: RHS
    ) -> Int where LHS.Element == UInt8, RHS.Element == UInt8 {
        let contiguousResult: Int? = lhs.withContiguousStorageIfAvailable {
            lhsBytes in
            rhs.withContiguousStorageIfAvailable { rhsBytes in
                compareContiguous(lhsBytes, rhsBytes)
            }
        } ?? nil
        if let contiguousResult {
            return contiguousResult
        }

        var lhsIndex = lhs.startIndex
        var rhsIndex = rhs.startIndex
        while lhsIndex != lhs.endIndex, rhsIndex != rhs.endIndex {
            let lhsByte = lhs[lhsIndex]
            let rhsByte = rhs[rhsIndex]
            if lhsByte != rhsByte {
                return lhsByte < rhsByte ? -1 : 1
            }
            lhs.formIndex(after: &lhsIndex)
            rhs.formIndex(after: &rhsIndex)
        }
        if lhsIndex == lhs.endIndex, rhsIndex == rhs.endIndex {
            return 0
        }
        return lhsIndex == lhs.endIndex ? -1 : 1
    }

    private static func compareContiguous(
        _ lhs: UnsafeBufferPointer<UInt8>,
        _ rhs: UnsafeBufferPointer<UInt8>
    ) -> Int {
        let commonCount = Swift.min(lhs.count, rhs.count)
        for index in 0..<commonCount {
            if lhs[index] != rhs[index] {
                return lhs[index] < rhs[index] ? -1 : 1
            }
        }
        if lhs.count == rhs.count {
            return 0
        }
        return lhs.count < rhs.count ? -1 : 1
    }

    public static func lessThan<LHS: Collection, RHS: Collection>(
        _ lhs: LHS,
        _ rhs: RHS
    ) -> Bool where LHS.Element == UInt8, RHS.Element == UInt8 {
        compare(lhs, rhs) < 0
    }
}
