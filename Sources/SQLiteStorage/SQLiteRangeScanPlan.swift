import StorageKit

struct SQLiteRangeScanPlan: Sendable {
    let begin: SQLRangeBoundary
    let end: SQLRangeBoundary
    let limit: Int
    let reverse: Bool
}
