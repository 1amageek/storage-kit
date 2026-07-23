import StorageKit

/// SQLite query plan for resolving one FoundationDB-compatible key selector.
struct SQLiteKeySelectionPlan: Sendable {
    enum Comparison: Sendable {
        case greaterThanOrEqual
        case greaterThan
        case lessThanOrEqual
        case lessThan

        var sql: String {
            switch self {
            case .greaterThanOrEqual: ">="
            case .greaterThan: ">"
            case .lessThanOrEqual: "<="
            case .lessThan: "<"
            }
        }
    }

    enum Order: Sendable {
        case ascending
        case descending

        var sql: String {
            switch self {
            case .ascending: "ASC"
            case .descending: "DESC"
            }
        }
    }

    let key: Bytes
    let comparison: Comparison
    let order: Order
    let offset: Int64

    init(selector: KeySelector) throws {
        self.key = selector.key

        if selector.offset > 0 {
            comparison = selector.orEqual
                ? .greaterThan
                : .greaterThanOrEqual
            order = .ascending
            offset = Int64(selector.offset - 1)
            return
        }

        comparison = selector.orEqual
            ? .lessThanOrEqual
            : .lessThan
        order = .descending
        guard let resolvedOffset = Int64(exactly: selector.offset.magnitude) else {
            throw StorageError(
                code: .resourceUnavailable,
                operation: .rangeRead,
                backend: .sqlite,
                message: "SQLite key selector offset exceeds Int64.max"
            )
        }
        offset = resolvedOffset
    }
}
