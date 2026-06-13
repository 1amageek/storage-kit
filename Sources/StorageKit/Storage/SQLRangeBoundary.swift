/// A KeySelector resolved into a SQL-expressible range boundary.
///
/// SQL backends (SQLite, PostgreSQL) cannot express FDB KeySelector offsets
/// directly. The four FDB-standard selector encodings map to either a direct
/// comparison against the selector key, or a comparison against a scalar
/// subquery that first resolves the selector to a concrete key.
///
/// ## Mapping (begin boundary — result keys are `>=` the resolved key)
///
/// | Selector | Encoding | SQL |
/// |---|---|---|
/// | firstGreaterOrEqual(k) | (orEqual: false, offset: 1) | `key >= k` |
/// | firstGreaterThan(k) | (orEqual: true, offset: 1) | `key > k` |
/// | lastLessOrEqual(k) | (orEqual: true, offset: 0) | `key >= (SELECT max(key) WHERE key <= k)` |
/// | lastLessThan(k) | (orEqual: false, offset: 0) | `key >= (SELECT max(key) WHERE key < k)` |
///
/// ## Mapping (end boundary — result keys are `<` the resolved key, EXCLUSIVE)
///
/// | Selector | Encoding | SQL |
/// |---|---|---|
/// | firstGreaterOrEqual(k) | (orEqual: false, offset: 1) | `key < k` |
/// | firstGreaterThan(k) | (orEqual: true, offset: 1) | `key <= k` |
/// | lastLessOrEqual(k) | (orEqual: true, offset: 0) | `key < (SELECT max(key) WHERE key <= k)` |
/// | lastLessThan(k) | (orEqual: false, offset: 0) | `key < (SELECT max(key) WHERE key < k)` |
///
/// The subquery cases follow FDB semantics: the selector resolves to a concrete
/// key first, then the range includes (begin) or excludes (end) that key.
/// When the subquery finds no key:
/// - begin: the selector resolves to "before all keys" → clamp to the start
///   (all keys match). Rendered via `COALESCE(subquery, <empty bytes>)` with
///   `key >= <empty>` being always true.
/// - end: the selector resolves to "before all keys" → empty range.
///   Rendered via `COALESCE(subquery, <empty bytes>)` with `key < <empty>`
///   being always false.
///
/// Selectors with other offsets cannot be expressed as a single SQL predicate
/// and are rejected with `StorageError(.invalidOperation)`.
package enum SQLRangeBoundary: Sendable, Hashable {
    /// `key {op} $key` — the selector key is compared directly.
    case direct(op: String, key: Bytes)
    /// `key {op} COALESCE((SELECT max(key) FROM t WHERE key {subqueryOp} $key), <empty>)`
    /// — the selector is resolved to a concrete key by a scalar subquery first.
    case resolvedSubquery(op: String, subqueryOp: String, key: Bytes)

    /// Resolve a begin selector to a SQL boundary.
    ///
    /// - Throws: `StorageError(.invalidOperation)` for selector offsets outside
    ///   the four FDB-standard encodings.
    package static func begin(_ selector: KeySelector) throws -> SQLRangeBoundary {
        switch (selector.orEqual, selector.offset) {
        case (false, 1):
            return .direct(op: ">=", key: selector.key)
        case (true, 1):
            return .direct(op: ">", key: selector.key)
        case (true, 0):
            return .resolvedSubquery(op: ">=", subqueryOp: "<=", key: selector.key)
        case (false, 0):
            return .resolvedSubquery(op: ">=", subqueryOp: "<", key: selector.key)
        default:
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "KeySelector(orEqual: \(selector.orEqual), offset: \(selector.offset)) "
                    + "cannot be expressed as a SQL begin boundary; "
                    + "only the four FDB-standard selectors are supported"
            )
        }
    }

    /// Resolve an end selector to a SQL boundary (exclusive).
    ///
    /// - Throws: `StorageError(.invalidOperation)` for selector offsets outside
    ///   the four FDB-standard encodings.
    package static func end(_ selector: KeySelector) throws -> SQLRangeBoundary {
        switch (selector.orEqual, selector.offset) {
        case (false, 1):
            return .direct(op: "<", key: selector.key)
        case (true, 1):
            return .direct(op: "<=", key: selector.key)
        case (true, 0):
            return .resolvedSubquery(op: "<", subqueryOp: "<=", key: selector.key)
        case (false, 0):
            return .resolvedSubquery(op: "<", subqueryOp: "<", key: selector.key)
        default:
            throw StorageError(
                code: .invalidOperation,
                operation: .rangeRead,
                message: "KeySelector(orEqual: \(selector.orEqual), offset: \(selector.offset)) "
                    + "cannot be expressed as a SQL end boundary; "
                    + "only the four FDB-standard selectors are supported"
            )
        }
    }
}
