import StorageKit

/// An immutable description of a PostgreSQL range scan.
///
/// `Transaction.getRange` is synchronous (per the protocol), so it cannot run
/// I/O. Instead it resolves the `KeySelector` boundaries into `SQLRangeBoundary`
/// values and captures them here. `PostgreSQLRangeResult`'s iterator consumes the
/// plan lazily, fetching rows in bounded batches via keyset pagination so that
/// memory stays O(`batchSize`) regardless of how large the range is.
struct RangeScanPlan: Sendable {

    /// Resolved lower boundary (derived from the begin `KeySelector`).
    let begin: SQLRangeBoundary

    /// Resolved upper boundary (derived from the end `KeySelector`, exclusive).
    let end: SQLRangeBoundary

    /// Maximum number of rows to return across the whole scan (0 = unlimited).
    let limit: Int

    /// If true, rows are returned in descending key order.
    let reverse: Bool

    /// Number of rows fetched per database round-trip.
    let batchSize: Int

    /// Name of the KV table (a validated bare SQL identifier).
    let tableName: String
}
