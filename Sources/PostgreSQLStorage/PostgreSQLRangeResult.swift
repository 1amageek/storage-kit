import DatabaseTypes
import StorageKit

/// A lazy, re-iterable PostgreSQL range result.
///
/// `Transaction.getRange` is synchronous (a protocol constraint) but PostgreSQL
/// I/O is async, so the scan is described by a `RangeScanPlan` and executed
/// lazily during iteration. Rows are fetched in bounded batches via keyset
/// pagination (`PostgreSQLStorageTransaction.fetchRangeBatch`), keeping memory at
/// O(`batchSize`) regardless of how large the range is.
///
/// The result is re-iterable: each `makeCursor()` starts a fresh scan
/// from the plan. A scan whose boundaries could not be resolved is represented as
/// a `.failure`, which throws on the first `next()` rather than silently yielding
/// nothing.
public struct PostgreSQLRangeResult: TransactionRangeResult {

    public typealias Element = (ByteString, ByteString)

    private enum Backing: Sendable {
        case scan(PostgreSQLStorageTransaction, RangeScanPlan)
        case failure(StorageError)
    }

    private let backing: Backing

    /// Create a scan that paginates `plan` against `transaction`.
    init(transaction: PostgreSQLStorageTransaction, plan: RangeScanPlan) {
        self.backing = .scan(transaction, plan)
    }

    /// Create a range result that throws on first iteration.
    init(error: any Error) {
        if let storageError = error as? StorageError {
            self.backing = .failure(storageError)
        } else {
            self.backing = .failure(PostgreSQLStorageEngine.mapError(error))
        }
    }

    public func makeCursor() -> Cursor {
        switch backing {
        case .scan(let transaction, let plan):
            return Cursor(transaction: transaction, plan: plan)
        case .failure(let error):
            return Cursor(failure: error)
        }
    }

    /// Keyset-pagination iterator.
    ///
    /// A value is transferred between consumers, but each instance remains a
    /// single-consumer range cursor.
    public struct Cursor: TransactionRangeCursor, Sendable {

        private var transaction: PostgreSQLStorageTransaction?
        private var plan: RangeScanPlan?
        private var pendingFailure: StorageError?

        /// Rows fetched in the current batch and the serving cursor into them.
        private var buffer: [(ByteString, ByteString)] = []
        private var bufferIndex = 0

        /// The last key emitted, used as the keyset cursor for the next batch.
        private var lastKey: ByteString?

        /// Total rows emitted so far (enforces `plan.limit`).
        private var emitted = 0

        /// Whether the first batch has run (drives the one-time buffer flush).
        private var started = false

        /// Whether the scan reached the end of the range (a short batch or the
        /// row limit). Once set, no further queries are issued.
        private var exhausted = false

        /// Terminal flag: once true, `next()` always returns nil.
        private var done = false

        init(transaction: PostgreSQLStorageTransaction, plan: RangeScanPlan) {
            self.transaction = transaction
            self.plan = plan
        }

        init(failure: StorageError) {
            self.transaction = nil
            self.plan = nil
            self.pendingFailure = failure
        }

        public mutating func next() async throws -> (ByteString, ByteString)? {
            do {
                return try await nextEntry()
            } catch {
                finishState()
                throw error
            }
        }

        public mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            finishState()
        }

        private mutating func nextEntry() async throws -> (ByteString, ByteString)? {
            if done { return nil }

            if let failure = pendingFailure {
                finishState()
                throw failure
            }

            guard let transaction, let plan else {
                finishState()
                return nil
            }

            // Refill the buffer when the serving cursor has drained it.
            while bufferIndex >= buffer.count {
                if exhausted {
                    finishState()
                    return nil
                }
                let remaining = plan.limit > 0 ? plan.limit - emitted : plan.batchSize
                if remaining <= 0 {
                    finishState()
                    return nil
                }
                // Matches fetchRangeBatch's own LIMIT computation, so a batch
                // shorter than `target` reliably signals an exhausted range.
                // `Swift.min` disambiguates from AsyncSequence.min() visible
                // through the enclosing PostgreSQLRangeResult type.
                let target = plan.limit > 0 ? Swift.min(plan.batchSize, remaining) : plan.batchSize
                let queryCursor = lastKey?.detached()
                lastKey = queryCursor
                buffer.removeAll(keepingCapacity: false)
                bufferIndex = 0
                let batch = try await transaction.fetchRangeBatch(
                    plan: plan,
                    after: queryCursor,
                    remaining: remaining,
                    flushFirst: !started
                )
                started = true
                buffer = batch
                bufferIndex = 0
                if batch.count < target {
                    exhausted = true
                }
                if batch.isEmpty {
                    finishState()
                    return nil
                }
            }

            let entry = buffer[bufferIndex]
            bufferIndex += 1
            lastKey = entry.0
            emitted += 1
            if plan.limit > 0, emitted >= plan.limit {
                exhausted = true
            }
            return entry
        }

        private mutating func finishState() {
            transaction = nil
            plan = nil
            pendingFailure = nil
            buffer.removeAll(keepingCapacity: false)
            bufferIndex = 0
            lastKey = nil
            done = true
        }
    }
}
