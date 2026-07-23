import StorageKit

/// Lazy, re-iterable SQLite range sequence.
///
/// Constructing the sequence and iterator performs no SQLite prepare, step, or
/// row copy. The first `next()` opens the statement and advances one row.
public struct SQLiteRangeResult: TransactionRangeResult {
    public typealias Element = (Bytes, Bytes)

    private enum Backing: Sendable {
        case scan(SQLiteStorageTransaction, SQLiteRangeScanPlan)
        case failure(StorageError)
    }

    private let backing: Backing

    init(
        transaction: SQLiteStorageTransaction,
        plan: SQLiteRangeScanPlan
    ) {
        self.backing = .scan(transaction, plan)
    }

    init(error: StorageError) {
        self.backing = .failure(error)
    }

    public func makeAsyncIterator() -> Iterator {
        switch backing {
        case .scan(let transaction, let plan):
            return Iterator(transaction: transaction, plan: plan)
        case .failure(let error):
            return Iterator(error: error)
        }
    }

    public struct Iterator: TransactionRangeIterator, Sendable {
        private let state: SQLiteRangeIteratorState

        init(
            transaction: SQLiteStorageTransaction,
            plan: SQLiteRangeScanPlan
        ) {
            self.state = SQLiteRangeIteratorState(
                transaction: transaction,
                plan: plan
            )
        }

        init(error: StorageError) {
            self.state = SQLiteRangeIteratorState(error: error)
        }

        public mutating func next() async throws -> (Bytes, Bytes)? {
            try await state.next()
        }

        public mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            await state.finish()
        }
    }
}
