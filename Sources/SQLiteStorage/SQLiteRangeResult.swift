import DatabaseTypes
import StorageKit

/// Lazy, re-iterable SQLite range result.
///
/// Constructing the sequence and iterator performs no SQLite prepare, step, or
/// row copy. The first `next()` opens the statement and advances one row.
public struct SQLiteRangeResult: TransactionRangeResult {
    public typealias Element = (ByteString, ByteString)

    private enum Backing: Sendable {
        case scan(SQLiteStorageTransaction, SQLiteRangeScanPlan)
        case failure(StorageError)
        case overlay(KeyValueCursor)
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

    init(cursor: KeyValueCursor) {
        self.backing = .overlay(cursor)
    }

    public func makeCursor() -> Cursor {
        switch backing {
        case .scan(let transaction, let plan):
            return Cursor(transaction: transaction, plan: plan)
        case .failure(let error):
            return Cursor(error: error)
        case .overlay(let cursor):
            return Cursor(cursor: cursor)
        }
    }

    public struct Cursor: TransactionRangeCursor, Sendable {
        private let state: SQLiteRangeIteratorState?
        private var overlay: KeyValueCursor?

        init(
            transaction: SQLiteStorageTransaction,
            plan: SQLiteRangeScanPlan
        ) {
            self.overlay = nil
            self.state = SQLiteRangeIteratorState(
                transaction: transaction,
                plan: plan
            )
        }

        init(error: StorageError) {
            self.overlay = nil
            self.state = SQLiteRangeIteratorState(error: error)
        }

        init(cursor: KeyValueCursor) {
            self.state = nil
            self.overlay = cursor
        }

        public mutating func next() async throws -> (ByteString, ByteString)? {
            if var cursor = overlay {
                defer { overlay = cursor }
                return try await cursor.next()
            }
            return try await state?.next()
        }

        public mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            if var cursor = overlay {
                defer { overlay = cursor }
                try await cursor.finish()
            } else {
                await state?.finish()
            }
        }
    }
}
