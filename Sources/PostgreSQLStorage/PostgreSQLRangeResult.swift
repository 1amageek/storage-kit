import StorageKit

/// Lazy AsyncSequence for PostgreSQL range scan results.
///
/// Unlike `KeyValueRangeResult` (which eagerly holds an array), this type
/// defers the async flush + SQL query to the first `next()` call.
///
/// This solves the `Transaction.getRange()` constraint: the protocol method
/// is non-async, but PostgreSQL queries require async I/O. By deferring
/// execution to iteration time (which is always in an async context),
/// we satisfy the protocol without blocking.
public struct PostgreSQLRangeResult: AsyncSequence, Sendable {
    public typealias Element = (Bytes, Bytes)

    private let operation: @Sendable () async throws -> [(Bytes, Bytes)]

    /// Create a lazy range result that executes the given async operation on first iteration.
    init(_ operation: @escaping @Sendable () async throws -> [(Bytes, Bytes)]) {
        self.operation = operation
    }

    /// Create an immediately-failing range result.
    init(error: any Error) {
        self.operation = { throw error }
    }

    public func makeAsyncIterator() -> Iterator {
        Iterator(operation: operation)
    }

    public struct Iterator: AsyncIteratorProtocol {
        private let operation: @Sendable () async throws -> [(Bytes, Bytes)]
        private var results: [(Bytes, Bytes)]?
        private var index: Int = 0

        init(operation: @escaping @Sendable () async throws -> [(Bytes, Bytes)]) {
            self.operation = operation
        }

        public mutating func next() async throws -> (Bytes, Bytes)? {
            if results == nil {
                results = try await operation()
            }
            guard let results, index < results.count else { return nil }
            let entry = results[index]
            index += 1
            return entry
        }
    }
}
