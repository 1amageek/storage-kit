import DatabaseTypes
/// Array-backed AsyncSequence for range scan results.
///
/// Used by the in-memory backend, whose transaction view already exists as an
/// owned collection. Persistent backends expose native lazy cursor sequences.
///
/// Supports deferred error propagation: if constructed with an error,
/// the error is thrown on the first `next()` call.
public struct KeyValueRangeResult: TransactionRangeResult {
    public typealias Element = (ByteString, ByteString)
    public typealias Failure = StorageError

    private let results: [(key: ByteString, value: ByteString)]
    private let error: StorageError?

    public init(_ results: [(key: ByteString, value: ByteString)]) {
        self.results = results
        self.error = nil
    }

    public init(error: StorageError) {
        self.results = []
        self.error = error
    }

    public func makeAsyncIterator() -> Iterator {
        Iterator(results: results, error: error)
    }

    public struct Iterator: TransactionRangeIterator, Sendable {
        public typealias Failure = StorageError

        private var results: [(key: ByteString, value: ByteString)]?
        private var error: StorageError?
        private var index: Int = 0

        init(results: [(key: ByteString, value: ByteString)], error: StorageError?) {
            self.results = results
            self.error = error
        }

        public mutating func next() async throws(StorageError) -> (ByteString, ByteString)? {
            if let error {
                self.error = nil
                results = nil
                throw error
            }
            guard let results else {
                return nil
            }
            guard index < results.count else {
                self.results = nil
                return nil
            }
            let entry = results[index]
            index += 1
            if index == results.count {
                self.results = nil
            }
            return (entry.key, entry.value)
        }

        public mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            results = nil
            error = nil
            index = 0
        }
    }
}
