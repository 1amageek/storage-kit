/// Array-backed AsyncSequence for range scan results.
///
/// Used by the in-memory backend, whose transaction view already exists as an
/// owned collection. Persistent backends expose native lazy cursor sequences.
///
/// Supports deferred error propagation: if constructed with an error,
/// the error is thrown on the first `next()` call.
public struct KeyValueRangeResult: TransactionRangeResult {
    public typealias Element = (Bytes, Bytes)

    private let results: [(key: Bytes, value: Bytes)]
    private let error: (any Error)?

    public init(_ results: [(key: Bytes, value: Bytes)]) {
        self.results = results
        self.error = nil
    }

    public init(error: any Error) {
        self.results = []
        self.error = error
    }

    public func makeAsyncIterator() -> Iterator {
        Iterator(results: results, error: error)
    }

    public struct Iterator: TransactionRangeIterator, Sendable {
        private var results: [(key: Bytes, value: Bytes)]?
        private var error: (any Error)?
        private var index: Int = 0

        init(results: [(key: Bytes, value: Bytes)], error: (any Error)?) {
            self.results = results
            self.error = error
        }

        public mutating func next() async throws -> (Bytes, Bytes)? {
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
