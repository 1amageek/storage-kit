/// Array-backed AsyncSequence for range scan results.
///
/// Used by backends that collect results eagerly (InMemory, SQLite).
/// FDB uses its own streaming `AsyncKVSequence` instead.
///
/// Supports deferred error propagation: if constructed with an error,
/// the error is thrown on the first `next()` call.
public struct KeyValueRangeResult: AsyncSequence, Sendable {
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

    public struct Iterator: AsyncIteratorProtocol {
        private let results: [(key: Bytes, value: Bytes)]
        private let error: (any Error)?
        private var index: Int = 0

        init(results: [(key: Bytes, value: Bytes)], error: (any Error)?) {
            self.results = results
            self.error = error
        }

        public mutating func next() async throws -> (Bytes, Bytes)? {
            if let error { throw error }
            guard index < results.count else { return nil }
            let entry = results[index]
            index += 1
            return (entry.key, entry.value)
        }
    }
}
