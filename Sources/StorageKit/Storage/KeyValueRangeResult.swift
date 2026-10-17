import DatabaseTypes
/// Array-backed range result.
///
/// Used by the in-memory backend, whose transaction view already exists as an
/// owned collection. Persistent backends expose native lazy cursor sequences.
///
/// Supports deferred error propagation: if constructed with an error,
/// the error is thrown on the first `next()` call.
public struct KeyValueRangeResult: TransactionRangeResult {
    public typealias Element = (ByteString, ByteString)

    private let results: [(key: ByteString, value: ByteString)]
    private let error: (any Error)?
    private let overlay: KeyValueCursor?

    public init(_ results: [(key: ByteString, value: ByteString)]) {
        self.results = results
        self.error = nil
        self.overlay = nil
    }

    public init(error: any Error) {
        self.results = []
        self.error = error
        self.overlay = nil
    }

    package init(cursor: KeyValueCursor) {
        self.results = []
        self.error = nil
        self.overlay = cursor
    }

    public func makeCursor() -> Cursor {
        Cursor(results: results, error: error, overlay: overlay)
    }

    public struct Cursor: TransactionRangeCursor, Sendable {

        private var results: [(key: ByteString, value: ByteString)]?
        private var error: (any Error)?
        private var index: Int = 0
        private var overlay: KeyValueCursor?

        init(results: [(key: ByteString, value: ByteString)], error: (any Error)?, overlay: KeyValueCursor?) {
            self.results = results
            self.error = error
            self.overlay = overlay
        }

        public mutating func next() async throws -> (ByteString, ByteString)? {
            if var cursor = overlay {
                do {
                    let result = try await cursor.next()
                    overlay = cursor
                    return result
                } catch {
                    overlay = cursor
                    throw error
                }
            }
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
            if var cursor = overlay {
                overlay = nil
                try await cursor.finish()
            }
        }
    }
}
