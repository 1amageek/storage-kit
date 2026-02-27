/// AsyncSequence representing range scan results.
///
/// Has different internal implementations per backend, but provides a unified AsyncSequence interface.
/// Includes a convenience initializer for immediate results (e.g. InMemory).
public struct KeyValueSequence: AsyncSequence, Sendable {
    public typealias Element = (key: Bytes, value: Bytes)

    private let stream: AsyncStream<Element>

    /// Create from an AsyncStream (when the backend returns results via streaming).
    public init(_ stream: AsyncStream<Element>) {
        self.stream = stream
    }

    /// Create from immediate results (e.g. InMemory, when all results are available at once).
    public init(_ results: [(key: Bytes, value: Bytes)]) {
        self.stream = AsyncStream { continuation in
            for item in results {
                continuation.yield(item)
            }
            continuation.finish()
        }
    }

    public func makeAsyncIterator() -> AsyncStream<Element>.AsyncIterator {
        stream.makeAsyncIterator()
    }
}
