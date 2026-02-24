/// Range scan の結果を表す AsyncSequence
///
/// バックエンドごとに異なる内部実装を持つが、統一的な AsyncSequence インターフェースを提供する。
/// InMemory 等の即時結果にも対応するコンビニエンスイニシャライザを備える。
public struct KeyValueSequence: AsyncSequence, Sendable {
    public typealias Element = (key: Bytes, value: Bytes)

    private let stream: AsyncStream<Element>

    /// AsyncStream から生成（バックエンドがストリーミングで結果を返す場合）
    public init(_ stream: AsyncStream<Element>) {
        self.stream = stream
    }

    /// 即時結果から生成（InMemory 等、全結果が即座に利用可能な場合）
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
