import StorageKit

/// Lazy range result backed by a host range request and local write overlay.
public struct CloudflareDurableObjectRangeResult: TransactionRangeResult {
    public typealias Element = (Bytes, Bytes)

    private let makeIteratorBody: @Sendable () -> Iterator

    init(scan: @escaping @Sendable () -> CloudflareDurableObjectRangeScan) {
        self.makeIteratorBody = {
            Iterator(scan: scan())
        }
    }

    public func makeAsyncIterator() -> Iterator {
        makeIteratorBody()
    }

    public struct Iterator: TransactionRangeIterator, Sendable {
        private var scan: any CloudflareDurableObjectRangeScanning

        init(scan: any CloudflareDurableObjectRangeScanning) {
            self.scan = scan
        }

        public mutating func next() async throws -> (Bytes, Bytes)? {
            try await scan.next()
        }

        public mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            try await scan.finish(isolation: actor)
        }
    }
}
