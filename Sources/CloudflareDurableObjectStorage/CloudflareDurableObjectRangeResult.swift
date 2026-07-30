import DatabaseTypes
import StorageKit

/// Lazy range result backed by a host range request and local write overlay.
public struct CloudflareDurableObjectRangeResult: TransactionRangeResult {
    public typealias Element = (ByteString, ByteString)

    private let makeCursorBody: @Sendable () -> Cursor

    init(scan: @escaping @Sendable () -> CloudflareDurableObjectRangeScan) {
        self.makeCursorBody = {
            Cursor(scan: scan())
        }
    }

    public func makeCursor() -> Cursor {
        makeCursorBody()
    }

    public struct Cursor: TransactionRangeCursor, Sendable {
        private var scan: any CloudflareDurableObjectRangeScanning

        init(scan: any CloudflareDurableObjectRangeScanning) {
            self.scan = scan
        }

        public mutating func next() async throws -> (ByteString, ByteString)? {
            try await scan.next()
        }

        public mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            try await scan.finish(isolation: actor)
        }
    }
}
