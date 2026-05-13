import FoundationDB
import StorageKit

public struct FDBStorageRangeResult: AsyncSequence, Sendable {
    public typealias Element = (Bytes, Bytes)

    private let base: FDB.AsyncKVSequence

    init(_ base: FDB.AsyncKVSequence) {
        self.base = base
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base.makeAsyncIterator())
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        private var base: FDB.AsyncKVSequence.AsyncIterator

        init(_ base: FDB.AsyncKVSequence.AsyncIterator) {
            self.base = base
        }

        public mutating func next() async throws -> Element? {
            do {
                return try await base.next()
            } catch let error as FDBError {
                throw FDBStorageTransaction.convertFDBError(error, operation: .rangeRead)
            } catch {
                throw FDBStorageTransaction.convertBackendError(error, operation: .rangeRead)
            }
        }
    }
}
