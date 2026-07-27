import DatabaseTypes
import StorageKit

protocol CloudflareDurableObjectRangeScanning: Sendable {
    mutating func next() async throws -> (ByteString, ByteString)?
    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws
}
