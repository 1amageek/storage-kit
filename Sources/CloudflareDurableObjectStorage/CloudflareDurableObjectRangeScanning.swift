import StorageKit

protocol CloudflareDurableObjectRangeScanning: Sendable {
    mutating func next() async throws -> (Bytes, Bytes)?
    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws
}
