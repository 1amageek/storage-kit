import StorageKit

protocol CloudflareDurableObjectRangeScanning: Sendable {
    mutating func next() async throws -> (Bytes, Bytes)?
}
