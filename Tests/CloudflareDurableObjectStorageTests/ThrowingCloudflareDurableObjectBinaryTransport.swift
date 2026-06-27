import CloudflareDurableObjectStorage
import StorageKit

final class ThrowingCloudflareDurableObjectBinaryTransport: CloudflareDurableObjectBinaryTransport, Sendable {
    private let error: StorageError

    init(error: StorageError) {
        self.error = error
    }

    func send(_ requestBytes: [UInt8]) async throws -> [UInt8] {
        throw error
    }
}
