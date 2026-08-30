import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire
import DatabaseTypes
import StorageKit

/// Serves every request from a real in-memory backend except `commit`, which
/// fails after dispatch with a connection failure.
///
/// This is the shape that produces an unknown commit outcome: the host may
/// already have applied the mutations, so the client reports
/// `commitUnknownResult` and the transaction can only be resolved by an
/// idempotent retry.
final class CommitFailingCloudflareDurableObjectStorageTransport:
    CloudflareDurableObjectStorageTransport, Sendable
{
    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    private let backend = InMemoryCloudflareDurableObjectStorageTransport()

    func send(
        _ requestBytes: ByteString
    ) async throws(StorageTransportError) -> ByteString {
        let request = try decodeStorageTransportRequest(requestBytes)
        guard case .commit = request else {
            return try await backend.send(requestBytes)
        }
        throw .storage(
            StorageError(
                code: .connectionFailure,
                operation: .commit,
                backend: .cloudflareDurableObject,
                message: "Test transport lost the connection after dispatching the commit"
            )
        )
    }
}
