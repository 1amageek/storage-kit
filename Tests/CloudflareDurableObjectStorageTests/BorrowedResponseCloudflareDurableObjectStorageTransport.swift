import CloudflareDurableObjectStorage
import DatabaseTypes

struct BorrowedResponseCloudflareDurableObjectStorageTransport:
    CloudflareDurableObjectStorageTransport {
    let responseBytes: ByteString

    var callExecution: CloudflareDurableObjectCallExecution {
        .synchronous
    }

    func send(_ requestBytes: ByteString) async -> ByteString {
        _ = requestBytes
        return responseBytes
    }
}
