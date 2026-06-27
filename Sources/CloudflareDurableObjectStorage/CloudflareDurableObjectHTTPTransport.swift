#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import Foundation
import StorageKit

/// URLSession-backed binary transport for a Durable Object HTTP endpoint.
public struct CloudflareDurableObjectHTTPTransport: CloudflareDurableObjectBinaryTransport {
    public let endpoint: URL
    public let headers: [(String, String)]
    public let timeoutInterval: TimeInterval

    public init(
        endpoint: URL,
        headers: [(String, String)] = [],
        timeoutInterval: TimeInterval = 30
    ) {
        self.endpoint = endpoint
        self.headers = headers
        self.timeoutInterval = timeoutInterval
    }

    public func send(_ requestBytes: [UInt8]) async throws -> [UInt8] {
        var request = URLRequest(url: endpoint, timeoutInterval: timeoutInterval)
        request.httpMethod = "POST"
        request.httpBody = Data(requestBytes)
        request.setValue("application/octet-stream", forHTTPHeaderField: "content-type")
        request.setValue("application/octet-stream", forHTTPHeaderField: "accept")
        for (name, value) in headers {
            request.setValue(value, forHTTPHeaderField: name)
        }

        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let httpResponse = response as? HTTPURLResponse else {
                throw StorageError(
                    code: .backendFailure,
                    operation: .execute,
                    backend: .cloudflareDurableObject,
                    message: "Cloudflare Durable Object transport returned a non-HTTP response"
                )
            }
            guard (200..<300).contains(httpResponse.statusCode) else {
                throw StorageError(
                    code: statusCode(httpResponse.statusCode),
                    operation: .execute,
                    backend: .cloudflareDurableObject,
                    message: "Cloudflare Durable Object transport returned HTTP \(httpResponse.statusCode)"
                )
            }
            return Array(data)
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as StorageError {
            throw error
        } catch {
            throw StorageError(
                code: .connectionFailure,
                operation: .execute,
                backend: .cloudflareDurableObject,
                message: "Cloudflare Durable Object HTTP transport failed",
                underlyingDescription: String(describing: error)
            )
        }
    }

    private func statusCode(_ value: Int) -> StorageError.Code {
        switch value {
        case 409:
            return .transactionConflict
        case 408, 425, 429, 500, 502, 503, 504:
            return .connectionFailure
        case 400..<500:
            return .invalidOperation
        default:
            return .backendFailure
        }
    }
}
