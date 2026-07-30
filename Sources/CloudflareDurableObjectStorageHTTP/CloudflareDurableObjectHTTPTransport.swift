import DatabaseTypes
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import CloudflareDurableObjectStorage
import Foundation
import StorageKit
import CloudflareDurableObjectStorageWire

/// URLSession-backed StorageKit Wire transport for a Durable Object endpoint.
public struct CloudflareDurableObjectHTTPTransport: CloudflareDurableObjectStorageTransport {
    public var callExecution: CloudflareDurableObjectCallExecution {
        .suspending
    }

    public static let defaultMaximumFrameBytes = 16 * 1_024 * 1_024
    private static let wireMediaType = "application/octet-stream"

    public let endpoint: URL
    public let headers: [(String, String)]
    public let timeoutInterval: TimeInterval
    public let maximumRequestBytes: Int
    public let maximumResponseBytes: Int
    private let sessionConfiguration: URLSessionConfiguration

    public init(
        endpoint: URL,
        headers: [(String, String)] = [],
        timeoutInterval: TimeInterval = 30,
        maximumRequestBytes: Int = Self.defaultMaximumFrameBytes,
        maximumResponseBytes: Int = Self.defaultMaximumFrameBytes,
        sessionConfiguration: URLSessionConfiguration = .ephemeral
    ) throws {
        guard maximumRequestBytes > 0, maximumResponseBytes > 0 else {
            throw CloudflareDurableObjectHTTPTransportError.invalidLimit
        }
        guard maximumRequestBytes <= Self.defaultMaximumFrameBytes else {
            throw CloudflareDurableObjectHTTPTransportError
                .limitExceedsProtocolMaximum(
                    actual: maximumRequestBytes,
                    maximum: Self.defaultMaximumFrameBytes
                )
        }
        guard maximumResponseBytes <= Self.defaultMaximumFrameBytes else {
            throw CloudflareDurableObjectHTTPTransportError
                .limitExceedsProtocolMaximum(
                    actual: maximumResponseBytes,
                    maximum: Self.defaultMaximumFrameBytes
                )
        }
        for (name, _) in headers where Self.isReservedHeader(name) {
            throw CloudflareDurableObjectHTTPTransportError.reservedHeader(
                name: name
            )
        }
        self.endpoint = endpoint
        self.headers = headers
        self.timeoutInterval = timeoutInterval
        self.maximumRequestBytes = maximumRequestBytes
        self.maximumResponseBytes = maximumResponseBytes
        self.sessionConfiguration = sessionConfiguration
    }

    public func send(
        _ requestBytes: ByteString
    ) async throws(StorageTransportError) -> ByteString {
        guard requestBytes.count <= maximumRequestBytes else {
            throw .rejected(
                stage: CloudflareDurableObjectHTTPTransportError.requestTooLarge(
                    actual: requestBytes.count,
                    maximum: maximumRequestBytes
                ).failureStage
            )
        }
        var request = URLRequest(url: endpoint, timeoutInterval: timeoutInterval)
        request.httpMethod = "POST"
        request.httpBody = requestBytes.withUnsafeBytes { Data($0) }
        for (name, value) in headers {
            request.setValue(value, forHTTPHeaderField: name)
        }
        request.setValue(
            Self.wireMediaType,
            forHTTPHeaderField: "Content-Type"
        )
        request.setValue(Self.wireMediaType, forHTTPHeaderField: "Accept")

        do {
            let collector = CloudflareDurableObjectHTTPResponseCollector(
                maximumResponseBytes: maximumResponseBytes,
                configuration: sessionConfiguration
            )
            let (bytes, response) = try await collector.data(for: request)
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
            let responseContentType = httpResponse.value(
                forHTTPHeaderField: "Content-Type"
            )
            guard Self.isWireMediaType(responseContentType) else {
                throw CloudflareDurableObjectHTTPTransportError
                    .unexpectedResponseMediaType(
                        actual: responseContentType
                    )
            }
            return bytes
        } catch is CancellationError {
            throw .cancelled
        } catch let error as CloudflareDurableObjectHTTPTransportError {
            throw .rejected(stage: error.failureStage)
        } catch let error as StorageError {
            throw .storage(error)
        } catch {
            throw .rejected(
                stage: .afterDispatch
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

    private static func isReservedHeader(_ name: String) -> Bool {
        name.caseInsensitiveCompare("Content-Type") == .orderedSame
            || name.caseInsensitiveCompare("Accept") == .orderedSame
    }

    private static func isWireMediaType(_ value: String?) -> Bool {
        guard let value else {
            return false
        }
        return value.trimmingCharacters(in: .whitespaces)
            .caseInsensitiveCompare(wireMediaType) == .orderedSame
    }
}
