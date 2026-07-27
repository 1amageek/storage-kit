import DatabaseTypes
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import Foundation
import StorageKitEmbeddedCore
import Testing
@testable import CloudflareDurableObjectStorageHTTP

@Suite("Cloudflare Durable Object HTTP Transport Tests", .serialized)
struct CloudflareDurableObjectHTTPTransportTests {
    @Test func rejectsOversizedRequestBeforeDispatch() async throws {
        ScriptedCloudflareDurableObjectURLProtocol.reset()
        let transport = try makeTransport(maximumRequestBytes: 2)

        do {
            _ = try await transport.send([0x01, 0x02, 0x03])
            Issue.record("Expected request size validation failure")
        } catch let error as CloudflareDurableObjectHTTPTransportError {
            #expect(
                error == .requestTooLarge(actual: 3, maximum: 2)
            )
        }
        #expect(
            ScriptedCloudflareDurableObjectURLProtocol.capturedRequests.isEmpty
        )
    }

    @Test func rejectsDeclaredOversizedResponse() async throws {
        ScriptedCloudflareDurableObjectURLProtocol.reset(
            responsePlan: .init(
                headers: [
                    "Content-Type": "application/octet-stream",
                    "Content-Length": "5",
                ]
            )
        )
        let transport = try makeTransport(maximumResponseBytes: 4)

        do {
            _ = try await transport.send([0x01])
            Issue.record("Expected declared response size validation failure")
        } catch let error as CloudflareDurableObjectHTTPTransportError {
            #expect(
                error == .responseTooLarge(actual: 5, maximum: 4)
            )
        }
    }

    @Test func rejectsStreamedOversizedResponse() async throws {
        ScriptedCloudflareDurableObjectURLProtocol.reset(
            responsePlan: .init(
                chunks: [
                    [0x01, 0x02, 0x03],
                    [0x04, 0x05],
                ]
            )
        )
        let transport = try makeTransport(maximumResponseBytes: 4)

        do {
            _ = try await transport.send([0x01])
            Issue.record("Expected streamed response size validation failure")
        } catch let error as CloudflareDurableObjectHTTPTransportError {
            #expect(
                error == .responseTooLarge(actual: 5, maximum: 4)
            )
        }
    }

    @Test func requiresWireResponseMediaType() async throws {
        ScriptedCloudflareDurableObjectURLProtocol.reset(
            responsePlan: .init(
                headers: ["Content-Type": "text/plain"],
                chunks: [[0x01]]
            )
        )
        let transport = try makeTransport()

        do {
            _ = try await transport.send([0x01])
            Issue.record("Expected response media type validation failure")
        } catch let error as CloudflareDurableObjectHTTPTransportError {
            #expect(
                error == .unexpectedResponseMediaType(actual: "text/plain")
            )
        }
    }

    @Test func requiresResponseContentTypeHeader() async throws {
        ScriptedCloudflareDurableObjectURLProtocol.reset(
            responsePlan: .init(headers: [:], chunks: [[0x01]])
        )
        let transport = try makeTransport()

        do {
            _ = try await transport.send([0x01])
            Issue.record("Expected missing response media type failure")
        } catch let error as CloudflareDurableObjectHTTPTransportError {
            #expect(
                error == .unexpectedResponseMediaType(actual: nil)
            )
        }
    }

    @Test func preservesProtocolHeadersAndCustomHeaders() async throws {
        ScriptedCloudflareDurableObjectURLProtocol.reset(
            responsePlan: .init(chunks: [[0x02, 0x03]])
        )
        let transport = try makeTransport(
            headers: [
                ("Authorization", "Bearer token"),
                ("X-Trace", "trace-id"),
            ],
            configurationHeaders: [
                "Content-Type": "application/json",
                "Accept": "text/plain",
            ]
        )

        let response = try await transport.send([0x01])

        #expect(response == [0x02, 0x03])
        let captured = try #require(
            ScriptedCloudflareDurableObjectURLProtocol.capturedRequests.first
        )
        #expect(captured.method == "POST")
        #expect(captured.contentType == "application/octet-stream")
        #expect(captured.accept == "application/octet-stream")
        #expect(captured.authorization == "Bearer token")
        #expect(captured.trace == "trace-id")
        #expect(!captured.bodyReadFailed)
        #expect(captured.body == [0x01])
    }

    @Test func rejectsCustomProtocolHeaderOverrides() throws {
        do {
            _ = try makeTransport(
                headers: [("cOnTeNt-TyPe", "application/json")]
            )
            Issue.record("Expected reserved Content-Type header failure")
        } catch let error as CloudflareDurableObjectHTTPTransportError {
            #expect(error == .reservedHeader(name: "cOnTeNt-TyPe"))
        }

        do {
            _ = try makeTransport(headers: [("ACCEPT", "text/plain")])
            Issue.record("Expected reserved Accept header failure")
        } catch let error as CloudflareDurableObjectHTTPTransportError {
            #expect(error == .reservedHeader(name: "ACCEPT"))
        }
    }

    @Test func cancellationBeforeTaskRegistrationCompletesPromptly() async throws {
        ScriptedCloudflareDurableObjectURLProtocol.reset(
            responsePlan: .init(holdsConnectionOpen: true)
        )
        let transport = try makeTransport()

        let result: Result<ByteString, any Error> = await Task {
            withUnsafeCurrentTask { task in
                task?.cancel()
            }
            do {
                return .success(try await transport.send([0x01]))
            } catch {
                return .failure(error)
            }
        }.value

        switch result {
        case .success:
            Issue.record("Expected cancellation")
        case .failure(let error):
            #expect(error is CancellationError)
        }
        #expect(
            ScriptedCloudflareDurableObjectURLProtocol.capturedRequests.isEmpty
        )
    }

    @Test func cancellationStopsActiveRequest() async throws {
        ScriptedCloudflareDurableObjectURLProtocol.reset(
            responsePlan: .init(holdsConnectionOpen: true)
        )
        let transport = try makeTransport()
        let task = Task {
            try await transport.send([0x01])
        }

        for _ in 0..<10_000 {
            if !ScriptedCloudflareDurableObjectURLProtocol
                .capturedRequests.isEmpty {
                break
            }
            await Task.yield()
        }
        #expect(
            !ScriptedCloudflareDurableObjectURLProtocol.capturedRequests.isEmpty
        )
        task.cancel()

        do {
            _ = try await task.value
            Issue.record("Expected cancellation")
        } catch {
            #expect(error is CancellationError)
        }
        for _ in 0..<10_000 {
            if ScriptedCloudflareDurableObjectURLProtocol.stopCount > 0 {
                break
            }
            await Task.yield()
        }
        #expect(ScriptedCloudflareDurableObjectURLProtocol.stopCount > 0)
    }

    private func makeTransport(
        headers: [(String, String)] = [],
        configurationHeaders: [String: String] = [:],
        maximumRequestBytes: Int = 1_024,
        maximumResponseBytes: Int = 1_024
    ) throws -> CloudflareDurableObjectHTTPTransport {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.httpAdditionalHeaders = configurationHeaders
        configuration.protocolClasses = [
            ScriptedCloudflareDurableObjectURLProtocol.self,
        ]
        let endpoint = try #require(
            URL(string: "https://storage-kit.invalid/execute")
        )
        return try CloudflareDurableObjectHTTPTransport(
            endpoint: endpoint,
            headers: headers,
            maximumRequestBytes: maximumRequestBytes,
            maximumResponseBytes: maximumResponseBytes,
            sessionConfiguration: configuration
        )
    }
}
