#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import Foundation
import Synchronization

final class ScriptedCloudflareDurableObjectURLProtocol: URLProtocol {
    struct ResponsePlan: Sendable {
        let statusCode: Int
        let headers: [String: String]
        let chunks: [[UInt8]]
        let holdsConnectionOpen: Bool

        init(
            statusCode: Int = 200,
            headers: [String: String] = [
                "Content-Type": "application/octet-stream",
            ],
            chunks: [[UInt8]] = [],
            holdsConnectionOpen: Bool = false
        ) {
            self.statusCode = statusCode
            self.headers = headers
            self.chunks = chunks
            self.holdsConnectionOpen = holdsConnectionOpen
        }
    }

    struct CapturedRequest: Sendable {
        let method: String?
        let contentType: String?
        let accept: String?
        let authorization: String?
        let trace: String?
        let body: [UInt8]
        let bodyReadFailed: Bool
    }

    private struct State: Sendable {
        var responsePlan = ResponsePlan()
        var capturedRequests: [CapturedRequest] = []
        var stopCount = 0
    }

    private static let state = Mutex(State())

    static func reset(responsePlan: ResponsePlan = ResponsePlan()) {
        state.withLock { state in
            state = State(responsePlan: responsePlan)
        }
    }

    static var capturedRequests: [CapturedRequest] {
        state.withLock { $0.capturedRequests }
    }

    static var stopCount: Int {
        state.withLock { $0.stopCount }
    }

    override class func canInit(with request: URLRequest) -> Bool {
        true
    }

    override class func canonicalRequest(
        for request: URLRequest
    ) -> URLRequest {
        request
    }

    override func startLoading() {
        let capturedBody = captureBody()
        let capturedRequest = CapturedRequest(
            method: request.httpMethod,
            contentType: request.value(forHTTPHeaderField: "Content-Type"),
            accept: request.value(forHTTPHeaderField: "Accept"),
            authorization: request.value(
                forHTTPHeaderField: "Authorization"
            ),
            trace: request.value(forHTTPHeaderField: "X-Trace"),
            body: capturedBody.bytes,
            bodyReadFailed: capturedBody.failed
        )
        let responsePlan = Self.state.withLock { state in
            state.capturedRequests.append(capturedRequest)
            return state.responsePlan
        }
        guard let url = request.url else {
            client?.urlProtocol(
                self,
                didFailWithError: URLError(.badURL)
            )
            return
        }
        guard let response = HTTPURLResponse(
            url: url,
            statusCode: responsePlan.statusCode,
            httpVersion: "HTTP/1.1",
            headerFields: responsePlan.headers
        ) else {
            client?.urlProtocol(
                self,
                didFailWithError: URLError(.badServerResponse)
            )
            return
        }
        client?.urlProtocol(
            self,
            didReceive: response,
            cacheStoragePolicy: .notAllowed
        )
        for chunk in responsePlan.chunks {
            client?.urlProtocol(self, didLoad: Data(chunk))
        }
        if !responsePlan.holdsConnectionOpen {
            client?.urlProtocolDidFinishLoading(self)
        }
    }

    override func stopLoading() {
        Self.state.withLock { state in
            state.stopCount += 1
        }
    }

    private func captureBody() -> (bytes: [UInt8], failed: Bool) {
        if let body = request.httpBody {
            return ([UInt8](body), false)
        }
        guard let stream = request.httpBodyStream else {
            return ([], false)
        }
        stream.open()
        defer {
            stream.close()
        }
        var bytes: [UInt8] = []
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while true {
            let count = stream.read(&buffer, maxLength: buffer.count)
            if count < 0 {
                return (bytes, true)
            }
            if count == 0 {
                return (bytes, false)
            }
            bytes.append(contentsOf: buffer.prefix(count))
        }
    }
}
