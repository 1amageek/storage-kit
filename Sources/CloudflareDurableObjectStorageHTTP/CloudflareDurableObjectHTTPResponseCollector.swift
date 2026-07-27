import DatabaseTypes
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import Foundation
import CloudflareDurableObjectStorageWire
import Synchronization

final class CloudflareDurableObjectHTTPResponseCollector:
    NSObject,
    URLSessionDataDelegate,
    Sendable {
    private struct State: Sendable {
        var body = CloudflareDurableObjectHTTPResponseBody()
        var response: URLResponse?
        var continuation:
            CheckedContinuation<(ByteString, URLResponse), any Error>?
        var session: URLSession?
        var task: URLSessionDataTask?
        var cancellationRequested = false
        var completed = false
    }

    private let maximumResponseBytes: Int
    private let configuration: URLSessionConfiguration
    private let state = Mutex(State())

    init(
        maximumResponseBytes: Int,
        configuration: URLSessionConfiguration
    ) {
        self.maximumResponseBytes = maximumResponseBytes
        self.configuration = configuration
    }

    func data(
        for request: URLRequest
    ) async throws -> (ByteString, URLResponse) {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let session = URLSession(
                    configuration: configuration,
                    delegate: self,
                    delegateQueue: nil
                )
                let task = session.dataTask(with: request)
                let cancellationRequested = state.withLock { state in
                    state.continuation = continuation
                    state.session = session
                    state.task = task
                    return state.cancellationRequested
                }
                if cancellationRequested {
                    finish(
                        .failure(CancellationError()),
                        cancelSession: true
                    )
                    return
                }
                task.resume()
            }
        } onCancel: {
            self.cancel()
        }
    }

    func urlSession(
        _ session: URLSession,
        dataTask: URLSessionDataTask,
        didReceive response: URLResponse,
        completionHandler: @escaping @Sendable (
            URLSession.ResponseDisposition
        ) -> Void
    ) {
        let expected = response.expectedContentLength
        guard expected <= Int64(maximumResponseBytes) else {
            completionHandler(.cancel)
            finish(
                .failure(
                    CloudflareDurableObjectHTTPTransportError.responseTooLarge(
                        actual: expected,
                        maximum: maximumResponseBytes
                    )
                ),
                cancelSession: true
            )
            return
        }
        let shouldAllow = state.withLock { state in
            guard !state.completed else {
                return false
            }
            state.response = response
            return true
        }
        completionHandler(shouldAllow ? .allow : .cancel)
    }

    func urlSession(
        _ session: URLSession,
        dataTask: URLSessionDataTask,
        didReceive data: Data
    ) {
        let overflow: Int64? = state.withLock { state in
            guard !state.completed else {
                return nil
            }
            guard state.body.append(
                data,
                maximumBytes: maximumResponseBytes
            ) else {
                return Int64(state.body.byteCount) + Int64(data.count)
            }
            return nil
        }
        guard let overflow else {
            return
        }
        dataTask.cancel()
        finish(
            .failure(
                CloudflareDurableObjectHTTPTransportError.responseTooLarge(
                    actual: overflow,
                    maximum: maximumResponseBytes
                )
            ),
            cancelSession: true
        )
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: (any Error)?
    ) {
        if let error {
            finish(.failure(error), cancelSession: false)
            return
        }
        let result: Result<(ByteString, URLResponse), any Error> =
            state.withLock { state in
                guard let response = state.response else {
                    return .failure(
                        CloudflareDurableObjectHTTPTransportError
                            .missingResponse
                    )
                }
                return .success((state.body.bytes(), response))
            }
        finish(result, cancelSession: false)
    }

    private func cancel() {
        let hasRegisteredContinuation = state.withLock { state in
            state.cancellationRequested = true
            return state.continuation != nil
        }
        guard hasRegisteredContinuation else {
            return
        }
        finish(.failure(CancellationError()), cancelSession: true)
    }

    private func finish(
        _ result: Result<(ByteString, URLResponse), any Error>,
        cancelSession: Bool
    ) {
        let completion = state.withLock { state -> (
            CheckedContinuation<(ByteString, URLResponse), any Error>?,
            URLSession?
        ) in
            guard !state.completed, let continuation = state.continuation else {
                return (nil, nil)
            }
            state.completed = true
            let session = state.session
            state.continuation = nil
            state.session = nil
            state.task = nil
            state.body = CloudflareDurableObjectHTTPResponseBody()
            return (continuation, session)
        }
        guard let continuation = completion.0 else {
            return
        }
        if cancelSession {
            completion.1?.invalidateAndCancel()
        } else {
            completion.1?.finishTasksAndInvalidate()
        }
        continuation.resume(with: result)
    }
}
