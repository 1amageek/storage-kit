import StorageKit
import Synchronization

/// Completes one suspending Durable Object call at its operation result,
/// configured deadline, or caller cancellation boundary.
final class CloudflareDurableObjectTimedCall<Value: Sendable>: Sendable {
    private struct State: Sendable {
        var continuation: CheckedContinuation<Value, any Error>?
        var operationTask: Task<Void, Never>?
        var timerTask: Task<Void, Never>?
        var isFinished = false
        var cancellationRequested = false
    }

    private let state = Mutex(State())

    func execute(
        until deadline: ContinuousClock.Instant,
        clock: any StorageMonotonicClock,
        timeoutError: @escaping @Sendable () -> any Error,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        try await withTaskCancellationHandler {
            try Task.checkCancellation()
            return try await withCheckedThrowingContinuation { continuation in
                let shouldStart = state.withLock { state -> Bool in
                    guard !state.cancellationRequested else {
                        state.isFinished = true
                        return false
                    }
                    state.continuation = continuation
                    return true
                }
                guard shouldStart else {
                    continuation.resume(throwing: CancellationError())
                    return
                }

                let operationTask = Task { [self] in
                    do {
                        finish(.success(try await operation()))
                    } catch {
                        finish(.failure(error))
                    }
                }
                installOperationTask(operationTask)

                let timerTask = Task { [self] in
                    do {
                        try await clock.sleep(until: deadline)
                    } catch is CancellationError {
                        return
                    } catch {
                        finish(.failure(error))
                        return
                    }
                    finish(.failure(timeoutError()))
                }
                installTimerTask(timerTask)
            }
        } onCancel: {
            cancel()
        }
    }

    private func installOperationTask(_ task: Task<Void, Never>) {
        let shouldCancel = state.withLock { state -> Bool in
            guard !state.isFinished else { return true }
            state.operationTask = task
            return false
        }
        if shouldCancel {
            task.cancel()
        }
    }

    private func installTimerTask(_ task: Task<Void, Never>) {
        let shouldCancel = state.withLock { state -> Bool in
            guard !state.isFinished else { return true }
            state.timerTask = task
            return false
        }
        if shouldCancel {
            task.cancel()
        }
    }

    private func finish(_ result: Result<Value, any Error>) {
        let completion = state.withLock { state -> (
            CheckedContinuation<Value, any Error>,
            Task<Void, Never>?,
            Task<Void, Never>?
        )? in
            guard !state.isFinished,
                  let continuation = state.continuation else {
                return nil
            }
            state.isFinished = true
            state.continuation = nil
            let operationTask = state.operationTask
            let timerTask = state.timerTask
            state.operationTask = nil
            state.timerTask = nil
            return (continuation, operationTask, timerTask)
        }
        guard let completion else { return }
        completion.1?.cancel()
        completion.2?.cancel()
        completion.0.resume(with: result)
    }

    private func cancel() {
        let completion = state.withLock { state -> (
            CheckedContinuation<Value, any Error>,
            Task<Void, Never>?,
            Task<Void, Never>?
        )? in
            guard !state.isFinished else { return nil }
            guard let continuation = state.continuation else {
                state.cancellationRequested = true
                return nil
            }
            state.isFinished = true
            state.continuation = nil
            let operationTask = state.operationTask
            let timerTask = state.timerTask
            state.operationTask = nil
            state.timerTask = nil
            return (continuation, operationTask, timerTask)
        }
        guard let completion else { return }
        completion.1?.cancel()
        completion.2?.cancel()
        completion.0.resume(throwing: CancellationError())
    }
}
