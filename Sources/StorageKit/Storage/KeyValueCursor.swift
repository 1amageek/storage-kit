import DatabaseTypes
import Synchronization

/// Type-erased, zero-copy cursor used at a transaction-access boundary.
///
/// The actor owns the backend iterator so `next()` calls remain ordered across
/// suspension points. Key and value buffers are returned unchanged; this type
/// erases only control flow and never materializes payload bytes.
public struct KeyValueCursor: Sendable {
    public typealias Element = (ByteString, ByteString)

    private let state: any KeyValueCursorState

    /// Erases a backend range result while retaining its native buffers.
    public init<Result: TransactionRangeResult>(
        consuming result: sending Result
    ) {
        self.state = TypedKeyValueCursorState(result: result)
    }

    /// Advances this single-consumer cursor.
    public mutating func next() async throws -> Element? {
        try await state.next()
    }

    /// Closes the backend iterator and awaits all native cleanup.
    public mutating func finish() async throws {
        try await state.finish()
    }

    /// Consumes the cursor and makes backend cleanup authoritative.
    public mutating func consume(
        _ body: (ByteString, ByteString) async throws -> Void
    ) async throws {
        do {
            while let (key, value) = try await next() {
                try await body(key, value)
            }
        } catch {
            let iterationError = error
            do {
                try await finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }
        try await finish()
    }
}

private protocol KeyValueCursorState: Actor {
    func next() async throws -> KeyValueCursor.Element?
    func finish() async throws
}

private actor TypedKeyValueCursorState<Result: TransactionRangeResult>:
    KeyValueCursorState {
    private enum State {
        case unopened(Result)
        case ready(Result.Cursor)
        case advancing(CursorAdvanceBoundary)
        case finishing(CursorFinishBoundary)
        case finished((any Error)?)
    }

    private var state: State

    init(result: sending Result) {
        self.state = .unopened(result)
    }

    func next() async throws -> KeyValueCursor.Element? {
        try ensureStorageTaskIsActive()

        var cursor: Result.Cursor
        switch state {
        case .unopened(let result):
            cursor = result.makeCursor()
        case .ready(let storedCursor):
            cursor = storedCursor
        case .advancing:
            throw concurrentOperationError("Concurrent cursor advance")
        case .finishing:
            throw concurrentOperationError("Cursor cleanup is in progress")
        case .finished:
            return nil
        }

        let boundary = CursorAdvanceBoundary()
        state = .advancing(boundary)

        let element: KeyValueCursor.Element?
        do {
            element = try await cursor.next()
        } catch {
            let iterationError = error
            do {
                try await cursor.finish(isolation: self)
                state = .finished(nil)
                boundary.resolve(.success(()))
            } catch {
                state = .finished(error)
                boundary.resolve(.failure(error))
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            throw iterationError
        }

        if element == nil || boundary.isFinishRequested {
            do {
                try await cursor.finish(isolation: self)
                state = .finished(nil)
                boundary.resolve(.success(()))
            } catch {
                state = .finished(error)
                boundary.resolve(.failure(error))
                throw error
            }
            return element
        }

        state = .ready(cursor)
        boundary.resolve(.success(()))
        return element
    }

    func finish() async throws {
        switch state {
        case .unopened:
            state = .finished(nil)
            return
        case .ready(var cursor):
            let boundary = CursorFinishBoundary()
            state = .finishing(boundary)
            do {
                try await cursor.finish(isolation: self)
                state = .finished(nil)
                boundary.resolve(.success(()))
            } catch {
                state = .finished(error)
                boundary.resolve(.failure(error))
                throw error
            }
        case .advancing(let boundary):
            try await boundary.requestFinish()
        case .finishing(let boundary):
            try await boundary.wait()
        case .finished(let error):
            if let error {
                throw error
            }
        }
    }

    private func concurrentOperationError(_ message: String) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: .rangeRead,
            message: message
        )
    }
}

private final class CursorAdvanceBoundary: Sendable {
    private struct State: Sendable {
        var finishRequested = false
        var result: Result<Void, CursorBoundaryFailure>?
        var waiters: [
            CheckedContinuation<Result<Void, CursorBoundaryFailure>, Never>
        ] = []
    }

    private let state = Mutex(State())

    var isFinishRequested: Bool {
        state.withLock { $0.finishRequested }
    }

    func requestFinish() async throws {
        let result = await withCheckedContinuation { continuation in
            let completed = state.withLock { state
                -> Result<Void, CursorBoundaryFailure>? in
                state.finishRequested = true
                if let result = state.result {
                    return result
                }
                state.waiters.append(continuation)
                return nil
            }
            if let completed {
                continuation.resume(returning: completed)
            }
        }
        switch result {
        case .success:
            return
        case .failure(let failure):
            throw failure.underlying
        }
    }

    func resolve(_ result: Result<Void, any Error>) {
        let normalized: Result<Void, CursorBoundaryFailure>
        switch result {
        case .success:
            normalized = .success(())
        case .failure(let error):
            normalized = .failure(CursorBoundaryFailure(error))
        }
        let waiters = state.withLock { state in
            precondition(state.result == nil)
            state.result = normalized
            let waiters = state.waiters
            state.waiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume(returning: normalized)
        }
    }
}

private final class CursorFinishBoundary: Sendable {
    private struct State: Sendable {
        var result: Result<Void, CursorBoundaryFailure>?
        var waiters: [
            CheckedContinuation<Result<Void, CursorBoundaryFailure>, Never>
        ] = []
    }

    private let state = Mutex(State())

    func wait() async throws {
        let result = await withCheckedContinuation { continuation in
            let completed = state.withLock { state
                -> Result<Void, CursorBoundaryFailure>? in
                if let result = state.result {
                    return result
                }
                state.waiters.append(continuation)
                return nil
            }
            if let completed {
                continuation.resume(returning: completed)
            }
        }
        switch result {
        case .success:
            return
        case .failure(let failure):
            throw failure.underlying
        }
    }

    func resolve(_ result: Result<Void, any Error>) {
        let normalized: Result<Void, CursorBoundaryFailure>
        switch result {
        case .success:
            normalized = .success(())
        case .failure(let error):
            normalized = .failure(CursorBoundaryFailure(error))
        }
        let waiters = state.withLock { state in
            precondition(state.result == nil)
            state.result = normalized
            let waiters = state.waiters
            state.waiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume(returning: normalized)
        }
    }
}

private struct CursorBoundaryFailure: Error, Sendable {
    let underlying: any Error

    init(_ underlying: any Error) {
        self.underlying = underlying
    }
}
