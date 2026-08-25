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
    private let lifetime: KeyValueCursorLifetime
    private let validateScope: (@Sendable () throws -> Void)?

    /// Erases a backend range result while retaining its native buffers.
    public init<Result: TransactionRangeResult>(
        consuming result: sending Result
    ) {
        let lifetime = KeyValueCursorLifetime()
        self.state = TypedKeyValueCursorState(
            result: result,
            lifetime: lifetime
        )
        self.lifetime = lifetime
        self.validateScope = nil
    }

    /// Returns an unopened cursor whose first advance is governed entirely by
    /// `validateScope`.
    ///
    /// This is used by nonthrowing transaction cursor factories when their
    /// caller-owned read scope has already ended. It creates no backend cursor;
    /// the validation failure is reported by `next()` through the ordinary
    /// cursor error contract.
    public init(
        validatingScope validateScope: @escaping @Sendable () throws -> Void
    ) {
        let lifetime = KeyValueCursorLifetime()
        self.state = TypedKeyValueCursorState(
            result: EmptyTransactionRangeResult(),
            lifetime: lifetime
        )
        self.lifetime = lifetime
        self.validateScope = validateScope
    }

    private init(
        state: any KeyValueCursorState,
        lifetime: KeyValueCursorLifetime,
        validateScope: @escaping @Sendable () throws -> Void
    ) {
        self.state = state
        self.lifetime = lifetime
        self.validateScope = validateScope
    }

    /// Returns a cursor that keeps `owner` alive until terminal cursor cleanup.
    ///
    /// This changes only the control-flow lifetime. Backend-owned key and value
    /// buffers continue through the same cursor state without materialization.
    public consuming func retainingLifetime<Owner: Sendable>(
        of owner: consuming Owner
    ) -> KeyValueCursor {
        lifetime.retain(owner)
        return self
    }

    /// Restricts iteration to a caller-owned scope without adding another
    /// cursor state or materializing backend-owned key/value buffers.
    ///
    /// Validation runs before and after every advance. A validation failure is
    /// terminal and awaits the same backend cursor's cleanup before escaping.
    /// Explicit `finish()` remains valid after the scope ends because cleanup
    /// is not a read capability.
    public consuming func validatingScope(
        _ validate: @escaping @Sendable () throws -> Void
    ) -> KeyValueCursor {
        let combinedValidation: @Sendable () throws -> Void
        if let existingValidation = validateScope {
            combinedValidation = {
                try existingValidation()
                try validate()
            }
        } else {
            combinedValidation = validate
        }
        return KeyValueCursor(
            state: state,
            lifetime: lifetime,
            validateScope: combinedValidation
        )
    }

    /// Advances this single-consumer cursor.
    ///
    /// Cancellation is terminal. A cursor that has started iteration closes its
    /// backend iterator before cancellation escapes to the caller.
    public mutating func next() async throws -> Element? {
        if let validateScope {
            do {
                try validateScope()
            } catch {
                return try await finish(afterScopeFailure: error)
            }
        }

        let element = try await state.next()

        if let validateScope {
            do {
                try validateScope()
            } catch {
                return try await finish(afterScopeFailure: error)
            }
        }
        return element
    }

    /// Closes the backend iterator and awaits all native cleanup.
    public mutating func finish() async throws {
        try await state.finish()
    }

    private func finish(afterScopeFailure scopeError: any Error) async throws
        -> Element? {
        do {
            try await state.finish()
        } catch {
            throw StorageRangeCleanupError(
                iterationError: scopeError,
                cleanupError: error
            )
        }
        throw scopeError
    }

    /// Consumes the cursor and makes backend cleanup authoritative.
    public mutating func consume(
        _ body: (ByteString, ByteString) async throws -> Void
    ) async throws {
        while true {
            let element: Element?
            do {
                element = try await next()
            } catch let cleanupError as StorageRangeCleanupError {
                // `next()` has already performed terminal cleanup and preserved
                // both failures. Do not wrap the same cleanup failure again.
                throw cleanupError
            } catch let cleanupError as StorageRangeTerminalCleanupError {
                // `next()` reached a terminal state before automatic cleanup
                // failed. There is no iteration failure to combine with it.
                throw cleanupError
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

            guard let (key, value) = element else { break }
            do {
                try await body(key, value)
            } catch {
                let bodyError = error
                do {
                    try await finish()
                } catch {
                    throw StorageRangeCleanupError(
                        iterationError: bodyError,
                        cleanupError: error
                    )
                }
                throw bodyError
            }
        }
        try await finish()
    }
}

private struct EmptyTransactionRangeResult: TransactionRangeResult {
    func makeCursor() -> Cursor {
        Cursor()
    }

    struct Cursor: TransactionRangeCursor {
        mutating func next() async throws -> (ByteString, ByteString)? {
            nil
        }

        mutating func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {}
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
    private let lifetime: KeyValueCursorLifetime

    init(
        result: sending Result,
        lifetime: KeyValueCursorLifetime
    ) {
        self.state = .unopened(result)
        self.lifetime = lifetime
    }

    deinit {
        // Releasing the backend result/cursor first preserves its synchronous
        // abandonment cleanup before any attached lifetime owner can disappear.
        state = .finished(nil)
        lifetime.end()
    }

    func next() async throws -> KeyValueCursor.Element? {
        var cursor: Result.Cursor
        switch state {
        case .unopened(let result):
            do {
                try ensureStorageTaskIsActive()
            } catch {
                state = .finished(nil)
                lifetime.end()
                throw error
            }
            cursor = result.makeCursor()
        case .ready(let storedCursor):
            cursor = storedCursor
            do {
                try ensureStorageTaskIsActive()
            } catch {
                try await finishReadyCursor(
                    &cursor,
                    after: error
                )
            }
        case .advancing:
            throw concurrentOperationError("Concurrent cursor advance")
        case .finishing:
            throw concurrentOperationError("Cursor cleanup is in progress")
        case .finished:
            lifetime.end()
            return nil
        }

        let boundary = CursorAdvanceBoundary()
        state = .advancing(boundary)

        let element: KeyValueCursor.Element?
        do {
            element = try await cursor.next()
            try ensureStorageTaskIsActive()
        } catch {
            let iterationError = error
            do {
                try await cursor.finish(isolation: self)
                state = .finished(nil)
                lifetime.end()
                boundary.resolve(.success(()))
            } catch {
                state = .finished(error)
                lifetime.end()
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
                lifetime.end()
                boundary.resolve(.success(()))
            } catch {
                state = .finished(error)
                lifetime.end()
                boundary.resolve(.failure(error))
                throw StorageRangeTerminalCleanupError(cleanupError: error)
            }
            return element
        }

        state = .ready(cursor)
        boundary.resolve(.success(()))
        return element
    }

    private func finishReadyCursor(
        _ cursor: inout Result.Cursor,
        after iterationError: any Error
    ) async throws -> Never {
        let boundary = CursorFinishBoundary()
        state = .finishing(boundary)
        do {
            try await cursor.finish(isolation: self)
            state = .finished(nil)
            lifetime.end()
            boundary.resolve(.success(()))
        } catch {
            state = .finished(error)
            lifetime.end()
            boundary.resolve(.failure(error))
            throw StorageRangeCleanupError(
                iterationError: iterationError,
                cleanupError: error
            )
        }
        throw iterationError
    }

    func finish() async throws {
        switch state {
        case .unopened:
            state = .finished(nil)
            lifetime.end()
            return
        case .ready(var cursor):
            let boundary = CursorFinishBoundary()
            state = .finishing(boundary)
            do {
                try await cursor.finish(isolation: self)
                state = .finished(nil)
                lifetime.end()
                boundary.resolve(.success(()))
            } catch {
                state = .finished(error)
                lifetime.end()
                boundary.resolve(.failure(error))
                throw error
            }
        case .advancing(let boundary):
            try await boundary.requestFinish()
        case .finishing(let boundary):
            try await boundary.wait()
        case .finished(let error):
            lifetime.end()
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

private final class KeyValueCursorLifetime: Sendable {
    private struct State: Sendable {
        var isEnded = false
        var owners: [any KeyValueCursorLifetimeOwner] = []
    }

    private let state = Mutex(State())

    func retain<Owner: Sendable>(_ owner: consuming Owner) {
        let retainedOwner = RetainedKeyValueCursorLifetimeOwner(owner: owner)
        let wasRetained = state.withLock { state in
            guard !state.isEnded else { return false }
            state.owners.append(retainedOwner)
            return true
        }
        if !wasRetained {
            // Keep rejection-side destruction outside the lifecycle lock. The
            // owner's deinit may re-enter a database lifecycle.
            withExtendedLifetime(retainedOwner) {}
        }
    }

    func end() {
        let owners = state.withLock { state in
            guard !state.isEnded else {
                return [any KeyValueCursorLifetimeOwner]()
            }
            state.isEnded = true
            let owners = state.owners
            state.owners.removeAll(keepingCapacity: false)
            return owners
        }
        _ = owners
    }
}

private protocol KeyValueCursorLifetimeOwner: Sendable {}

private final class RetainedKeyValueCursorLifetimeOwner<Owner: Sendable>:
    KeyValueCursorLifetimeOwner {
    private let owner: Owner

    init(owner: consuming Owner) {
        self.owner = owner
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
