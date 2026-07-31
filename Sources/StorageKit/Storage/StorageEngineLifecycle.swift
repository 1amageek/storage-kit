import Synchronization

/// Coordinates terminal admission and asynchronous cleanup for one storage
/// engine identity.
///
/// Backend modules in this package share this implementation so Native, WASM,
/// and Embedded builds use the same shutdown state machine.
package final class StorageEngineLifecycle: Sendable {
    private enum Phase: Sendable {
        case active
        case shuttingDown(StorageEngineShutdownCompletion)
        case shutdown
    }

    private let phase = Mutex(Phase.active)

    package init() {}

    package var isShutdownRequested: Bool {
        phase.withLock { phase in
            switch phase {
            case .active:
                false
            case .shuttingDown, .shutdown:
                true
            }
        }
    }

    package func requireActive(
        backend: StorageBackend,
        operation: StorageOperation
    ) throws {
        try phase.withLock { phase in
            guard case .active = phase else {
                throw shutdownError(backend: backend, operation: operation)
            }
        }
    }

    /// Performs one synchronous admission while the lifecycle remains active.
    ///
    /// The body must not suspend. This closes the check-then-create race between
    /// transaction/resource construction and `requestShutdown()`.
    package func withActiveAdmission<Result>(
        backend: StorageBackend,
        operation: StorageOperation,
        _ body: () throws -> Result
    ) throws -> Result {
        try phase.withLock { phase in
            guard case .active = phase else {
                throw shutdownError(backend: backend, operation: operation)
            }
            return try body()
        }
    }

    private func shutdownError(
        backend: StorageBackend,
        operation: StorageOperation
    ) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: backend,
            message: "Storage engine shutdown has been requested"
        )
    }

    /// Completes shutdown synchronously for an engine without owned
    /// asynchronous resources.
    package func requestShutdown() {
        guard let completion = beginShutdown() else { return }
        finishShutdown(completion)
    }

    /// Completes shutdown synchronously after one terminal preparation.
    ///
    /// `prepare` runs outside the lifecycle lock after admission has closed and
    /// before completion becomes observable. This form is for owned resources
    /// whose release does not suspend.
    package func requestShutdown(
        prepare: @Sendable () -> Void
    ) {
        guard let completion = beginShutdown() else { return }
        prepare()
        finishShutdown(completion)
    }

    /// Starts one asynchronous cleanup operation.
    ///
    /// `prepare` runs synchronously after admission is closed. It is intended
    /// for terminal signals such as task cancellation. The cleanup closure is
    /// then executed outside the lifecycle lock and its completion is shared by
    /// every waiter.
    package func requestShutdown(
        prepare: @Sendable () -> Void = {},
        cleanup: @escaping @Sendable () async -> Void
    ) {
        guard let completion = beginShutdown() else { return }
        prepare()
        Task { [self] in
            await cleanup()
            finishShutdown(completion)
        }
    }

    package func waitUntilShutdown() async {
        let completion = phase.withLock { phase
            -> StorageEngineShutdownCompletion? in
            switch phase {
            case .active:
                preconditionFailure(
                    "Shutdown must be requested before awaiting completion"
                )
            case .shuttingDown(let completion):
                return completion
            case .shutdown:
                return nil
            }
        }
        await completion?.wait()
    }

    private func beginShutdown() -> StorageEngineShutdownCompletion? {
        phase.withLock { phase in
            guard case .active = phase else { return nil }
            let completion = StorageEngineShutdownCompletion()
            phase = .shuttingDown(completion)
            return completion
        }
    }

    private func finishShutdown(
        _ completed: StorageEngineShutdownCompletion
    ) {
        let shouldResume = phase.withLock { phase in
            guard case .shuttingDown(let current) = phase else {
                return false
            }
            precondition(
                current === completed,
                "Only the authoritative storage shutdown may complete"
            )
            phase = .shutdown
            return true
        }
        if shouldResume {
            completed.resolve()
        }
    }
}

private final class StorageEngineShutdownCompletion: Sendable {
    private struct State: Sendable {
        var isResolved = false
        var waiters: [CheckedContinuation<Void, Never>] = []
    }

    private let state = Mutex(State())

    func wait() async {
        await withCheckedContinuation { continuation in
            let resumeImmediately = state.withLock { state in
                guard !state.isResolved else { return true }
                state.waiters.append(continuation)
                return false
            }
            if resumeImmediately {
                continuation.resume()
            }
        }
    }

    func resolve() {
        let waiters = state.withLock { state in
            precondition(
                !state.isResolved,
                "Storage shutdown completion must resolve exactly once"
            )
            state.isResolved = true
            let waiters = state.waiters
            state.waiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume()
        }
    }
}
