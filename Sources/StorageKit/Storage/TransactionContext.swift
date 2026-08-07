import Synchronization

/// Task-local lookup for the transaction owned by the current operation.
///
/// Prevents nested transaction deadlocks in single-connection backends (SQLite).
/// When a transaction is active on the current Task, engines can route nested
/// calls without acquiring an incompatible second transaction.
///
/// ## How it works
///
/// 1. The lifecycle owner calls `withActiveTransaction(_:operation:)`
/// 2. `withTransaction()` checks the TaskLocal before creating a new transaction
/// 3. Backend-specific `createTransaction()` implementations may return a
///    nested child transaction that composes with the active parent
///
/// ## Thread-safety
///
/// `@TaskLocal` values are inherited by unstructured child tasks. The binding
/// therefore owns an explicit dynamic lifetime: after the operation exits,
/// inherited lookups no longer expose the transaction. Nested transactions that
/// acquire a lease keep the owner from committing until their lifecycle ends.
public enum ActiveTransactionContext: Sendable {
    @TaskLocal private static var binding: Binding?

    package final class Lease: Sendable {
        package let transaction: any Transaction

        private let binding: Binding
        private let released = Mutex(false)

        fileprivate init(
            transaction: any Transaction,
            binding: Binding
        ) {
            self.transaction = transaction
            self.binding = binding
        }

        package func release() {
            let shouldRelease = released.withLock { released in
                guard !released else { return false }
                released = true
                return true
            }
            if shouldRelease {
                binding.releaseLease()
            }
        }

        deinit {
            release()
        }
    }

    fileprivate final class Binding: Sendable {
        private enum Phase: Sendable {
            case active(leaseCount: Int)
            case closing(leaseCount: Int, DrainCompletion)
            case closed
        }

        let transaction: any Transaction
        private let phase = Mutex(Phase.active(leaseCount: 0))

        init(transaction: any Transaction) {
            self.transaction = transaction
        }

        var activeTransaction: (any Transaction)? {
            phase.withLock { phase in
                guard case .active = phase else { return nil }
                return transaction
            }
        }

        func acquireLease() -> Lease? {
            phase.withLock { phase in
                guard case .active(let leaseCount) = phase else {
                    return nil
                }
                phase = .active(leaseCount: leaseCount + 1)
                return Lease(transaction: transaction, binding: self)
            }
        }

        func closeAndWait() async {
            let completion = phase.withLock { phase -> DrainCompletion? in
                guard case .active(let leaseCount) = phase else {
                    preconditionFailure(
                        "An active transaction binding must close exactly once"
                    )
                }
                guard leaseCount > 0 else {
                    phase = .closed
                    return nil
                }
                let completion = DrainCompletion()
                phase = .closing(
                    leaseCount: leaseCount,
                    completion
                )
                return completion
            }
            await completion?.wait()
        }

        fileprivate func releaseLease() {
            let completion = phase.withLock { phase -> DrainCompletion? in
                switch phase {
                case .active(let leaseCount):
                    precondition(leaseCount > 0)
                    phase = .active(leaseCount: leaseCount - 1)
                    return nil
                case .closing(let leaseCount, let completion):
                    precondition(leaseCount > 0)
                    if leaseCount == 1 {
                        phase = .closed
                        return completion
                    }
                    phase = .closing(
                        leaseCount: leaseCount - 1,
                        completion
                    )
                    return nil
                case .closed:
                    preconditionFailure(
                        "An active transaction lease cannot outlive its binding"
                    )
                }
            }
            completion?.resolve()
        }
    }

    private final class DrainCompletion: Sendable {
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
                precondition(!state.isResolved)
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

    package static var current: (any Transaction)? {
        binding?.activeTransaction
    }

    package static func acquireCurrentTransaction() -> Lease? {
        binding?.acquireLease()
    }

    /// Whether the current task is already executing a storage transaction.
    public static var isActive: Bool {
        current != nil
    }

    /// Runs one operation with task-scoped access to an owned transaction.
    ///
    /// The operation receives no commit or cancellation authority.
    public static func withActiveTransaction<T: Sendable>(
        _ transaction: any Transaction,
        operation: (any TransactionAccess) async throws -> T
    ) async rethrows -> T {
        let binding = Binding(transaction: transaction)
        do {
            let result = try await $binding.withValue(binding) {
                try await operation(transaction)
            }
            await binding.closeAndWait()
            return result
        } catch {
            await binding.closeAndWait()
            throw error
        }
    }
}
