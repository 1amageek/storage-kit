import DatabaseTypes
import FoundationDB
import StorageKit
import Synchronization

public struct FDBStorageRangeResult: TransactionRangeResult {
    public typealias Element = (ByteString, ByteString)

    private enum Source: Sendable {
        case sequence(FDB.AsyncKVSequence, FDBStorageTransaction)
        case failure(StorageError)
    }

    private let source: Source

    init(
        _ base: FDB.AsyncKVSequence,
        transaction: FDBStorageTransaction
    ) {
        self.source = .sequence(base, transaction)
    }

    init(error: StorageError) {
        self.source = .failure(error)
    }

    public func makeAsyncIterator() -> AsyncIterator {
        switch source {
        case .sequence(let sequence, let transaction):
            return AsyncIterator(
                sequence.makeAsyncIterator(),
                transaction: transaction
            )
        case .failure(let error):
            return AsyncIterator(error: error)
        }
    }

    /// A reference iterator whose aliases share one serialized iteration state.
    /// Only that state may enter the FoundationDB iterator, so `next()`
    /// and `finish()` cannot overlap even when aliases are used concurrently.
    public final class AsyncIterator: TransactionRangeIterator, Sendable {
        private let iterationState: RangeIterationState

        init(
            _ base: FDB.AsyncKVSequence.AsyncIterator,
            transaction: FDBStorageTransaction
        ) {
            self.iterationState = RangeIterationState(
                base,
                transaction: transaction
            )
        }

        init(error: StorageError) {
            self.iterationState = RangeIterationState(error: error)
        }

        public func next() async throws -> Element? {
            try await iterationState.next()
        }

        public func finish(
            isolation actor: isolated (any Actor)?
        ) async throws {
            try await iterationState.finish()
        }

        var finishWaiterCount: Int {
            get async {
                await iterationState.finishWaiterCount
            }
        }

        var nextWaiterCount: Int {
            get async {
                await iterationState.nextWaiterCount
            }
        }
    }
}

private actor RangeIterationState {
    private enum Source {
        case sequence(
            FDB.AsyncKVSequence.AsyncIterator,
            FDBStorageTransaction,
            TransactionActivityLease?
        )
        case reading(
            RangeIterationBoundary,
            finishRequested: Bool
        )
        case finishing(RangeIterationBoundary)
        case failure(StorageError)
        case finished
    }

    private var source: Source
    private(set) var finishWaiterCount = 0
    private(set) var nextWaiterCount = 0

    init(
        _ base: FDB.AsyncKVSequence.AsyncIterator,
        transaction: FDBStorageTransaction
    ) {
        self.source = .sequence(base, transaction, nil)
    }

    init(error: StorageError) {
        self.source = .failure(error)
    }

    func next() async throws -> (ByteString, ByteString)? {
        while true {
            switch source {
            case .failure(let error):
                source = .finished
                throw error
            case .finished:
                return nil
            case .reading(let completion, _),
                 .finishing(let completion):
                nextWaiterCount += 1
                defer { nextWaiterCount -= 1 }
                try await completion.waitForBoundary()
                try Task.checkCancellation()
            case .sequence(
                let iterator,
                let transaction,
                let currentLease
            ):
                return try await readNext(
                    iterator: iterator,
                    transaction: transaction,
                    currentLease: currentLease
                )
            }
        }
    }

    func finish() async throws {
        while true {
            switch source {
            case .failure, .finished:
                source = .finished
                return
            case .reading(let completion, false):
                source = .reading(completion, finishRequested: true)
                try await waitForFinishOperation(completion)
            case .reading(let completion, true),
                 .finishing(let completion):
                try await waitForFinishOperation(completion)
            case .sequence(let iterator, let transaction, let lease):
                let completion = RangeIterationBoundary()
                source = .finishing(completion)
                var activeLease = lease
                do {
                    if let lease {
                        try lease.resume()
                    } else {
                        activeLease = try transaction.makeOperationLease(
                            for: .rangeRead
                        )
                    }
                    guard let activeLease else {
                        preconditionFailure("Range operation lease was not created")
                    }
                    try await iterator.finish()
                    activeLease.release()
                    source = .finished
                    completion.resolve(.success(()))
                    return
                } catch is CancellationError {
                    activeLease?.release()
                    source = .finished
                    completion.resolve(.failure(.cancelled))
                    throw CancellationError()
                } catch let error as FDBError {
                    let converted = FDBStorageTransaction.convertFDBError(
                        error,
                        operation: .rangeRead
                    )
                    activeLease?.release()
                    source = .finished
                    completion.resolve(.failure(.storage(converted)))
                    throw converted
                } catch let error as StorageError {
                    activeLease?.release()
                    source = .finished
                    completion.resolve(.failure(.storage(error)))
                    throw error
                } catch {
                    let converted = FDBStorageTransaction.convertBackendError(
                        error,
                        operation: .rangeRead
                    )
                    activeLease?.release()
                    source = .finished
                    completion.resolve(.failure(.storage(converted)))
                    throw converted
                }
            }
        }
    }

    private func waitForFinishOperation(
        _ completion: RangeIterationBoundary
    ) async throws {
        finishWaiterCount += 1
        defer { finishWaiterCount -= 1 }
        try await completion.waitForBoundary()
    }

    private func readNext(
        iterator: FDB.AsyncKVSequence.AsyncIterator,
        transaction: FDBStorageTransaction,
        currentLease: TransactionActivityLease?
    ) async throws -> (ByteString, ByteString)? {
        let lease: TransactionActivityLease
        do {
            if let currentLease {
                try currentLease.resume()
                lease = currentLease
            } else {
                lease = try transaction.makeOperationLease(for: .rangeRead)
            }
        } catch let error as StorageError {
            source = .finished
            throw error
        } catch {
            source = .finished
            throw FDBStorageTransaction.convertBackendError(
                error,
                operation: .rangeRead
            )
        }

        let completion = RangeIterationBoundary()
        source = .reading(completion, finishRequested: false)

        do {
            let element = try await iterator.next()
            try Task.checkCancellation()
            try transaction.validateOperationLease(for: .rangeRead)

            if finishWasRequested(for: completion) {
                return try await completeRequestedFinish(
                    iterator: iterator,
                    lease: lease,
                    completion: completion
                )
            }

            guard let element else {
                lease.release()
                source = .finished
                completion.resolve(.success(()))
                return nil
            }

            lease.pause()
            source = .sequence(iterator, transaction, lease)
            completion.resolve(.success(()))
            return (element.key, element.value)
        } catch is CancellationError {
            lease.release()
            source = .finished
            completion.resolveIfPending(.failure(.cancelled))
            throw CancellationError()
        } catch let error as FDBError {
            let converted = FDBStorageTransaction.convertFDBError(
                error,
                operation: .rangeRead
            )
            lease.release()
            source = .finished
            completion.resolveIfPending(.failure(.storage(converted)))
            throw converted
        } catch let error as StorageError {
            lease.release()
            source = .finished
            completion.resolveIfPending(.failure(.storage(error)))
            throw error
        } catch {
            let converted = FDBStorageTransaction.convertBackendError(
                error,
                operation: .rangeRead
            )
            lease.release()
            source = .finished
            completion.resolveIfPending(.failure(.storage(converted)))
            throw converted
        }
    }

    private func finishWasRequested(
        for completion: RangeIterationBoundary
    ) -> Bool {
        guard case .reading(let activeCompletion, let requested) = source,
              activeCompletion === completion else {
            return false
        }
        return requested
    }

    private func completeRequestedFinish(
        iterator: FDB.AsyncKVSequence.AsyncIterator,
        lease: TransactionActivityLease,
        completion: RangeIterationBoundary
    ) async throws -> (ByteString, ByteString)? {
        source = .finishing(completion)
        do {
            try await iterator.finish()
            lease.release()
            source = .finished
            completion.resolve(.success(()))
            return nil
        } catch is CancellationError {
            lease.release()
            source = .finished
            completion.resolve(.failure(.cancelled))
            throw CancellationError()
        } catch let error as FDBError {
            let converted = FDBStorageTransaction.convertFDBError(
                error,
                operation: .rangeRead
            )
            lease.release()
            source = .finished
            completion.resolve(.failure(.storage(converted)))
            throw converted
        } catch let error as StorageError {
            lease.release()
            source = .finished
            completion.resolve(.failure(.storage(error)))
            throw error
        } catch {
            let converted = FDBStorageTransaction.convertBackendError(
                error,
                operation: .rangeRead
            )
            lease.release()
            source = .finished
            completion.resolve(.failure(.storage(converted)))
            throw converted
        }
    }
}

private enum RangeIterationBoundaryFailure: Error, Sendable {
    case cancelled
    case storage(StorageError)

    func throwAtBoundary() throws {
        switch self {
        case .cancelled:
            throw CancellationError()
        case .storage(let error):
            throw error
        }
    }
}

private final class RangeIterationBoundary: Sendable {
    private struct MutableState: Sendable {
        var result: Result<Void, RangeIterationBoundaryFailure>?
        var waiters: [CheckedContinuation<
            Result<Void, RangeIterationBoundaryFailure>,
            Never
        >] = []
    }

    private let state = Mutex(MutableState())

    func wait() async -> Result<Void, RangeIterationBoundaryFailure> {
        let completed = state.withLock { $0.result }
        if let completed {
            return completed
        }
        return await withCheckedContinuation { continuation in
            let racedResult = state.withLock { state
                -> Result<Void, RangeIterationBoundaryFailure>? in
                if let result = state.result {
                    return result
                }
                state.waiters.append(continuation)
                return nil
            }
            if let racedResult {
                continuation.resume(returning: racedResult)
            }
        }
    }

    func waitForBoundary() async throws {
        switch await wait() {
        case .success:
            return
        case .failure(let failure):
            try failure.throwAtBoundary()
        }
    }

    func resolve(_ result: Result<Void, RangeIterationBoundaryFailure>) {
        let waiters = state.withLock { state in
            precondition(
                state.result == nil,
                "Range iterator operation resolved more than once"
            )
            state.result = result
            let waiters = state.waiters
            state.waiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume(returning: result)
        }
    }

    func resolveIfPending(
        _ result: Result<Void, RangeIterationBoundaryFailure>
    ) {
        let waiters = state.withLock { state
            -> [CheckedContinuation<
                Result<Void, RangeIterationBoundaryFailure>,
                Never
            >]? in
            guard state.result == nil else {
                return nil
            }
            state.result = result
            let waiters = state.waiters
            state.waiters.removeAll(keepingCapacity: false)
            return waiters
        }
        guard let waiters else { return }
        for waiter in waiters {
            waiter.resume(returning: result)
        }
    }
}
