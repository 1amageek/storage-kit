import DatabaseTypes
import FoundationDB
import StorageKit
import Synchronization
import Testing
@testable import FDBStorage

@Suite("FoundationDB transaction footprint admission")
struct FDBStorageTransactionFootprintTests {
    @Test("FoundationDB bounded point reads preserve the exact value bound")
    func boundedPointReadPreservesExactValueBound() async throws {
        let value: ByteString = [0x01, 0x02, 0x03]
        let backend = SizeReportingTransaction(
            approximateSize: 0,
            pointValue: value
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        #expect(
            try await transaction.getValue(
                for: [0x41],
                snapshot: true,
                maximumByteCount: value.count
            ) == value
        )

        var failure: StorageError?
        do {
            _ = try await transaction.getValue(
                for: [0x41],
                snapshot: true,
                maximumByteCount: value.count - 1
            )
        } catch let error as StorageError {
            failure = error
        }
        #expect(failure?.code == .valueTooLarge)
        #expect(failure?.backend == .foundationDB)
        #expect(
            failure?.byteLimitViolation?.observedByteCount
                == UInt64(value.count)
        )
        #expect(
            failure?.byteLimitViolation?.maximumByteCount
                == UInt64(value.count - 1)
        )
        #expect(transaction.storageFailure == nil)

        // A caller-owned bound violation does not invalidate the transaction.
        #expect(
            try await transaction.getValue(
                for: [0x41],
                snapshot: true,
                maximumByteCount: value.count
            ) == value
        )

        do {
            _ = try await transaction.getValue(
                for: [0x41],
                snapshot: true,
                maximumByteCount: -1
            )
            Issue.record("Expected an invalid point-read maximum")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.backend == .foundationDB)
        }
        try await transaction.commit()
    }

    @Test("Commit request limits reject values outside FoundationDB's range", arguments: [31, 10_000_001])
    func rejectsInvalidConfiguredLimit(value: Int) {
        #expect(throws: CommitRequestLimitError.self) {
            _ = try CommitRequestLimit(maximumByteCount: value)
        }
    }

    @Test("A custom limit configures both FoundationDB and pre-dispatch gates")
    func appliesCustomLimitToBothGates() async throws {
        let limit = try CommitRequestLimit(maximumByteCount: 64)
        let backend = SizeReportingTransaction(approximateSize: 65)
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain(),
            commitRequestLimit: limit
        )

        let failure = await commitFailure(from: transaction)

        #expect(backend.configuredSizeLimit == 64)
        #expect(failure.code == .transactionTooLarge)
        #expect(failure.byteLimitViolation?.maximumByteCount == 64)
        #expect(backend.commitCount == 0)
    }

    @Test("The FoundationDB footprint boundary is admitted before commit")
    func admitsExactBoundary() async throws {
        let limit = CommitRequestLimit.default
        let backend = SizeReportingTransaction(approximateSize: limit.maximumByteCount)
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain(),
            commitRequestLimit: limit
        )

        try await transaction.commit()

        #expect(backend.approximateSizeRequestCount == 1)
        #expect(backend.commitCount == 1)
        #expect(backend.cancelCount == 0)
        #expect(backend.configuredSizeLimit == limit.maximumByteCount)
    }

    @Test("An oversized footprint is rejected and cancelled before commit dispatch")
    func rejectsBeforeCommitDispatch() async throws {
        let maximum = CommitRequestLimit.default.maximumByteCount
        let backend = SizeReportingTransaction(approximateSize: maximum + 1)
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let failure = await commitFailure(from: transaction)

        #expect(failure.code == .transactionTooLarge)
        #expect(failure.operation == .prepare)
        #expect(failure.retryDisposition == .never)
        #expect(failure.byteLimitViolation == StorageByteLimitViolation(
            resource: .commitRequest,
            observedByteCount: UInt64(maximum + 1),
            maximumByteCount: UInt64(maximum),
            measurement: .estimated
        ))
        #expect(backend.approximateSizeRequestCount == 1)
        #expect(backend.commitCount == 0)
        #expect(backend.cancelCount == 1)
    }

    @Test("Opaque namespace mutations participate in commit-request admission")
    func rejectsOpaqueNamespaceMutationFootprint() async throws {
        let maximum = CommitRequestLimit.default.maximumByteCount
        let backend = SizeReportingTransaction(
            approximateSize: 0,
            footprintAfterMutation: maximum + 1
        )
        let transactionDomain = StorageTransactionDomain()
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: transactionDomain
        )

        try await transaction.withNamespaceOperation(
            transactionDomain: transactionDomain,
            writes: true,
            operation: .write
        ) { rawTransaction in
            try rawTransaction.setValue([0x01], for: [0x02])
        }

        let failure = await commitFailure(from: transaction)

        #expect(failure.code == .transactionTooLarge)
        #expect(backend.setCount == 1)
        #expect(backend.commitCount == 0)
        #expect(backend.cancelCount == 1)
    }

    @Test("Footprint lookup failures retain pre-dispatch retry certainty")
    func preservesPreDispatchFailureCertainty() async throws {
        let backend = SizeReportingTransaction(
            approximateSize: 0,
            approximateSizeError: .transactionTooOld
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let failure = await commitFailure(from: transaction)

        #expect(failure.code == .transactionTooOld)
        #expect(failure.operation == .prepare)
        #expect(failure.retryDisposition == .safe)
        #expect(failure.code != .commitUnknownResult)
        #expect(backend.commitCount == 0)
        #expect(backend.cancelCount == 1)
    }

    @Test(
        "Impossible unknown-commit errors during preflight become contract violations",
        arguments: [FDBErrorCode.commitUnknownResult, .idempotencyStatusUnknown]
    )
    func rejectsUnknownCommitStateBeforeDispatch(errorCode: FDBErrorCode) async throws {
        let backend = SizeReportingTransaction(
            approximateSize: 0,
            approximateSizeError: errorCode
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let failure = await commitFailure(from: transaction)

        #expect(failure.code == .backendContractViolation)
        #expect(failure.operation == .prepare)
        #expect(failure.code != .commitUnknownResult)
        #expect(backend.commitCount == 0)
        #expect(backend.cancelCount == 1)
    }

    @Test("A negative footprint is an explicit backend contract violation")
    func rejectsNegativeFootprint() async throws {
        let backend = SizeReportingTransaction(approximateSize: -1)
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let failure = await commitFailure(from: transaction)

        #expect(failure.code == .backendContractViolation)
        #expect(failure.operation == .prepare)
        #expect(backend.commitCount == 0)
        #expect(backend.cancelCount == 1)
    }

    @Test("A backend size rejection remains deterministic after commit dispatch")
    func preservesBackendSizeRejection() async throws {
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            commitError: .transactionTooLarge
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let failure = await commitFailure(from: transaction)

        #expect(failure.code == .transactionTooLarge)
        #expect(failure.operation == .commit)
        #expect(failure.backendCode == FDBErrorCode.transactionTooLarge.rawValue)
        #expect(failure.retryDisposition == .never)
        #expect(backend.commitCount == 1)
    }

    @Test("Namespace mutation prevents late logical admission configuration")
    func rejectsConfigurationAfterNamespaceMutation() async throws {
        let backend = SizeReportingTransaction(approximateSize: 1)
        let transactionDomain = StorageTransactionDomain()
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: transactionDomain
        )
        try await transaction.withNamespaceOperation(
            transactionDomain: transactionDomain,
            writes: true,
            operation: .write
        ) { rawTransaction in
            try rawTransaction.setValue([0x01], for: [0x02])
        }

        #expect(throws: TransactionMutationByteLimitError.configurationAfterAdmission) {
            try transaction.configureMutationByteLimit(maximumBytes: 1_024)
        }
    }

    @Test("A suspended point read prevents commit admission")
    func pointReadLeasePreventsCommit() async throws {
        let gate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            valueReadGate: gate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        async let readValue = transaction.getValue(
            for: ByteString([0x01]),
            snapshot: false
        )
        try await gate.waitUntilEntered()

        let busyFailure = await commitFailure(from: transaction)
        #expect(busyFailure.code == .transactionBusy)
        #expect(backend.approximateSizeRequestCount == 0)
        #expect(backend.commitCount == 0)

        await gate.release()
        let value = try await readValue
        #expect(value == nil)
        try await transaction.commit()
        #expect(backend.commitCount == 1)
    }

    @Test("Cancellation waits for an admitted point read to drain")
    func cancellationDrainsPointRead() async throws {
        let gate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            valueReadGate: gate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let readTask = Task {
            try await transaction.getValue(
                for: ByteString([0x01]),
                snapshot: false
            )
        }
        try await gate.waitUntilEntered()

        let cancellationReturned = Mutex(false)
        let cancelTask = Task {
            try await transaction.cancel()
            cancellationReturned.withLock { $0 = true }
        }
        try await waitUntil("raw cancellation dispatch") {
            backend.cancelCount == 1
        }
        #expect(!cancellationReturned.withLock { $0 })

        await gate.release()
        _ = try await readTask.value
        try await cancelTask.value

        #expect(cancellationReturned.withLock { $0 })
        #expect(backend.cancelCount == 1)
    }

    @Test("A suspended lazy range prevents commit admission until cursor cleanup")
    func rangeLeasePreventsCommit() async throws {
        let gate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            rangeReadGate: gate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )
        let rows = transaction.getRange(
            from: .firstGreaterOrEqual(ByteString([0x00])),
            to: .firstGreaterOrEqual(ByteString([0xff])),
            limit: 1,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )

        async let rangeFailure = firstRangeFailure(rows)
        try await gate.waitUntilEntered()

        let busyFailure = await commitFailure(from: transaction)
        #expect(busyFailure.code == .transactionBusy)
        #expect(backend.approximateSizeRequestCount == 0)
        #expect(backend.commitCount == 0)

        await gate.release()
        let observedRangeFailure = await rangeFailure
        #expect(observedRangeFailure?.operation == .rangeRead)
        try await transaction.commit()
        #expect(backend.commitCount == 1)
    }

    @Test("Concurrent commit observers share one in-flight admission and commit")
    func sharesOneInFlightCommitOutcome() async throws {
        let footprintReadGate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            footprintReadGate: footprintReadGate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let first = Task {
            try await transaction.commit()
        }
        try await footprintReadGate.waitUntilEntered()

        let second = Task {
            try await transaction.commit()
        }
        try await waitUntil("concurrent commit follower") {
            transaction.commitFollowerCount == 1
        }

        #expect(backend.approximateSizeRequestCount == 1)
        #expect(backend.commitCount == 0)

        await footprintReadGate.release()
        try await first.value
        try await second.value

        #expect(backend.approximateSizeRequestCount == 1)
        #expect(backend.commitCount == 1)
        #expect(backend.cancelCount == 0)
    }

    @Test("Concurrent commit observers share one rejected admission")
    func sharesOneInFlightAdmissionFailure() async throws {
        let footprintReadGate = OperationGate()
        let maximum = CommitRequestLimit.default.maximumByteCount
        let backend = SizeReportingTransaction(
            approximateSize: maximum + 1,
            footprintReadGate: footprintReadGate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let first = Task {
            await commitFailure(from: transaction)
        }
        try await footprintReadGate.waitUntilEntered()

        let second = Task {
            await commitFailure(from: transaction)
        }
        try await waitUntil("concurrent rejected commit follower") {
            transaction.commitFollowerCount == 1
        }

        #expect(backend.approximateSizeRequestCount == 1)
        #expect(backend.commitCount == 0)
        #expect(backend.cancelCount == 0)

        await footprintReadGate.release()
        let firstFailure = await first.value
        let secondFailure = await second.value

        #expect(firstFailure == secondFailure)
        #expect(firstFailure.code == .transactionTooLarge)
        #expect(backend.approximateSizeRequestCount == 1)
        #expect(backend.commitCount == 0)
        #expect(backend.cancelCount == 1)
    }

    @Test("Cancellation preempts suspended commit admission before dispatch")
    func cancellationPreemptsCommitAdmission() async throws {
        let footprintReadGate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            footprintReadGate: footprintReadGate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let commitTask = Task {
            await commitFailure(from: transaction)
        }
        try await footprintReadGate.waitUntilEntered()

        let cancellationReturned = Mutex(false)
        let cancelTask = Task {
            defer {
                cancellationReturned.withLock { $0 = true }
            }
            try await transaction.cancel()
        }
        try await waitUntil("raw cancellation dispatch") {
            backend.cancelCount == 1
        }

        #expect(backend.cancelCount == 1)
        #expect(backend.commitCount == 0)
        #expect(backend.approximateSizeRequestCount == 1)
        #expect(!cancellationReturned.withLock { $0 })

        let lateCommitFailure = await commitFailure(from: transaction)
        #expect(lateCommitFailure.code == .transactionCancelled)
        #expect(lateCommitFailure.operation == .prepare)

        await footprintReadGate.release()
        try await cancelTask.value
        let leaderFailure = await commitTask.value
        #expect(leaderFailure == lateCommitFailure)
        #expect(cancellationReturned.withLock { $0 })
        #expect(backend.cancelCount == 1)
        #expect(backend.commitCount == 0)

        do {
            _ = try await transaction.getValue(for: [0x01], snapshot: true)
            Issue.record("Cancelled transaction accepted a new read")
        } catch let error as StorageError {
            #expect(error.code == .transactionCancelled)
            #expect(error.operation == .read)
        } catch {
            Issue.record("Cancelled transaction returned an unexpected error: \(error)")
        }
    }

    @Test("Cancellation waits once raw commit dispatch has started")
    func cancellationWaitsForDispatchedCommit() async throws {
        let commitGate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            commitGate: commitGate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let commitTask = Task {
            try await transaction.commit()
        }
        try await commitGate.waitUntilEntered()

        let cancelTask = Task {
            await cancellationFailure(from: transaction)
        }
        try await waitUntil("cancellation waiting for dispatched commit") {
            transaction.commitCancellationWaiterCount == 1
        }

        #expect(backend.commitCount == 1)
        #expect(backend.cancelCount == 0)

        await commitGate.release()
        try await commitTask.value
        let failure = await cancelTask.value

        #expect(failure.code == .invalidOperation)
        #expect(failure.operation == .cancel)
        #expect(backend.cancelCount == 0)
    }

    @Test("Cancellation preserves an unknown result after commit dispatch")
    func cancellationPreservesUnknownCommitResult() async throws {
        let commitGate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            commitError: .commitUnknownResult,
            commitGate: commitGate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let commitTask = Task {
            await commitFailure(from: transaction)
        }
        try await commitGate.waitUntilEntered()

        let cancelTask = Task {
            await cancellationFailure(from: transaction)
        }
        try await waitUntil("cancellation waiting for unknown commit result") {
            transaction.commitCancellationWaiterCount == 1
        }

        await commitGate.release()
        let commitError = await commitTask.value
        let cancellationError = await cancelTask.value

        #expect(commitError.code == .commitUnknownResult)
        #expect(cancellationError == commitError)
        #expect(backend.commitCount == 1)
        #expect(backend.cancelCount == 0)
    }

    @Test("Cancellation accepts deterministic failure after commit dispatch")
    func cancellationAcceptsDeterministicCommitFailure() async throws {
        let commitGate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            commitError: .transactionTooLarge,
            commitGate: commitGate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        let commitTask = Task {
            await commitFailure(from: transaction)
        }
        try await commitGate.waitUntilEntered()

        let cancelTask = Task {
            try await transaction.cancel()
        }
        try await waitUntil("cancellation waiting for deterministic commit failure") {
            transaction.commitCancellationWaiterCount == 1
        }

        await commitGate.release()
        let commitError = await commitTask.value
        try await cancelTask.value

        #expect(commitError.code == .transactionTooLarge)
        #expect(backend.commitCount == 1)
        #expect(backend.cancelCount == 0)
    }

    @Test("Cached range rows are invalid after cancellation")
    func cachedRangeRowsRespectCancellation() async throws {
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            rangePages: [RangeBatch(
                records: [
                    rangeRow(key: [0x01], value: [0x11]),
                    rangeRow(key: [0x02], value: [0x22]),
                ],
                hasMore: false
            )]
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )
        let rows = makeRange(from: transaction)
        var cursor = rows.makeCursor()

        let first = try await cursor.next()
        #expect(first?.0 == ByteString([0x01]))
        try await transaction.cancel()

        do {
            _ = try await cursor.next()
            Issue.record("Cached range row was returned after cancellation")
        } catch let error as StorageError {
            #expect(error.code == .transactionCancelled)
        } catch {
            Issue.record("Range cancellation returned an unexpected error: \(error)")
        }

        try await cursor.finish()
        #expect(backend.cancelCount == 1)
    }

    @Test("Range cursor aliases share one terminal state and lease")
    func rangeIteratorAliasesShareTerminalState() async throws {
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            rangePages: [RangeBatch(
                records: [
                    rangeRow(key: [0x01], value: [0x11]),
                    rangeRow(key: [0x02], value: [0x22]),
                ],
                hasMore: false
            )]
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )
        let cursor = makeRange(from: transaction).makeCursor()

        _ = try await cursor.next()
        var alias = cursor
        try await alias.finish()

        let rowAfterAliasFinished = try await cursor.next()
        #expect(rowAfterAliasFinished == nil)
        try await transaction.commit()
        #expect(backend.commitCount == 1)
    }

    @Test("Range finish joins an in-flight read and prevents state revival")
    func rangeFinishJoinsInFlightRead() async throws {
        let rangeReadGate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            rangeReadGate: rangeReadGate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )
        let cursor = makeRange(from: transaction).makeCursor()

        let readTask = Task {
            await nextFailure(from: cursor)
        }
        try await rangeReadGate.waitUntilEntered()

        let alias = cursor
        let finishTask = Task {
            await finishFailure(from: alias)
        }
        try await waitUntilIteratorState("range finish waiter admission") {
            await cursor.finishWaiterCount == 1
        }

        let busyFailure = await commitFailure(from: transaction)
        #expect(busyFailure.code == .transactionBusy)
        #expect(backend.commitCount == 0)

        await rangeReadGate.release()
        let readFailure = try #require(await readTask.value)
        let finishFailure = try #require(await finishTask.value)
        #expect(readFailure == finishFailure)
        #expect(readFailure.operation == .rangeRead)
        let repeatedFinishFailure = try #require(
            await self.finishFailure(from: cursor)
        )
        #expect(repeatedFinishFailure == finishFailure)

        let rowAfterFinish = try await cursor.next()
        #expect(rowAfterFinish == nil)
        try await transaction.commit()
        #expect(backend.commitCount == 1)
    }

    @Test("Concurrent range readers observe the same typed failure")
    func concurrentRangeReadersShareFailure() async throws {
        let rangeReadGate = OperationGate()
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            rangeReadGate: rangeReadGate
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )
        let cursor = makeRange(from: transaction).makeCursor()

        let leader = Task {
            await nextFailure(from: cursor)
        }
        try await rangeReadGate.waitUntilEntered()

        let follower = Task {
            await nextFailure(from: cursor)
        }
        try await waitUntilIteratorState("concurrent range read follower") {
            await cursor.nextWaiterCount == 1
        }

        #expect(backend.rangeReadCount == 1)
        await rangeReadGate.release()

        let leaderFailure = try #require(await leader.value)
        let followerFailure = try #require(await follower.value)
        #expect(leaderFailure == followerFailure)
        #expect(leaderFailure.code == .backendFailure)
        #expect(leaderFailure.operation == .rangeRead)
        #expect(backend.rangeReadCount == 1)

        var finishingIterator = cursor
        try await finishingIterator.finish()
        try await transaction.commit()
        #expect(backend.commitCount == 1)
    }

    @Test("Range lease ends on natural exhaustion")
    func naturalRangeExhaustionReleasesLease() async throws {
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            rangePages: [RangeBatch(
                records: [rangeRow(key: [0x01], value: [0x11])],
                hasMore: false
            )]
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )
        let cursor = makeRange(from: transaction).makeCursor()

        _ = try await cursor.next()
        let busyFailure = await commitFailure(from: transaction)
        #expect(busyFailure.code == .transactionBusy)

        let exhausted = try await cursor.next()
        #expect(exhausted == nil)
        try await transaction.commit()
        #expect(backend.commitCount == 1)
    }

    @Test("Dropping a range cursor releases its lease")
    func droppingRangeIteratorReleasesLease() async throws {
        let backend = SizeReportingTransaction(
            approximateSize: 1,
            rangePages: [RangeBatch(
                records: [rangeRow(key: [0x01], value: [0x11])],
                hasMore: false
            )]
        )
        let transaction = try FDBStorageTransaction(
            backend,
            transactionDomain: StorageTransactionDomain()
        )

        try await readOneAndDropIterator(makeRange(from: transaction))
        try await transaction.commit()
        #expect(backend.commitCount == 1)
    }

    private func makeRange(
        from transaction: FDBStorageTransaction
    ) -> FDBStorageRangeResult {
        transaction.getRange(
            from: .firstGreaterOrEqual(ByteString([0x00])),
            to: .firstGreaterOrEqual(ByteString([0xff])),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )
    }

    private func readOneAndDropIterator(
        _ rows: FDBStorageRangeResult
    ) async throws {
        let cursor = rows.makeCursor()
        _ = try await cursor.next()
    }

    private func waitUntil(
        _ description: String,
        condition: @escaping @Sendable () -> Bool
    ) async throws {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(5))
        while !condition() {
            guard clock.now < deadline else {
                throw FoundationDBFootprintDeadlineError.deadlineExceeded(description)
            }
            await Task.yield()
        }
    }

    private func waitUntilIteratorState(
        _ description: String,
        condition: @escaping @Sendable () async -> Bool
    ) async throws {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(5))
        while !(await condition()) {
            guard clock.now < deadline else {
                throw FoundationDBFootprintDeadlineError.deadlineExceeded(description)
            }
            await Task.yield()
        }
    }

    private func commitFailure(
        from transaction: FDBStorageTransaction
    ) async -> StorageError {
        do {
            try await transaction.commit()
            Issue.record("Commit unexpectedly succeeded")
            return StorageError(
                code: .backendFailure,
                operation: .commit,
                backend: .foundationDB,
                message: "Test did not observe the required commit failure"
            )
        } catch let error as StorageError {
            return error
        } catch {
            Issue.record("Commit returned an unexpected error type: \(error)")
            return StorageError(
                code: .backendFailure,
                operation: .commit,
                backend: .foundationDB,
                message: "Test observed an unexpected error type",
                underlyingDescription: String(describing: error)
            )
        }
    }

    private func cancellationFailure(
        from transaction: FDBStorageTransaction
    ) async -> StorageError {
        do {
            try await transaction.cancel()
            Issue.record("Cancellation unexpectedly succeeded")
            return StorageError(
                code: .backendFailure,
                operation: .cancel,
                backend: .foundationDB,
                message: "Test did not observe the required cancellation failure"
            )
        } catch let error as StorageError {
            return error
        } catch {
            Issue.record("Cancellation returned an unexpected error type: \(error)")
            return StorageError(
                code: .backendFailure,
                operation: .cancel,
                backend: .foundationDB,
                message: "Test observed an unexpected cancellation error type",
                underlyingDescription: String(describing: error)
            )
        }
    }

    private func firstRangeFailure(
        _ rows: FDBStorageRangeResult
    ) async -> StorageError? {
        var cursor = rows.makeCursor()
        do {
            _ = try await cursor.next()
            try await cursor.finish()
            Issue.record("Range iteration unexpectedly succeeded")
            return nil
        } catch let error as StorageError {
            return error
        } catch {
            Issue.record("Range iteration returned an unexpected error: \(error)")
            return nil
        }
    }

    private func nextFailure(
        from cursor: FDBStorageRangeResult.Cursor
    ) async -> StorageError? {
        do {
            _ = try await cursor.next()
            Issue.record("Range read unexpectedly succeeded")
            return nil
        } catch let error as StorageError {
            return error
        } catch {
            Issue.record("Range read returned an unexpected error: \(error)")
            return nil
        }
    }

    private func finishFailure(
        from cursor: FDBStorageRangeResult.Cursor
    ) async -> StorageError? {
        var cursor = cursor
        do {
            try await cursor.finish()
            Issue.record("Range finish unexpectedly succeeded")
            return nil
        } catch let error as StorageError {
            return error
        } catch {
            Issue.record("Range finish returned an unexpected error: \(error)")
            return nil
        }
    }
}

private enum FoundationDBFootprintDeadlineError: Error {
    case deadlineExceeded(String)
}

private func rangeRow(
    key: ByteString,
    value: ByteString
) -> FDB.KeyValue {
    FDB.KeyValue(key: key, value: value)
}

private final class SizeReportingTransaction: TransactionProtocol, Sendable {
    private struct State: Sendable {
        var approximateSize: Int
        let footprintAfterMutation: Int?
        let approximateSizeError: FDBErrorCode?
        let commitError: FDBErrorCode?
        let rangePages: [RangeBatch]
        var approximateSizeRequestCount = 0
        var nextRangePageIndex = 0
        var setCount = 0
        var rangeReadCount = 0
        var commitCount = 0
        var cancelCount = 0
        var configuredSizeLimit: Int?
    }

    private let state: Mutex<State>
    private let footprintReadGate: OperationGate?
    private let commitGate: OperationGate?
    private let valueReadGate: OperationGate?
    private let rangeReadGate: OperationGate?
    private let pointValue: ByteString?

    init(
        approximateSize: Int,
        footprintAfterMutation: Int? = nil,
        approximateSizeError: FDBErrorCode? = nil,
        commitError: FDBErrorCode? = nil,
        rangePages: [RangeBatch] = [],
        footprintReadGate: OperationGate? = nil,
        commitGate: OperationGate? = nil,
        valueReadGate: OperationGate? = nil,
        rangeReadGate: OperationGate? = nil,
        pointValue: ByteString? = nil
    ) {
        self.state = Mutex(State(
            approximateSize: approximateSize,
            footprintAfterMutation: footprintAfterMutation,
            approximateSizeError: approximateSizeError,
            commitError: commitError,
            rangePages: rangePages
        ))
        self.footprintReadGate = footprintReadGate
        self.commitGate = commitGate
        self.valueReadGate = valueReadGate
        self.rangeReadGate = rangeReadGate
        self.pointValue = pointValue
    }

    var approximateSizeRequestCount: Int {
        state.withLock { $0.approximateSizeRequestCount }
    }

    var setCount: Int {
        state.withLock { $0.setCount }
    }

    var commitCount: Int {
        state.withLock { $0.commitCount }
    }

    var rangeReadCount: Int {
        state.withLock { $0.rangeReadCount }
    }

    var cancelCount: Int {
        state.withLock { $0.cancelCount }
    }

    var configuredSizeLimit: Int? {
        state.withLock { $0.configuredSizeLimit }
    }

    func getValue<Key: FDB.ByteInput>(
        for key: Key,
        snapshot: Bool
    ) async throws -> ByteString? {
        if let valueReadGate {
            await valueReadGate.suspendOperation()
        }
        return pointValue
    }

    func setValue<Value: FDB.ByteInput, Key: FDB.ByteInput>(
        _ value: Value,
        for key: Key
    ) throws {
        state.withLock { state in
            state.setCount += 1
            if let footprintAfterMutation = state.footprintAfterMutation {
                state.approximateSize = footprintAfterMutation
            }
        }
    }

    func clear<Key: FDB.ByteInput>(key: Key) throws {}

    func clearRange<Begin: FDB.ByteInput, End: FDB.ByteInput>(
        beginKey: Begin,
        endKey: End
    ) throws {}

    func getKey(
        selector: FDB.KeySelector,
        snapshot: Bool
    ) async throws -> ByteString {
        []
    }

    func readRangeBatch(
        from begin: FDB.KeySelector,
        to end: FDB.KeySelector,
        limit: Int,
        targetBytes: Int,
        streamingMode: FDB.StreamingMode,
        iteration: Int,
        reverse: Bool,
        snapshot: Bool
    ) async throws -> RangeBatch {
        state.withLock { $0.rangeReadCount += 1 }
        if let rangeReadGate {
            await rangeReadGate.suspendOperation()
            throw FDBError(.invalidAPICall)
        }
        return state.withLock { state in
            guard state.nextRangePageIndex < state.rangePages.count else {
                return RangeBatch(records: [], hasMore: false)
            }
            let page = state.rangePages[state.nextRangePageIndex]
            state.nextRangePageIndex += 1
            return page
        }
    }

    func commit() async throws {
        let commitError = state.withLock { state in
            state.commitCount += 1
            return state.commitError
        }
        if let commitGate {
            await commitGate.suspendOperation()
        }
        if let commitError {
            throw FDBError(commitError)
        }
    }

    func cancel() {
        state.withLock { $0.cancelCount += 1 }
    }

    func requestVersionstamp() -> any FDB.PendingTransactionVersionstamp {
        FixedTransactionVersionstamp()
    }

    func setReadVersion(_ version: FDB.Version) {}

    func getReadVersion() async throws -> FDB.Version {
        0
    }

    func onError(_ error: FDBError) async throws {}

    func getEstimatedRangeSizeBytes<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End
    ) async throws -> Int64 {
        0
    }

    func getRangeSplitPoints<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End,
        chunkSize: Int64
    ) async throws -> [ByteString] {
        []
    }

    func getCommittedVersion() throws -> FDB.Version {
        0
    }

    func approximateSize() async throws -> Int64 {
        let result = state.withLock { state -> (Int, FDBErrorCode?) in
            state.approximateSizeRequestCount += 1
            return (state.approximateSize, state.approximateSizeError)
        }
        if let footprintReadGate {
            await footprintReadGate.suspendOperation()
        }
        if let error = result.1 {
            throw FDBError(error)
        }
        return Int64(result.0)
    }

    func atomicOp<Key: FDB.ByteInput, Parameter: FDB.ByteInput>(
        key: Key,
        param: Parameter,
        mutationType: FDB.MutationType
    ) throws {}

    func addConflictRange<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End,
        type: FDB.ConflictRangeType
    ) throws {}

    func setOption<Value: FDB.ByteInput>(
        to value: Value,
        forOption option: FDB.TransactionOption
    ) throws {}

    func setOption(forOption option: FDB.TransactionOption) throws {}

    func setOption(
        to value: String,
        forOption option: FDB.TransactionOption
    ) throws {}

    func setOption(
        to value: Int,
        forOption option: FDB.TransactionOption
    ) throws {
        if option == .sizeLimit {
            state.withLock { $0.configuredSizeLimit = value }
        }
    }
}

private actor OperationGate {
    private var entered = false
    private var released = false
    private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

    func suspendOperation() async {
        entered = true
        guard !released else { return }
        await withCheckedContinuation { continuation in
            releaseWaiters.append(continuation)
        }
    }

    func waitUntilEntered() async throws {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: .seconds(5))
        while !entered {
            guard clock.now < deadline else {
                throw FoundationDBFootprintDeadlineError.deadlineExceeded(
                    "operation gate entry"
                )
            }
            await Task.yield()
        }
    }

    func release() {
        guard !released else { return }
        released = true
        let waitingForRelease = releaseWaiters
        releaseWaiters.removeAll(keepingCapacity: false)
        for waiter in waitingForRelease {
            waiter.resume()
        }
    }
}
