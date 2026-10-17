import DatabaseTypes
import StorageKit

/// Behavioral checkpoint cases shared by real adapters and the reference engine.
public struct TransactionCheckpointConformanceCase<Engine: StorageEngine>: Sendable {
    private let makeEngine: @Sendable () async throws -> Engine

    public init(makeEngine: @escaping @Sendable () async throws -> Engine) {
        self.makeEngine = makeEngine
    }

    private func limits(maximumEntries: Int = 4096, maximumDepth: Int = 32) throws -> TransactionCheckpointLimits {
        try TransactionCheckpointLimits(
            maximumEntries: maximumEntries, maximumDepth: maximumDepth,
            maximumRetainedBytes: 4 * 1024 * 1024,
            maximumWorkUnits: 1_000_000, maximumReadPageRows: 8
        )
    }

    private func transaction(_ engine: Engine) throws -> any Transaction {
        let transaction = try engine.createOwnedTransaction()
        try transaction.configureCheckpoints(limits: limits())
        return transaction
    }

    private func checkpointAccess(_ transaction: any TransactionAccess) throws -> any TransactionCheckpointAccess {
        guard transaction.capabilities.transactionCheckpoints, let access = transaction.checkpoints else {
            throw DirectoryConformanceFailure(step: "checkpoints", message: "Configured adapter did not expose complete checkpoint support")
        }
        return access
    }

    public func verifyTokens() async throws {
        try await withEngine { engine in
            let transaction = try self.transaction(engine)
            let access = try self.checkpointAccess(transaction)
            let first = try await access.createCheckpoint()
            try transaction.setValue([1], for: [1])
            let later = try await access.createCheckpoint()
            try transaction.setValue([2], for: [2])
            try await access.rollback(to: first)
            try require(try await transaction.getValue(for: [1]) == nil, "CP-01", "rollback must discard the first suffix")
            try await requireCheckpointFailure { try await access.rollback(to: later) }
            try transaction.setValue([3], for: [3])
            try await access.rollback(to: first)
            let releaseTarget = try await access.createCheckpoint()
            try transaction.setValue([4], for: [4])
            _ = try await access.createCheckpoint()
            try await access.release(releaseTarget)
            try await requireCheckpointFailure { try await access.rollback(to: releaseTarget) }
            let other = try self.transaction(engine)
            let foreign = try await self.checkpointAccess(other).createCheckpoint()
            try await requireCheckpointFailure { try await access.rollback(to: foreign) }
            try await other.cancel()
            try await transaction.commit()
            try await requireCheckpointFailure { try await access.rollback(to: first) }
            let persisted = try await engine.withTransaction { try await $0.getValue(for: [4]) }
            try require(persisted == [4], "CP-01", "release must preserve mutations at commit")
        }
    }

    public func verifyOverlay() async throws {
        try await withEngine { engine in
            try await engine.withTransaction { transaction in
                try transaction.setValue([10], for: [0x10])
                try transaction.setValue([30], for: [0x30])
                try transaction.setValue([50], for: [0x50])
            }
            let transaction = try self.transaction(engine)
            let checkpoint = try self.checkpointAccess(transaction)
            let marker = try await checkpoint.createCheckpoint()
            try transaction.setValue([20], for: [0x20])
            try transaction.clearRange(beginKey: [0x30], endKey: [0x50])
            try transaction.atomicOp(key: [0x20], param: [2], mutationType: .add)
            try require(try await transaction.getValue(for: [0x20], snapshot: false, maximumByteCount: 1) == [22], "CP-02", "point atomic replay must preserve width/order")
            try require(try await transaction.getKey(selector: .firstGreaterThan([0x10]), snapshot: false) == [0x20], "CP-02", "selectors must include inserted rows")
            let reverse = try await TransactionRangeCollection.collect(
                using: transaction, from: .firstGreaterOrEqual([0]), to: .firstGreaterOrEqual([0x60]),
                limit: 2, reverse: true, snapshot: false, streamingMode: .iterator
            )
            try require(reverse.map(\.0) == [[0x50], [0x20]], "CP-02", "reverse limit must apply after overlay")
            let size = try await transaction.getEstimatedRangeSizeBytes(beginKey: [0], endKey: [0x60])
            try require(size == 6, "CP-02", "metrics must see surviving key/value bytes")
            try await checkpoint.rollback(to: marker)
            let rows = try await TransactionRangeCollection.collect(
                using: transaction, from: .firstGreaterOrEqual([0]), to: .firstGreaterOrEqual([0x60]),
                limit: 0, reverse: false, snapshot: false, streamingMode: .iterator
            )
            try require(rows.map(\.0) == [[0x10], [0x30], [0x50]], "CP-02", "rollback must restore the raw view")
            try await transaction.commit()
            let absent = try await engine.withTransaction { try await $0.getValue(for: [0x20]) }
            try require(absent == nil, "CP-02", "discarded rows must not be replayed")
        }
    }

    public func verifyDirectoryRollback() async throws {
        try await withEngine { engine in
            try await engine.withTransaction { transaction in
                _ = try await engine.directoryAccess.openOrInitializeRoot(transaction: transaction)
            }
            let transaction = try self.transaction(engine)
            let access = try self.checkpointAccess(transaction)
            guard let root = try await engine.directoryAccess.openRoot(transaction: transaction) else {
                throw DirectoryConformanceFailure(step: "CP-03", message: "root is absent")
            }
            let marker = try await access.createCheckpoint()
            let discarded = try await engine.directoryAccess.openOrCreatePartition("ephemeral", in: root, transaction: transaction)
            let lease = try await engine.leasePartition(discarded, transaction: transaction)
            let key = discarded.root.root.prefix.appending(0x6B)
            try transaction.setValue([9], for: key)
            try await access.rollback(to: marker)
            let replacement = try await engine.directoryAccess.openOrCreatePartition("ephemeral", in: root, transaction: transaction)
            try await requireStale {
                _ = try await engine.leasePartition(discarded, transaction: transaction)
            }
            do {
                _ = try await lease.withReadAccess(transaction) { try await $0.getValue(for: key) }
                throw DirectoryConformanceFailure(step: "CP-03", message: "discarded lease rebound")
            } catch let error as StorageError where error.code == .staleLease {}
            let oldParent = discarded.root
            let catalog = engine.directoryAccess
            try await requireStale { _ = try await catalog.open("x", expecting: nil, in: oldParent, transaction: transaction) }
            try await requireStale { _ = try await catalog.openOrCreateDirectory("x", in: oldParent, transaction: transaction) }
            try await requireStale { _ = try await catalog.listChildren(in: oldParent, after: nil, limit: 1, transaction: transaction) }
            try await requireStale { try await catalog.remove("x", in: oldParent, transaction: transaction) }
            try await requireStale { _ = try await catalog.move("x", in: oldParent, to: "y", in: root, transaction: transaction) }
            try await requireStale { _ = try await catalog.move("x", in: root, to: "y", in: oldParent, transaction: transaction) }
            let nested = try await catalog.openOrCreatePartition("nested", in: replacement.root, transaction: transaction)
            _ = try await catalog.openOrCreateDirectory("leaf", in: nested.root, transaction: transaction)
            lease.release()
            try await transaction.commit()
            let persisted = try await engine.withTransaction { transaction in
                let reopened = try await catalog.openDirectory(at: nested.root.address, transaction: transaction)
                return (reopened != nil, try await transaction.getValue(for: key))
            }
            try require(persisted.0 && persisted.1 == nil, "CP-03", "metadata must commit while discarded content stays absent")
        }
    }

    public func verifyConflicts() async throws {
        try await withEngine { engine in
            let transaction = try self.transaction(engine)
            let access = try self.checkpointAccess(transaction)
            let marker = try await access.createCheckpoint()
            if transaction.capabilities.explicitConflictRanges {
                try transaction.addConflictRange(beginKey: [0x10], endKey: [0x20], type: .write)
            }
            try transaction.setValue([1], for: [0x11])
            try await access.rollback(to: marker)
            try await engine.withTransaction { try $0.setValue([8], for: [0x11]) }
            try transaction.setValue([2], for: [0x30])
            try await transaction.commit()
            let persisted = try await engine.withTransaction { transaction in
                (try await transaction.getValue(for: [0x11]), try await transaction.getValue(for: [0x30]))
            }
            try require(persisted.0 == [8] && persisted.1 == [2], "CP-04", "discarded writes/conflicts must not replace concurrent data")
        }
    }

    public func verifyLifecycle() async throws {
        try await withEngine { engine in
            let transaction = try self.transaction(engine)
            let access = try self.checkpointAccess(transaction)
            let marker = try await access.createCheckpoint()
            try transaction.setValue([1], for: [1])
            var cursor = transaction.rangeCursor(from: .firstGreaterOrEqual([0]), to: .firstGreaterOrEqual([9]), limit: 0, reverse: false, snapshot: false, streamingMode: .iterator)
            _ = try await cursor.next()
            try await access.rollback(to: marker)
            do {
                let row = try await cursor.next()
                try require(row == nil, "CP-05", "a closed cursor must not return a discarded row")
            } catch is TransactionCheckpointError {}
            try await cursor.finish()
            try await transaction.cancel()
            do {
                _ = try await transaction.getValue(for: [1])
                throw DirectoryConformanceFailure(step: "CP-05", message: "cancelled transaction remained readable")
            } catch is TransactionCheckpointError {} catch is StorageError {}
        }
    }

    public func verifyBudgets() async throws {
        try await withEngine { engine in
            let transaction = try engine.createOwnedTransaction()
            try transaction.configureMutationByteLimit(maximumBytes: 20)
            try transaction.configureCheckpoints(limits: limits(maximumEntries: 4, maximumDepth: 1))
            let access = try self.checkpointAccess(transaction)
            let marker = try await access.createCheckpoint()
            try await requireCheckpointFailure { _ = try await access.createCheckpoint() }
            try transaction.setValue([1], for: [1])
            try await access.rollback(to: marker)
            do {
                try transaction.setValue([2], for: [2])
                throw DirectoryConformanceFailure(step: "CP-06", message: "rollback refunded cumulative mutation admission")
            } catch is TransactionMutationByteLimitError {}
            try await transaction.cancel()

            let exhausted = try engine.createOwnedTransaction()
            try exhausted.configureCheckpoints(limits: TransactionCheckpointLimits(
                maximumEntries: 4, maximumDepth: 1, maximumRetainedBytes: 64,
                maximumWorkUnits: 1, maximumReadPageRows: 1
            ))
            let exhaustedAccess = try self.checkpointAccess(exhausted)
            _ = try await exhaustedAccess.createCheckpoint()
            let expected = TransactionCheckpointError.limitExceeded(
                resource: .workUnits, actual: 2, maximum: 1
            )
            for _ in 0..<2 {
                do {
                    try await exhausted.commit()
                    throw DirectoryConformanceFailure(step: "CP-06", message: "exhausted commit unexpectedly succeeded")
                } catch let error as TransactionCheckpointError {
                    try require(error == expected, "CP-06", "commit lost its original work-limit payload")
                }
            }
            try require(exhausted.storageFailure == nil, "CP-06", "checkpoint failure fabricated a backend failure")
            do {
                _ = try await exhausted.getValue(for: [1])
                throw DirectoryConformanceFailure(step: "CP-06", message: "failed transaction remained readable")
            } catch let error as TransactionCheckpointError {
                try require(error == expected, "CP-06", "failed transaction lost terminal admission cause")
            }
            try await exhausted.cancel()
            do {
                _ = try await exhaustedAccess.createCheckpoint()
                throw DirectoryConformanceFailure(step: "CP-06", message: "cancelled journal remained open")
            } catch let error as TransactionCheckpointError {
                // Exhausted work is cumulative even after authority closes.
                try require(error == .closed || error == expected, "CP-06", "cancelled journal changed failure ownership")
            }
        }
    }

    private func withEngine(_ operation: (Engine) async throws -> Void) async throws {
        let engine = try await makeEngine()
        do { try await operation(engine); await engine.shutdown() }
        catch { await engine.shutdown(); throw error }
    }

    private func require(_ condition: Bool, _ step: String, _ message: String) throws {
        guard condition else { throw DirectoryConformanceFailure(step: step, message: message) }
    }

    private func requireCheckpointFailure(_ operation: () async throws -> Void) async throws {
        do {
            try await operation()
            throw DirectoryConformanceFailure(step: "checkpoint identity", message: "operation unexpectedly succeeded")
        } catch is TransactionCheckpointError {}
    }

    private func requireStale(_ operation: () async throws -> Void) async throws {
        do {
            try await operation()
            throw DirectoryConformanceFailure(step: "checkpoint provenance", message: "discarded value remained usable")
        } catch let error as StorageError where error.code == .staleLease {}
    }
}
