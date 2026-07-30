import DatabaseTypes
import StorageKit
import Synchronization

/// SQLite transaction with synchronous mutation buffering and lazy async I/O.
///
/// Every async database operation flushes the transaction-owned mutation buffer
/// into the current native SQLite transaction/savepoint before reading. Nested
/// children suspend their parent until strict-LIFO `commit()` or `cancel()`.
public final class SQLiteStorageTransaction:
    Transaction,
    StorageCompactionTransaction,
    Sendable {
    public static let maximumCompactionWorkUnitsPerSlice: UInt64 = 4_096

    public typealias RangeResult = SQLiteRangeResult

    public static let declaredCapabilities = TransactionCapabilities.none
    public var capabilities: TransactionCapabilities { Self.declaredCapabilities }
    public var compaction: StorageCompactionAccess? {
        StorageCompactionAccess(limits: compactionLimits) {
            [self] maximumWorkUnits, continuation in
            try await stageCompactionSlice(
                maximumWorkUnits: maximumWorkUnits,
                continuation: continuation
            )
        }
    }
    public var mutationByteLimit: Int? { mutationByteMeter.maximumBytes }

    private enum Lifecycle: Sendable {
        case open
        case committing(TransactionOperationCompletion)
        case committed
        case cancelling(TransactionOperationCompletion)
        case cancelled
        case failed(StorageError, cleanupRequired: Bool)
    }

    private struct MutableState: Sendable {
        var writes: [SQLiteWriteOperation] = []
        var lifecycle: Lifecycle = .open
        var activeChildIdentifier: UInt64?
        var activeRangeRegistrations: Set<UInt64> = []
        var nextRangeRegistrationIdentifier: UInt64 = 1
    }

    private let identifier: UInt64
    private let rootIdentifier: UInt64
    private let parent: SQLiteStorageTransaction?
    private let coordinator: SQLiteTransactionCoordinator
    private let connection: SQLiteConnectionHandle
    private let lifetime: SQLiteStorageLifetime
    private let mutationByteMeter: TransactionMutationByteMeter
    private let state = Mutex(MutableState())
    public let transactionDomain: StorageTransactionDomain

    public var storageFailure: StorageError? {
        state.withLock { state in
            guard case .failed(let error, _) = state.lifecycle else {
                return nil
            }
            return error
        }
    }

    init(
        identifier: UInt64,
        coordinator: SQLiteTransactionCoordinator,
        connection: SQLiteConnectionHandle,
        lifetime: SQLiteStorageLifetime,
        transactionDomain: StorageTransactionDomain
    ) {
        self.identifier = identifier
        self.rootIdentifier = identifier
        self.parent = nil
        self.coordinator = coordinator
        self.connection = connection
        self.lifetime = lifetime
        self.mutationByteMeter = TransactionMutationByteMeter()
        self.transactionDomain = transactionDomain
    }

    private init(
        identifier: UInt64,
        parent: SQLiteStorageTransaction
    ) {
        self.identifier = identifier
        self.rootIdentifier = parent.rootIdentifier
        self.parent = parent
        self.coordinator = parent.coordinator
        self.connection = parent.connection
        self.lifetime = parent.lifetime
        self.mutationByteMeter = parent.mutationByteMeter
        self.transactionDomain = parent.transactionDomain
    }

    public func configureMutationByteLimit(maximumBytes: Int?) throws {
        try mutationByteMeter.configure(maximumBytes: maximumBytes)
    }

    deinit {
        let coordinator = coordinator
        let identifier = identifier
        let rootIdentifier = rootIdentifier
        let parent = parent
        let cleanupRequired = state.withLock { state in
            switch state.lifecycle {
            case .open, .failed(_, cleanupRequired: true):
                return true
            case .committing, .committed, .cancelling, .cancelled,
                 .failed(_, cleanupRequired: false):
                return false
            }
        }
        Task {
            if cleanupRequired {
                if let parent {
                    let outcome = await coordinator.cancelChild(
                        rootIdentifier: rootIdentifier,
                        parentIdentifier: parent.identifier,
                        childIdentifier: identifier
                    )
                    parent.resolveAbandonedChild(
                        identifier: identifier,
                        outcome: outcome
                    )
                } else {
                    await coordinator.abandonRoot(identifier: identifier)
                }
            }
            await coordinator.retireTerminalIdentifier(identifier)
        }
    }

    var hasActiveChild: Bool {
        state.withLock { $0.activeChildIdentifier != nil }
    }

    func makeChild(identifier: UInt64) throws -> SQLiteStorageTransaction {
        try state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: .beginTransaction)
            guard state.activeChildIdentifier == nil else {
                throw Self.invalidState(
                    "SQLite nested transactions must be created in strict LIFO order",
                    operation: .beginTransaction
                )
            }
            guard state.activeRangeRegistrations.isEmpty else {
                throw Self.invalidState(
                    "SQLite transaction cannot create a child while a range cursor is open",
                    operation: .beginTransaction
                )
            }
            state.activeChildIdentifier = identifier
        }
        return SQLiteStorageTransaction(identifier: identifier, parent: self)
    }

    // MARK: - Read

    public func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        let writes = try takeWrites(operation: .read)
        do {
            try await ensureBackendStarted()
            return try await coordinator.readValue(
                rootIdentifier: rootIdentifier,
                transactionIdentifier: identifier,
                writes: writes,
                key: key
            )
        } catch {
            throw recordFailure(error, operation: .read)
        }
    }

    public func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        let plan: SQLiteKeySelectionPlan
        do {
            plan = try SQLiteKeySelectionPlan(selector: selector)
        } catch {
            throw map(error, operation: .rangeRead)
        }

        let writes = try takeWrites(operation: .rangeRead)
        do {
            try await ensureBackendStarted()
            return try await coordinator.readKey(
                rootIdentifier: rootIdentifier,
                transactionIdentifier: identifier,
                writes: writes,
                plan: plan
            )
        } catch {
            throw recordFailure(error, operation: .rangeRead)
        }
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> SQLiteRangeResult {
        do {
            try state.withLock { state in
                try Self.validateReadable(
                    state,
                    operation: .rangeRead
                )
            }
            guard limit >= 0 else {
                throw Self.invalidState(
                    "SQLite range limit must not be negative",
                    operation: .rangeRead
                )
            }
            return SQLiteRangeResult(
                transaction: self,
                plan: SQLiteRangeScanPlan(
                    begin: try SQLRangeBoundary.begin(begin),
                    end: try SQLRangeBoundary.end(end),
                    limit: limit,
                    reverse: reverse
                )
            )
        } catch {
            return SQLiteRangeResult(
                error: map(error, operation: .rangeRead)
            )
        }
    }

    func openRange(plan: SQLiteRangeScanPlan) async throws -> SQLiteOpenedRange {
        let acquired = try registerRangeAndTakeWrites()
        do {
            try await ensureBackendStarted()
            let opened = try await coordinator.openRange(
                rootIdentifier: rootIdentifier,
                transactionIdentifier: identifier,
                writes: acquired.1,
                plan: plan
            )
            if opened.first == nil {
                unregisterRange(acquired.0)
            }
            return SQLiteOpenedRange(
                registrationIdentifier: acquired.0,
                cursorIdentifier: opened.cursorIdentifier,
                first: opened.first
            )
        } catch {
            unregisterRange(acquired.0)
            throw recordFailure(error, operation: .rangeRead)
        }
    }

    func nextRange(
        registrationIdentifier: UInt64,
        cursorIdentifier: UInt64
    ) async throws -> (ByteString, ByteString)? {
        try state.withLock { state in
            try Self.validateReadable(state, operation: .rangeRead)
            guard state.activeRangeRegistrations.contains(
                registrationIdentifier
            ) else {
                throw Self.invalidState(
                    "SQLite range iterator is not registered",
                    operation: .rangeRead
                )
            }
        }
        do {
            let row = try await coordinator.nextRange(
                rootIdentifier: rootIdentifier,
                transactionIdentifier: identifier,
                cursorIdentifier: cursorIdentifier
            )
            if row == nil {
                unregisterRange(registrationIdentifier)
            }
            return row
        } catch {
            abandonRange(
                registrationIdentifier: registrationIdentifier,
                cursorIdentifier: cursorIdentifier
            )
            throw recordFailure(error, operation: .rangeRead)
        }
    }

    func abandonRange(
        registrationIdentifier: UInt64,
        cursorIdentifier: UInt64
    ) {
        connection.closeRangeCursor(identifier: cursorIdentifier)
        unregisterRange(registrationIdentifier)
    }

    // MARK: - Write

    public func setValue(_ value: ByteString, for key: ByteString) throws {
        try appendAdmittedWrite(operation: .write) {
            try mutationByteMeter.recordSet(key: key, value: value)
        } makeWrite: {
            .set(key: key, value: value)
        }
    }

    public func clear(key: ByteString) throws {
        try appendAdmittedWrite(operation: .delete) {
            try mutationByteMeter.recordClear(key: key)
        } makeWrite: {
            .clear(key: key)
        }
    }

    public func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        try appendAdmittedWrite(operation: .deleteRange) {
            try mutationByteMeter.recordClearRange(
                beginKey: beginKey,
                endKey: endKey
            )
        } makeWrite: {
            .clearRange(begin: beginKey, end: endKey)
        }
    }

    public func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        try appendAdmittedWrite(operation: .write) {
            try mutationByteMeter.recordAtomic(
                key: key,
                parameter: param
            )
        } makeWrite: {
            .atomic(
                key: key,
                parameter: param,
                mutationType: mutationType
            )
        }
    }

    // MARK: - Physical Maintenance

    public var compactionLimits: StorageCompactionLimits {
        StorageCompactionLimits(
            maximumWorkUnitsPerSlice:
                Self.maximumCompactionWorkUnitsPerSlice
        )
    }

    public func stageCompactionSlice(
        maximumWorkUnits: UInt64,
        continuation: StorageCompactionContinuation?
    ) async throws -> StorageCompactionResult {
        let maximum = Self.maximumCompactionWorkUnitsPerSlice
        guard maximumWorkUnits > 0, maximumWorkUnits <= maximum else {
            throw StorageCompactionError.invalidMaximumWorkUnits(
                actual: maximumWorkUnits,
                maximum: maximum
            )
        }
        guard parent == nil else {
            throw StorageCompactionError.nestedTransaction
        }
        if let continuation {
            try SQLiteIncrementalCompactionToken.validate(
                continuation
            )
        }

        let writes = try takeWrites(operation: .execute)
        do {
            try await ensureBackendStarted()
            let metrics = try await coordinator.compact(
                rootIdentifier: rootIdentifier,
                transactionIdentifier: identifier,
                writes: writes,
                maximumWorkUnits: maximumWorkUnits
            )
            guard metrics.freePagesAfter <= metrics.freePagesBefore else {
                throw StorageCompactionError.backendFailure(
                    description:
                        "SQLite freelist grew during a transaction-scoped compaction slice"
                )
            }
            let consumed = metrics.freePagesBefore - metrics.freePagesAfter
            if metrics.freePagesBefore > 0, consumed == 0 {
                throw StorageCompactionError.backendMadeNoProgress(
                    remainingWorkUnits: metrics.freePagesAfter
                )
            }
            return StorageCompactionResult(
                workUnitsConsumed: consumed,
                remainingWorkUnits: metrics.freePagesAfter,
                continuation: metrics.freePagesAfter == 0
                    ? nil
                    : SQLiteIncrementalCompactionToken.current
            )
        } catch let error as StorageCompactionError {
            throw error
        } catch {
            throw recordFailure(error, operation: .execute)
        }
    }

    // MARK: - Terminal Operations

    public func commit() async throws {
        enum Start {
            case leader(TransactionOperationCompletion)
            case waitForCommit(TransactionOperationCompletion)
            case waitForCancellation(TransactionOperationCompletion)
            case committed
            case cancelled
            case failed(StorageError)
        }

        let start = state.withLock { state -> Start in
            if state.activeChildIdentifier != nil {
                return .failed(Self.invalidState(
                    "SQLite transaction cannot commit while its child is active",
                    operation: .commit
                ))
            }
            switch state.lifecycle {
            case .open:
                let completion = TransactionOperationCompletion()
                state.lifecycle = .committing(completion)
                return .leader(completion)
            case .committing(let completion):
                return .waitForCommit(completion)
            case .committed:
                return .committed
            case .cancelling(let completion):
                return .waitForCancellation(completion)
            case .cancelled:
                return .cancelled
            case .failed(let error, _):
                return .failed(error)
            }
        }

        switch start {
        case .leader(let completion):
            let result = await performCommit()
            switch result {
            case .success:
                completion.succeed()
            case .failure(let error):
                completion.fail(error)
            }
            try result.get()
        case .waitForCommit(let completion):
            try await completion.wait()
        case .waitForCancellation(let completion):
            try await completion.wait()
            throw Self.invalidState(
                "SQLite transaction was cancelled",
                operation: .commit
            )
        case .committed:
            return
        case .cancelled:
            throw Self.invalidState(
                "SQLite transaction was cancelled",
                operation: .commit
            )
        case .failed(let error):
            throw error
        }
    }

    public func cancel() async throws {
        enum Start {
            case leader(TransactionOperationCompletion)
            case waitForCancellation(TransactionOperationCompletion)
            case waitForCommit(TransactionOperationCompletion)
            case cancelled
            case committed
            case cleanedFailure
            case failed(StorageError)
        }

        let start = state.withLock { state -> Start in
            if state.activeChildIdentifier != nil {
                return .failed(Self.invalidState(
                    "SQLite transaction cannot cancel while its child is active",
                    operation: .cancel
                ))
            }
            switch state.lifecycle {
            case .open:
                let completion = TransactionOperationCompletion()
                state.lifecycle = .cancelling(completion)
                state.writes.removeAll(keepingCapacity: false)
                return .leader(completion)
            case .committing(let completion):
                return .waitForCommit(completion)
            case .committed:
                return .committed
            case .cancelling(let completion):
                return .waitForCancellation(completion)
            case .cancelled:
                return .cancelled
            case .failed(_, let cleanupRequired):
                guard cleanupRequired else { return .cleanedFailure }
                let completion = TransactionOperationCompletion()
                state.lifecycle = .cancelling(completion)
                state.writes.removeAll(keepingCapacity: false)
                return .leader(completion)
            }
        }

        switch start {
        case .leader(let completion):
            let result = await performCancellation()
            switch result {
            case .success:
                completion.succeed()
            case .failure(let error):
                completion.fail(error)
            }
            try result.get()
        case .waitForCancellation(let completion):
            try await completion.wait()
        case .waitForCommit(let completion):
            do {
                try await completion.wait()
            } catch {
                return
            }
            throw Self.invalidState(
                "SQLite transaction was committed",
                operation: .cancel
            )
        case .cancelled, .cleanedFailure:
            return
        case .committed:
            throw Self.invalidState(
                "SQLite transaction was committed",
                operation: .cancel
            )
        case .failed(let error):
            throw error
        }
    }

    private func performCommit() async -> Result<Void, StorageError> {
        let writes = state.withLock { state -> [SQLiteWriteOperation] in
            let writes = state.writes
            state.writes.removeAll(keepingCapacity: false)
            state.activeRangeRegistrations.removeAll(keepingCapacity: false)
            return writes
        }

        if let parent {
            do {
                try await ensureBackendStarted()
            } catch {
                let mapped = map(error, operation: .commit)
                state.withLock {
                    $0.lifecycle = .failed(mapped, cleanupRequired: true)
                }
                return .failure(mapped)
            }
            let outcome = await coordinator.commitChild(
                rootIdentifier: rootIdentifier,
                childIdentifier: identifier,
                writes: writes
            )
            if outcome.parentResumed {
                do {
                    try parent.releaseChild(identifier: identifier)
                } catch {
                    let mapped = map(error, operation: .commit)
                    state.withLock {
                        $0.lifecycle = .failed(mapped, cleanupRequired: false)
                    }
                    return .failure(mapped)
                }
            } else if let error = outcome.error {
                parent.recordDescendantFatalFailure(error)
            }
            if let error = outcome.error {
                state.withLock {
                    $0.lifecycle = .failed(
                        error,
                        cleanupRequired: !outcome.parentResumed
                    )
                }
                return .failure(error)
            }
            state.withLock { $0.lifecycle = .committed }
            return .success(())
        }

        let error = await coordinator.commitRoot(
            identifier: identifier,
            writes: writes
        )
        if let error {
            state.withLock {
                $0.lifecycle = .failed(error, cleanupRequired: false)
            }
            return .failure(error)
        }
        state.withLock { $0.lifecycle = .committed }
        return .success(())
    }

    private func performCancellation() async -> Result<Void, StorageError> {
        state.withLock {
            $0.activeRangeRegistrations.removeAll(keepingCapacity: false)
        }
        if let parent {
            let outcome = await coordinator.cancelChild(
                rootIdentifier: rootIdentifier,
                parentIdentifier: parent.identifier,
                childIdentifier: identifier
            )
            if outcome.parentResumed {
                do {
                    try parent.releaseChild(identifier: identifier)
                } catch {
                    let mapped = map(error, operation: .cancel)
                    state.withLock {
                        $0.lifecycle = .failed(mapped, cleanupRequired: false)
                    }
                    return .failure(mapped)
                }
            } else if let error = outcome.error {
                parent.recordDescendantFatalFailure(error)
            }
            if let error = outcome.error {
                state.withLock {
                    $0.lifecycle = .failed(
                        error,
                        cleanupRequired: !outcome.parentResumed
                    )
                }
                return .failure(error)
            }
            state.withLock { $0.lifecycle = .cancelled }
            return .success(())
        }

        let error = await coordinator.cancelRoot(identifier: identifier)
        if let error {
            state.withLock {
                $0.lifecycle = .failed(error, cleanupRequired: false)
            }
            return .failure(error)
        }
        state.withLock { $0.lifecycle = .cancelled }
        return .success(())
    }

    // MARK: - Transaction Lifecycle and Backend Access

    private func ensureBackendStarted() async throws {
        guard !lifetime.isClosed else {
            throw Self.invalidState(
                "SQLite storage engine is closed",
                operation: .beginTransaction
            )
        }
        guard let parent else {
            try await coordinator.beginRoot(identifier: identifier)
            return
        }

        let parentWrites: [SQLiteWriteOperation]
        do {
            parentWrites = try parent.takeWritesForChildStart(
                childIdentifier: identifier
            )
            try await parent.ensureBackendStarted()
            try await coordinator.beginChild(
                rootIdentifier: rootIdentifier,
                parentIdentifier: parent.identifier,
                childIdentifier: identifier,
                parentWrites: parentWrites
            )
        } catch {
            let mapped = map(error, operation: .beginTransaction)
            parent.recordDescendantFatalFailure(mapped)
            throw mapped
        }
    }

    private func appendAdmittedWrite(
        operation: StorageOperation,
        admit: () throws -> Void,
        makeWrite: () -> SQLiteWriteOperation
    ) throws {
        try state.withLock { state in
            try Self.validateOpen(state.lifecycle, operation: operation)
            guard state.activeChildIdentifier == nil else {
                throw Self.invalidState(
                    "SQLite parent transaction is suspended by its child",
                    operation: operation
                )
            }
            guard state.activeRangeRegistrations.isEmpty else {
                throw Self.invalidState(
                    "SQLite transaction cannot mutate while a range cursor is open",
                    operation: operation
                )
            }
            try admit()
            state.writes.append(makeWrite())
        }
    }

    private func takeWrites(
        operation: StorageOperation
    ) throws -> [SQLiteWriteOperation] {
        try state.withLock { state in
            try Self.validateReadable(state, operation: operation)
            let writes = state.writes
            state.writes.removeAll(keepingCapacity: false)
            return writes
        }
    }

    private func takeWritesForChildStart(
        childIdentifier: UInt64
    ) throws -> [SQLiteWriteOperation] {
        try state.withLock { state in
            switch state.lifecycle {
            case .open:
                break
            case .failed(let error, _):
                throw error
            default:
                throw Self.invalidState(
                    "SQLite parent transaction is not open",
                    operation: .beginTransaction
                )
            }
            guard state.activeChildIdentifier == childIdentifier else {
                throw Self.invalidState(
                    "SQLite child does not own the parent suspension",
                    operation: .beginTransaction
                )
            }
            let writes = state.writes
            state.writes.removeAll(keepingCapacity: false)
            return writes
        }
    }

    private func registerRangeAndTakeWrites()
        throws -> (UInt64, [SQLiteWriteOperation]) {
        try state.withLock { state in
            try Self.validateReadable(state, operation: .rangeRead)
            let registration = state.nextRangeRegistrationIdentifier
            let (next, overflow) = registration.addingReportingOverflow(1)
            guard !overflow else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .rangeRead,
                    backend: .sqlite,
                    message: "SQLite range registration space is exhausted"
                )
            }
            state.nextRangeRegistrationIdentifier = next
            state.activeRangeRegistrations.insert(registration)
            let writes = state.writes
            state.writes.removeAll(keepingCapacity: false)
            return (registration, writes)
        }
    }

    private func unregisterRange(_ registration: UInt64) {
        _ = state.withLock { state in
            state.activeRangeRegistrations.remove(registration)
        }
    }

    private func releaseChild(identifier: UInt64) throws {
        try state.withLock { state in
            guard state.activeChildIdentifier == identifier else {
                throw Self.invalidState(
                    "SQLite child completion violated strict LIFO ownership",
                    operation: .commit
                )
            }
            state.activeChildIdentifier = nil
        }
    }

    private func resolveAbandonedChild(
        identifier: UInt64,
        outcome: SQLiteChildTerminalOutcome
    ) {
        if outcome.parentResumed {
            do {
                try releaseChild(identifier: identifier)
            } catch {
                recordDescendantFatalFailure(
                    map(error, operation: .cancel)
                )
            }
        } else if let error = outcome.error {
            recordDescendantFatalFailure(error)
        }
    }

    private func recordDescendantFatalFailure(_ error: StorageError) {
        state.withLock { state in
            state.lifecycle = .failed(error, cleanupRequired: true)
            state.writes.removeAll(keepingCapacity: false)
        }
        parent?.recordDescendantFatalFailure(error)
    }

    private func recordFailure(
        _ error: any Error,
        operation: StorageOperation
    ) -> StorageError {
        let mapped = map(error, operation: operation)
        state.withLock { state in
            switch state.lifecycle {
            case .open, .committing:
                state.lifecycle = .failed(mapped, cleanupRequired: true)
                state.writes.removeAll(keepingCapacity: false)
            case .failed:
                break
            case .committed, .cancelling, .cancelled:
                break
            }
        }
        return mapped
    }

    private func map(
        _ error: any Error,
        operation: StorageOperation
    ) -> StorageError {
        if let storageError = error as? StorageError {
            if storageError.backend == .sqlite {
                return storageError
            }
            return StorageError(
                code: storageError.code,
                operation: operation,
                backend: .sqlite,
                message: storageError.message,
                backendCode: storageError.backendCode,
                underlyingDescription: storageError.underlyingDescription
            )
        }
        return StorageError(
            code: .backendFailure,
            operation: operation,
            backend: .sqlite,
            message: "SQLite transaction failed",
            underlyingDescription: String(describing: error)
        )
    }

    private static func validateReadable(
        _ state: MutableState,
        operation: StorageOperation
    ) throws {
        switch state.lifecycle {
        case .open:
            break
        case .committing:
            throw invalidState(
                "SQLite transaction is committing",
                operation: operation
            )
        case .committed:
            throw invalidState(
                "SQLite transaction was committed",
                operation: operation
            )
        case .cancelling:
            throw invalidState(
                "SQLite transaction is cancelling",
                operation: operation
            )
        case .cancelled:
            throw invalidState(
                "SQLite transaction was cancelled",
                operation: operation
            )
        case .failed(let error, _):
            throw error
        }
        guard state.activeChildIdentifier == nil else {
            throw invalidState(
                "SQLite parent transaction is suspended by its child",
                operation: operation
            )
        }
    }

    private static func validateOpen(
        _ lifecycle: Lifecycle,
        operation: StorageOperation
    ) throws {
        switch lifecycle {
        case .open:
            return
        case .committing:
            throw invalidState(
                "SQLite transaction is committing",
                operation: operation
            )
        case .committed:
            throw invalidState(
                "SQLite transaction was committed",
                operation: operation
            )
        case .cancelling:
            throw invalidState(
                "SQLite transaction is cancelling",
                operation: operation
            )
        case .cancelled:
            throw invalidState(
                "SQLite transaction was cancelled",
                operation: operation
            )
        case .failed(let error, _):
            throw error
        }
    }

    private static func invalidState(
        _ message: String,
        operation: StorageOperation
    ) -> StorageError {
        StorageError(
            code: .invalidOperation,
            operation: operation,
            backend: .sqlite,
            message: message
        )
    }
}

extension SQLiteStorageTransaction {
    public func getValue(for key: ByteString) async throws -> ByteString? {
        try await getValue(for: key, snapshot: false)
    }

    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        KeyValueCursor(
            consuming: getRange(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        )
    }

    public func collectRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) async throws -> [(ByteString, ByteString)] {
        try await TransactionRangeCollection.collect(
            using: self,
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
    }
}
