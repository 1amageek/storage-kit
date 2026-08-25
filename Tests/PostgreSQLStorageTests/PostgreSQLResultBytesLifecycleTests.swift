import DatabaseTypes
import NIOCore
import Synchronization
import Testing
@testable import PostgreSQLStorage
@testable import StorageKit

extension SerializedPostgreSQLStorageTests {
@Suite(
    "PostgreSQL Result Bytes Lifecycle",
    .serialized,
    .enabled(if: PostgreSQLTestEnvironment.isConfigured)
)
struct PostgreSQLResultBytesLifecycleTests {

    @Test("Production result owner remains inline and zero-copy")
    func productionResultOwnerRemainsInlineAndZeroCopy() throws {
        var buffer = ByteBufferAllocator().buffer(capacity: 4)
        buffer.writeBytes([0x11, 0x22, 0x33, 0x44])
        let sourceAddress = buffer.withUnsafeReadableBytes { bytes in
            UInt(bitPattern: bytes.baseAddress)
        }

        let result = PostgreSQLResultBytesFactory.production
            .makeByteString(retaining: buffer)

        #expect(result == ByteString([0x11, 0x22, 0x33, 0x44]))
        #expect(resultAddress(of: result) == sourceAddress)
        #expect(
            MemoryLayout<
                PostgreSQLResultBytesOwner<
                    PostgreSQLResultBytesNoObservation
                >
            >.size == MemoryLayout<ByteBuffer>.size
        )
        #expect(
            MemoryLayout<
                PostgreSQLResultBytesOwner<
                    PostgreSQLResultBytesNoObservation
                >
            >.size <= 3 * MemoryLayout<UInt>.size
        )
    }

    @Test("Point result view retains decoded storage until final release")
    func pointResultRetainsDecodedStorage() async throws {
        let lifecycle = PostgreSQLResultBytesLifecycleProbe()
        let engine = try await makeEngine(lifecycleObserver: lifecycle)
        defer { await engine.waitUntilShutdown() }

        let key: ByteString = [0x41]
        let value: ByteString = [0x11, 0x22, 0x33, 0x44]
        try await engine.withTransaction { transaction in
            try transaction.setValue(value, for: key)
        }

        var result = try await engine.withTransaction { transaction in
            try await transaction.getValue(for: key)
        }
        let retainedResultAddress = resultAddress(
            of: try #require(result)
        )
        var resultView: ByteString? = try {
            let bytes = try #require(result)
            return bytes[1..<3]
        }()
        let retained = lifecycle.snapshot()

        #expect(retained.createdOwners.count == 1)
        #expect(retained.releasedIdentifiers.isEmpty)
        #expect(retained.liveOwners.count == 1)
        #expect(retained.liveOwners.first?.readableByteCount == value.count)
        #expect(retained.liveOwners.first?.baseAddress == retainedResultAddress)
        #expect(retained.duplicateReleaseCount == 0)

        result = nil
        let retainedByView = lifecycle.snapshot()
        #expect(retainedByView.liveOwners.count == 1)
        #expect(retainedByView.releasedIdentifiers.isEmpty)
        #expect(resultView == ByteString([0x22, 0x33]))
        let retainedViewAddress = resultAddress(
            of: try #require(resultView)
        )
        #expect(retainedViewAddress == retainedResultAddress + 1)

        resultView = nil
        let released = lifecycle.snapshot()
        #expect(released.liveOwners.isEmpty)
        #expect(released.releasedIdentifiers.count == 1)
        #expect(released.duplicateReleaseCount == 0)
    }

    @Test("Cursor finish releases only the unreturned range owners")
    func cursorFinishReleasesOnlyUnreturnedRangeOwners() async throws {
        let lifecycle = PostgreSQLResultBytesLifecycleProbe()
        let engine = try await makeEngine(lifecycleObserver: lifecycle)
        defer { await engine.waitUntilShutdown() }

        try await engine.withTransaction { transaction in
            try transaction.setValue([0x11], for: [0x41])
            try transaction.setValue([0x22], for: [0x42])
            try transaction.setValue([0x33], for: [0x43])
        }

        var evidence = try await readRangeEvidence(
            engine: engine,
            lifecycle: lifecycle
        )

        #expect(evidence.beforeFinish.createdOwners.count == 6)
        #expect(evidence.beforeFinish.releasedIdentifiers.isEmpty)
        #expect(evidence.beforeFinish.liveOwners.count == 6)

        #expect(evidence.afterFinish.createdOwners.count == 6)
        #expect(evidence.afterFinish.releasedIdentifiers.count == 4)
        #expect(evidence.afterFinish.liveOwners.count == 2)
        #expect(evidence.afterFinish.duplicateReleaseCount == 0)

        let afterTransactionCompletion = lifecycle.snapshot()
        #expect(afterTransactionCompletion.liveOwners.count == 2)
        #expect(afterTransactionCompletion.releasedIdentifiers.count == 4)

        let returnedAddresses: Set<UInt>
        do {
            let returnedEntry = try #require(evidence.returnedEntry)
            returnedAddresses = resultAddresses(of: returnedEntry)
        }
        let liveAddresses = Set(
            evidence.afterFinish.liveOwners.compactMap(\.baseAddress)
        )
        #expect(liveAddresses == returnedAddresses)

        evidence.returnedEntry = nil
        let afterConsumerRelease = lifecycle.snapshot()
        #expect(afterConsumerRelease.liveOwners.isEmpty)
        #expect(afterConsumerRelease.releasedIdentifiers.count == 6)
        #expect(afterConsumerRelease.duplicateReleaseCount == 0)
    }

    @Test("Consumer failure releases every range result owner")
    func consumerFailureReleasesEveryRangeResultOwner() async throws {
        let lifecycle = PostgreSQLResultBytesLifecycleProbe()
        let engine = try await makeEngine(lifecycleObserver: lifecycle)
        defer { await engine.waitUntilShutdown() }

        try await engine.withTransaction { transaction in
            try transaction.setValue([0x11], for: [0x41])
            try transaction.setValue([0x22], for: [0x42])
            try transaction.setValue([0x33], for: [0x43])
        }

        await #expect(
            throws: PostgreSQLResultBytesLifecycleTestError.consumerFailure
        ) {
            try await engine.withTransaction { transaction in
                var cursor = transaction.rangeCursor(
                    from: .firstGreaterOrEqual([0x41]),
                    to: .firstGreaterOrEqual([0x44]),
                    limit: 0,
                    reverse: false,
                    snapshot: false,
                    streamingMode: .small
                )
                _ = try #require(try await cursor.next())
                #expect(lifecycle.snapshot().liveOwners.count == 6)
                throw PostgreSQLResultBytesLifecycleTestError.consumerFailure
            }
        }

        let released = lifecycle.snapshot()
        #expect(released.createdOwners.count == 6)
        #expect(released.liveOwners.isEmpty)
        #expect(released.releasedIdentifiers.count == 6)
        #expect(released.duplicateReleaseCount == 0)
    }

    private func readRangeEvidence(
        engine: PostgreSQLStorageEngine,
        lifecycle: PostgreSQLResultBytesLifecycleProbe
    ) async throws -> PostgreSQLRangeLifecycleEvidence {
        let transaction = try engine.createTransaction()
        var transactionFinished = false
        defer {
            if !transactionFinished {
                do {
                    try await transaction.cancel()
                } catch {
                    Issue.record(
                        "Failed to cancel lifecycle-evidence transaction: \(error)"
                    )
                }
            }
        }

        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual([0x41]),
            to: .firstGreaterOrEqual([0x44]),
            limit: 0,
            reverse: false,
            snapshot: false,
            streamingMode: .small
        )
        let entry = try #require(try await cursor.next())
        let beforeFinish = lifecycle.snapshot()
        try await cursor.finish()
        let afterFinish = lifecycle.snapshot()
        try await transaction.commit()
        transactionFinished = true
        return PostgreSQLRangeLifecycleEvidence(
            returnedEntry: entry,
            beforeFinish: beforeFinish,
            afterFinish: afterFinish
        )
    }

    private func makeEngine(
        lifecycleObserver:
            any PostgreSQLResultBytesLifecycleObserver
    ) async throws -> PostgreSQLStorageEngine {
        let engine = try await PostgreSQLStorageEngine(
            configuration: PostgreSQLTestEnvironment.makeConfiguration(),
            resultBytesLifecycleObserver: lifecycleObserver
        )
        try await engine.withTransaction { transaction in
            try transaction.clearRange(
                beginKey: [0x00],
                endKey: [0xFF, 0xFF]
            )
        }
        return engine
    }

    private func resultAddress(of bytes: ByteString) -> UInt {
        bytes.withUnsafeBytes { buffer in
            UInt(bitPattern: buffer.baseAddress)
        }
    }

    private func resultAddresses(
        of entry: (ByteString, ByteString)
    ) -> Set<UInt> {
        [
            resultAddress(of: entry.0),
            resultAddress(of: entry.1),
        ]
    }
}
}

private enum PostgreSQLResultBytesLifecycleTestError: Error, Equatable {
    case consumerFailure
}

private struct PostgreSQLRangeLifecycleEvidence: Sendable {
    var returnedEntry: (ByteString, ByteString)?
    let beforeFinish: PostgreSQLResultBytesLifecycleSnapshot
    let afterFinish: PostgreSQLResultBytesLifecycleSnapshot
}

private struct PostgreSQLResultBytesLifecycleRecord: Sendable {
    let identifier: UInt64
    let readableByteCount: Int
    let baseAddress: UInt?
}

private struct PostgreSQLResultBytesLifecycleSnapshot: Sendable {
    let createdOwners:
        [UInt64: PostgreSQLResultBytesLifecycleRecord]
    let releasedIdentifiers: Set<UInt64>
    let duplicateReleaseCount: Int

    var liveOwners: [PostgreSQLResultBytesLifecycleRecord] {
        createdOwners.values.filter {
            !releasedIdentifiers.contains($0.identifier)
        }
    }
}

private final class PostgreSQLResultBytesLifecycleProbe:
        PostgreSQLResultBytesLifecycleObserver,
        Sendable {
    private struct State: Sendable {
        var nextIdentifier: UInt64 = 1
        var createdOwners:
            [UInt64: PostgreSQLResultBytesLifecycleRecord] = [:]
        var releasedIdentifiers: Set<UInt64> = []
        var duplicateReleaseCount = 0
    }

    private let state = Mutex(State())

    func resultBytesOwnerCreated(
        readableByteCount: Int,
        baseAddress: UInt?
    ) -> UInt64 {
        state.withLock { state in
            let identifier = state.nextIdentifier
            state.nextIdentifier += 1
            state.createdOwners[identifier] =
                PostgreSQLResultBytesLifecycleRecord(
                    identifier: identifier,
                    readableByteCount: readableByteCount,
                    baseAddress: baseAddress
                )
            return identifier
        }
    }

    func resultBytesOwnerReleased(identifier: UInt64) {
        state.withLock { state in
            if !state.releasedIdentifiers.insert(identifier).inserted {
                state.duplicateReleaseCount += 1
            }
        }
    }

    func snapshot() -> PostgreSQLResultBytesLifecycleSnapshot {
        state.withLock { state in
            PostgreSQLResultBytesLifecycleSnapshot(
                createdOwners: state.createdOwners,
                releasedIdentifiers: state.releasedIdentifiers,
                duplicateReleaseCount: state.duplicateReleaseCount
            )
        }
    }
}
