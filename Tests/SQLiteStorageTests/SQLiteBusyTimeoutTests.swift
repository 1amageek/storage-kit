import DatabaseTypes
import Foundation
import Testing
@testable import StorageKit
@testable import SQLiteStorage

/// Verifies the two-layer contention contract: `PRAGMA busy_timeout` absorbs
/// micro-contention at the SQLite level, while longer contention still fails
/// fast with the typed, retryable `StorageError.transactionBusy`.
@Suite("SQLite Busy Timeout")
struct SQLiteBusyTimeoutTests {

    @Test func contentionReleasedWithinBusyTimeoutIsAbsorbed() async throws {
        let path = temporaryDatabasePath()
        defer { removeTemporaryDatabase(path) }

        let holder = try SQLiteStorageEngine(configuration: .file(path))
        let waiter = try SQLiteStorageEngine(
            configuration: .file(path, busyTimeoutMilliseconds: 5_000)
        )
        defer {
            holder.requestShutdown()
            waiter.requestShutdown()
        }

        let held = try holder.createTransaction()
        _ = try await held.getValue(for: [0x00])

        let release = Task {
            try await Task.sleep(for: .milliseconds(150))
            try await held.cancel()
        }

        // Begins while the holder still owns the write lock; the busy timeout
        // must carry the wait across the holder's release instead of failing.
        let contending = try waiter.createTransaction()
        let value = try await contending.getValue(for: [0x00])
        #expect(value == nil)

        try await release.value
        try await contending.cancel()
        await holder.shutdown()
        await waiter.shutdown()
    }

    @Test func zeroBusyTimeoutFailsImmediately() async throws {
        let path = temporaryDatabasePath()
        defer { removeTemporaryDatabase(path) }

        let holder = try SQLiteStorageEngine(configuration: .file(path))
        let failFast = try SQLiteStorageEngine(
            configuration: .file(path, busyTimeoutMilliseconds: 0)
        )
        defer {
            holder.requestShutdown()
            failFast.requestShutdown()
        }

        let held = try holder.createTransaction()
        _ = try await held.getValue(for: [0x00])

        let contending = try failFast.createTransaction()
        do {
            _ = try await contending.getValue(for: [0x00])
            Issue.record("Expected SQLite busy error")
        } catch let error as StorageError {
            #expect(error.code == .transactionBusy)
            #expect(error.isRetryable == true)
        }

        try await contending.cancel()
        try await held.cancel()
        await holder.shutdown()
        await failFast.shutdown()
    }

    @Test func negativeBusyTimeoutIsRejected() throws {
        let path = temporaryDatabasePath()
        defer { removeTemporaryDatabase(path) }

        do {
            _ = try SQLiteStorageEngine(
                configuration: .file(path, busyTimeoutMilliseconds: -1)
            )
            Issue.record("Expected a configuration validation error")
        } catch let error as StorageError {
            #expect(error.code == .invalidOperation)
            #expect(error.backend == .sqlite)
        }
    }

    private func temporaryDatabasePath() -> String {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("storage-kit-busy-\(UUID().uuidString).sqlite")
            .path
    }

    private func removeTemporaryDatabase(_ path: String) {
        let paths = [path, "\(path)-wal", "\(path)-shm"]
        for candidate in paths where FileManager.default.fileExists(atPath: candidate) {
            try? FileManager.default.removeItem(atPath: candidate)
        }
    }
}
