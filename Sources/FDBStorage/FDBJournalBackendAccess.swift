import DatabaseTypes
import FoundationDB
import StorageKit

/// Reads the native snapshot without retaining or entering its transaction adapter.
struct FDBJournalBackendAccess: TransactionJournalBackendAccess {
    let native: any TransactionProtocol
    let includesSystemKeys: Bool

    func getValue(for key: ByteString, snapshot: Bool, maximumByteCount: Int) async throws -> ByteString? {
        guard maximumByteCount >= 0 else {
            throw StorageError.invalidPointReadMaximum(maximumByteCount, backend: .foundationDB)
        }
        guard let value = try await native.getValue(for: key, snapshot: snapshot) else { return nil }
        // The bindings retain the native result without copying. The bound is
        // enforced before that owner crosses into journal-owned workspace.
        guard value.count <= maximumByteCount else {
            throw StorageError.pointReadValueTooLarge(observedByteCount: value.count, maximumByteCount: maximumByteCount, backend: .foundationDB)
        }
        return value
    }

    func rangeCursor(in range: TransactionJournalRange, reverse: Bool, snapshot: Bool, pageSize: Int, maximumByteCount: Int) -> KeyValueCursor {
        KeyValueCursor(consuming: FDBJournalRangeResult(native: native, range: range, reverse: reverse, snapshot: snapshot, pageSize: pageSize, maximumByteCount: maximumByteCount, includesSystemKeys: includesSystemKeys))
    }
}
