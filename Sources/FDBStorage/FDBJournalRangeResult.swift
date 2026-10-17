import DatabaseTypes
import FoundationDB
import StorageKit

/// Native pages transfer bounded row views into the journal overlay.
struct FDBJournalRangeResult: TransactionRangeResult {
    typealias Element = (ByteString, ByteString)
    let native: any TransactionProtocol
    let range: TransactionJournalRange
    let reverse: Bool
    let snapshot: Bool
    let pageSize: Int
    let maximumByteCount: Int
    let includesSystemKeys: Bool

    func makeCursor() -> Cursor {
        Cursor(state: IterationState(native: native, range: range, reverse: reverse, snapshot: snapshot, pageSize: pageSize, maximumByteCount: maximumByteCount, includesSystemKeys: includesSystemKeys))
    }

    struct Cursor: TransactionRangeCursor {
        let state: IterationState
        func next() async throws -> Element? { try await state.next() }
        func finish(isolation actor: isolated (any Actor)?) async throws { await state.finish() }
    }

    actor IterationState {
        let native: any TransactionProtocol
        var begin: FDB.KeySelector
        var end: FDB.KeySelector
        let reverse: Bool
        let snapshot: Bool
        let pageSize: Int
        let maximumByteCount: Int
        var batch: [FDB.KeyValue] = []
        var index = 0
        var more = true
        var iteration = 1
        var closed = false

        init(native: any TransactionProtocol, range: TransactionJournalRange, reverse: Bool, snapshot: Bool, pageSize: Int, maximumByteCount: Int, includesSystemKeys: Bool) {
            self.native = native
            switch range.lower {
            case .unbounded: begin = .firstGreaterOrEqual(ByteString())
            case .inclusive(let key): begin = .firstGreaterOrEqual(key)
            case .exclusive(let key): begin = .firstGreaterThan(key)
            }
            switch range.upper {
            case .unbounded: end = .firstGreaterOrEqual(includesSystemKeys ? ByteString([0xff, 0xff]) : ByteString([0xff]))
            case .inclusive(let key): end = .firstGreaterThan(key)
            case .exclusive(let key): end = .firstGreaterOrEqual(key)
            }
            self.reverse = reverse
            self.snapshot = snapshot
            self.pageSize = pageSize
            self.maximumByteCount = maximumByteCount
        }

        func next() async throws -> Element? {
            guard !closed else { return nil }
            guard pageSize > 0, maximumByteCount >= 0 else {
                throw StorageError(code: .invalidOperation, operation: .rangeRead, backend: .foundationDB, message: "Invalid native checkpoint page limits")
            }
            while index == batch.count {
                batch.removeAll(keepingCapacity: false)
                index = 0
                guard more else { closed = true; return nil }
                let page: RangeBatch
                do {
                    page = try await native.readRangeBatch(from: begin, to: end, limit: pageSize, targetBytes: maximumByteCount, streamingMode: .iterator, iteration: iteration, reverse: reverse, snapshot: snapshot)
                } catch let error as FDBError {
                    throw FDBStorageTransaction.convertFDBError(error, operation: .rangeRead)
                }
                guard !closed else { return nil }
                batch = page.records
                more = page.hasMore
                iteration += 1
                if let last = batch.last {
                    if reverse { end = .firstGreaterOrEqual(last.key) }
                    else { begin = .firstGreaterThan(last.key) }
                } else if more {
                    throw StorageError(code: .backendContractViolation, operation: .rangeRead, backend: .foundationDB, message: "Native range returned an empty continuation page")
                }
            }
            let row = batch[index]
            let bytes = row.key.count.addingReportingOverflow(row.value.count)
            guard !bytes.overflow, bytes.partialValue <= maximumByteCount else {
                throw TransactionCheckpointError.limitExceeded(resource: .retainedBytes, actual: bytes.overflow ? Int.max : bytes.partialValue, maximum: maximumByteCount)
            }
            index += 1
            return (row.key, row.value)
        }

        func finish() {
            closed = true
            batch.removeAll(keepingCapacity: false)
        }
    }
}
