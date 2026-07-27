import DatabaseTypes
import FoundationDB

struct FixedTransactionVersionstamp: FDB.PendingTransactionVersionstamp {
    var value: FDB.TransactionVersionstamp {
        get async throws {
            try FDB.TransactionVersionstamp(
                bytes: FDB.ByteString([UInt8](repeating: 0, count: 10))
            )
        }
    }
}
