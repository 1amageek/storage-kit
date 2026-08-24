import DatabaseTypes
import Testing
@testable import StorageKit

@Suite("Transaction read access")
struct TransactionReadAccessTests {
    @Test("read-only capability preserves the admitted transaction view")
    func readOnlyCapabilityPreservesTransactionView() async throws {
        let engine = InMemoryEngine()
        try await engine.withTransaction { transaction in
            try transaction.setValue([0xA1], for: [0x01])
            try transaction.setValue([0xA2], for: [0x02])

            let access: any TransactionReadAccess = transaction

            #expect(access.transactionDomain === transaction.transactionDomain)
            #expect(try await access.getValue(for: [0x01]) == [0xA1])
            #expect(
                try await access.getKey(
                    selector: .firstGreaterOrEqual([0x02])
                ) == [0x02]
            )
            let rows = try await access.collectRange(
                begin: [0x01],
                end: [0x03]
            )
            #expect(rows.map(\.0) == [[0x01], [0x02]])
            #expect(rows.map(\.1) == [[0xA1], [0xA2]])
        }
    }
}
