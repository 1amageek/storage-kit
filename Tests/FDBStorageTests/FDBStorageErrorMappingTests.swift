import FoundationDB
import StorageKit
import Testing
@testable import FDBStorage

@Suite("FoundationDB Error Mapping Tests")
struct FDBStorageErrorMappingTests {
    @Test func numericCodesPreserveRetryAndCertaintySemantics() {
        let cases: [(Int32, StorageError.Code, StorageRetryDisposition)] = [
            (1007, .transactionTooOld, .safe),
            (1009, .transactionFutureVersion, .safe),
            (1020, .transactionConflict, .safe),
            (1021, .backendContractViolation, .never),
            (1022, .backendContractViolation, .never),
            (1004, .transactionTimedOut, .safe),
            (1031, .transactionTimedOut, .safe),
            (1025, .transactionCancelled, .never),
            (1101, .transactionCancelled, .never),
            (1026, .connectionFailure, .safe),
            (1049, .connectionFailure, .safe),
            (1039, .connectionFailure, .safe),
            (1037, .transactionBusy, .safe),
            (1038, .transactionBusy, .safe),
            (1042, .transactionBusy, .safe),
            (1051, .transactionBusy, .safe),
            (1078, .transactionBusy, .safe),
            (1213, .transactionBusy, .safe),
            (1223, .transactionBusy, .safe),
            (2101, .transactionTooLarge, .never),
            (2102, .keyTooLarge, .never),
            (2103, .valueTooLarge, .never),
            (9999, .backendFailure, .never),
        ]

        for (numericCode, expectedCode, expectedDisposition) in cases {
            let mapped = FDBStorageTransaction.convertFDBError(
                FDBError(code: numericCode),
                operation: .read
            )
            #expect(mapped.code == expectedCode)
            #expect(mapped.retryDisposition == expectedDisposition)
            #expect(mapped.backend == .foundationDB)
            #expect(mapped.backendCode == numericCode)
        }
    }

    @Test func ambiguousCommitFailuresNeverBecomeSafeRetries() {
        let ambiguousCodes: [Int32] = [
            1004, 1021, 1022, 1025, 1026, 1031, 1039, 1049, 1101, 9999,
        ]

        for numericCode in ambiguousCodes {
            let mapped = FDBStorageTransaction.convertFDBError(
                FDBError(code: numericCode),
                operation: .commit
            )
            #expect(mapped.code == .commitUnknownResult)
            #expect(mapped.retryDisposition == .requiresIdempotency)
            #expect(!mapped.isRetryable)
            #expect(mapped.backendCode == numericCode)
        }
    }

    @Test func deterministicSizeLimitsStayCertainDuringCommit() {
        let cases: [(FDBErrorCode, StorageError.Code)] = [
            (.transactionTooLarge, .transactionTooLarge),
            (.keyTooLarge, .keyTooLarge),
            (.valueTooLarge, .valueTooLarge),
        ]

        for (fdbCode, expectedCode) in cases {
            let mapped = FDBStorageTransaction.convertFDBError(
                FDBError(fdbCode),
                operation: .commit
            )
            #expect(mapped.code == expectedCode)
            #expect(mapped.retryDisposition == .never)
            #expect(!mapped.isRetryable)
            #expect(mapped.backendCode == fdbCode.rawValue)
        }
    }

    @Test func declaredCapabilitiesMatchSupportedFoundationDBOperations() {
        let capabilities = FDBStorageTransaction.declaredCapabilities

        #expect(capabilities.transactionTimeout)
        #expect(capabilities.schedulingPriority)
        #expect(capabilities.readPriority)
        #expect(capabilities.readCacheControl)
        #expect(capabilities.systemKeyAccess)
        #expect(capabilities.historicalReadVersion)
        #expect(capabilities.readVersion)
        #expect(capabilities.committedVersion)
        #expect(capabilities.explicitConflictRanges)
        #expect(capabilities.committedVersionstamp)
        #expect(capabilities.versionstampedMutations)
    }
}
