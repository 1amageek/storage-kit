import Testing
@testable import StorageKit

@Suite("Storage transaction cleanup error")
struct StorageTransactionCleanupErrorTests {
    private enum Failure: Error {
        case operation
        case firstCancellation
        case secondCancellation
    }

    @Test("Additional cancellation failures preserve the root operation")
    func preservesEveryFailure() throws {
        let first = StorageTransactionCleanupError(
            operationError: Failure.operation,
            cancellationError: Failure.firstCancellation
        )
        let combined = first.addingCancellationError(
            Failure.secondCancellation
        )

        #expect(combined.operationError as? Failure == .operation)
        #expect(combined.cancellationErrors.count == 2)
        #expect(
            combined.cancellationErrors[0] as? Failure
                == .firstCancellation
        )
        #expect(
            combined.cancellationErrors[1] as? Failure
                == .secondCancellation
        )
    }

    @Test("Nested cleanup errors flatten rather than hiding failures")
    func flattensNestedCleanupFailures() throws {
        let first = StorageTransactionCleanupError(
            operationError: Failure.operation,
            cancellationError: Failure.firstCancellation
        )
        let nested = StorageTransactionCleanupError(
            operationError: first,
            cancellationError: Failure.secondCancellation
        )

        #expect(nested.operationError as? Failure == .operation)
        #expect(nested.cancellationErrors.count == 2)
    }
}
