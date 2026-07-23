/// Resolves one transaction versionstamp after its transaction commits.
public struct TransactionVersionstampRequest:
        PendingTransactionVersionstamp,
        Sendable {
    private enum Source: Sendable {
        case completion(TransactionVersionstampCompletion)
        case resolver(
            @Sendable () async throws -> TransactionVersionstamp
        )
    }

    private let source: Source

    package init(
        completion: TransactionVersionstampCompletion
    ) {
        self.source = .completion(completion)
    }

    package init(
        failure: StorageError
    ) {
        self.source = .completion(
            TransactionVersionstampCompletion(resolved: .failure(failure))
        )
    }

    package init(
        resolveValue: @escaping @Sendable () async throws -> TransactionVersionstamp
    ) {
        self.source = .resolver(resolveValue)
    }

    public var value: TransactionVersionstamp {
        get async throws {
            switch source {
            case .completion(let completion):
                return try await completion.wait().get()
            case .resolver(let resolveValue):
                return try await resolveValue()
            }
        }
    }
}
