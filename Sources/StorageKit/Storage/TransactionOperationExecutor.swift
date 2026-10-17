/// Restores typed results above the existential operation-scope boundary.
public struct TransactionOperationExecutor: Sendable {
    private let access: any TransactionReadAccess

    public init(access: any TransactionReadAccess) {
        self.access = access
    }

    public func withOperation<Result: Sendable & Copyable>(
        _ operation: @escaping @Sendable () async throws -> Result
    ) async throws -> Result {
        let publication = TransactionOperationPublication<Result>()
        try await access.performOperation {
            await publication.publish(try await operation())
        }
        return await publication.result()
    }
}

private actor TransactionOperationPublication<Result: Sendable & Copyable> {
    private var value: Result?

    func publish(_ value: Result) {
        precondition(self.value == nil)
        self.value = value
    }

    func result() -> Result {
        guard let value else { preconditionFailure("Operation result was not published") }
        return value
    }
}
