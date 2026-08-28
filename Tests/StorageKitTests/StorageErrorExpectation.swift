import StorageKit
import Testing

/// Runs `body` and records an issue unless it throws a `StorageError` with `code`.
@discardableResult
func expectStorageError(
    _ code: StorageError.Code,
    sourceLocation: SourceLocation = #_sourceLocation,
    _ body: () async throws -> Void
) async -> StorageError? {
    do {
        try await body()
        Issue.record("Expected StorageError \(code) but the operation succeeded", sourceLocation: sourceLocation)
        return nil
    } catch let error as StorageError {
        #expect(error.code == code, "Expected \(code) but got \(error.code): \(error.message)", sourceLocation: sourceLocation)
        return error
    } catch {
        Issue.record("Expected StorageError \(code) but got \(error)", sourceLocation: sourceLocation)
        return nil
    }
}
