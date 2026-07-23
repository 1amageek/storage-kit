import Testing
@testable import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageEmbedded

@Suite("Cloudflare Durable Object Embedded Parity Tests")
struct CloudflareDurableObjectEmbeddedParityTests {
    @Test func embeddedScopeRejectsSameBlankValuesAsStorageScope() throws {
        #expect(throws: CloudflareDurableObjectScopeValidationError.self) {
            _ = try CloudflareDurableObjectStorageScope(databaseID: " \t\n")
        }
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectEmbeddedScope(databaseID: " \t\n")
        }
    }

    @Test func embeddedAndStorageScopesResolveSameName() throws {
        let storageScope = try CloudflareDurableObjectStorageScope(
            databaseID: "database",
            tenantID: "tenant",
            workspaceID: "workspace"
        )
        let embeddedScope = try CloudflareDurableObjectEmbeddedScope(
            databaseID: "database",
            tenantID: "tenant",
            workspaceID: "workspace"
        )

        let storageName = try storageScope.durableObjectName()
        let embeddedName = CloudflareDurableObjectEmbeddedNameCodec.name(for: embeddedScope)

        #expect(embeddedName == storageName)
    }
}
