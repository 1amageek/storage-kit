import Testing
@testable import CloudflareDurableObjectStorage
import CloudflareDurableObjectStorageWire

@Suite("Cloudflare Durable Object Storage Wire Parity Tests")
struct StorageWireParityTests {
    @Test func wireScopeRejectsSameBlankValuesAsStorageScope() throws {
        #expect(throws: CloudflareDurableObjectScopeValidationError.self) {
            _ = try CloudflareDurableObjectStorageScope(databaseID: " \t\n")
        }
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWireScope(databaseID: " \t\n")
        }
    }

    @Test func wireAndStorageScopesResolveSameName() throws {
        let storageScope = try CloudflareDurableObjectStorageScope(
            databaseID: "database",
            tenantID: "tenant",
            workspaceID: "workspace"
        )
        let wireScope = try StorageWireScope(
            databaseID: "database",
            tenantID: "tenant",
            workspaceID: "workspace"
        )

        let storageName = try storageScope.durableObjectName()
        let wireName = wireScope.durableObjectName

        #expect(wireName == storageName)
    }
}
