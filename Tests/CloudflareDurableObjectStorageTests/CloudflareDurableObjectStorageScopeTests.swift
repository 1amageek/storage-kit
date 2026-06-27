import Testing
@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object Scope Tests")
struct CloudflareDurableObjectStorageScopeTests {
    @Test func scopePreservesIdentifiersExactly() throws {
        let scope = try CloudflareDurableObjectStorageScope(
            databaseID: "MainDB",
            tenantID: "TenantA",
            workspaceID: "WorkspaceB"
        )

        #expect(scope.databaseID == "MainDB")
        #expect(scope.tenantID == "TenantA")
        #expect(scope.workspaceID == "WorkspaceB")
        #expect(scope.canonicalDescription == "databaseID=MainDB;tenantID=TenantA;workspaceID=WorkspaceB")
    }

    @Test func databaseIDRejectsBlankValue() {
        #expect(throws: CloudflareDurableObjectScopeValidationError.self) {
            _ = try CloudflareDurableObjectStorageScope(databaseID: " \t\n")
        }
    }

    @Test func optionalComponentsRejectBlankValueWhenPresent() {
        #expect(throws: CloudflareDurableObjectScopeValidationError.self) {
            _ = try CloudflareDurableObjectStorageScope(databaseID: "main", tenantID: "")
        }
        #expect(throws: CloudflareDurableObjectScopeValidationError.self) {
            _ = try CloudflareDurableObjectStorageScope(databaseID: "main", workspaceID: " ")
        }
    }

    @Test func scopeRejectsControlCharacters() {
        #expect(throws: CloudflareDurableObjectScopeValidationError.self) {
            _ = try CloudflareDurableObjectStorageScope(databaseID: "main\u{0000}")
        }
    }
}
