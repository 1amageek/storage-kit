import Testing
@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object storage scope naming")
struct CloudflareDurableObjectStorageScopeNameTests {
    @Test func nameUsesCanonicalNamespace() throws {
        let scope = try CloudflareDurableObjectStorageScope(
            databaseID: "main",
            tenantID: "tenant",
            workspaceID: "workspace"
        )

        let first = try scope.durableObjectName()
        let second = try scope.durableObjectName()

        #expect(first == second)
        #expect(first == "storage-kit/cfdo/v1/database/bWFpbg/tenant/dGVuYW50/workspace/d29ya3NwYWNl")
    }

    @Test func optionalComponentsUseReservedEmptyMarker() throws {
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")
        let name = try scope.durableObjectName()

        #expect(name == "storage-kit/cfdo/v1/database/bWFpbg/tenant/_/workspace/_")
    }

    @Test func emptyMarkerDoesNotCollideWithLiteralUnderscore() throws {
        let absent = try CloudflareDurableObjectStorageScope(databaseID: "main")
        let literal = try CloudflareDurableObjectStorageScope(
            databaseID: "main",
            tenantID: "_",
            workspaceID: "_"
        )

        #expect(
            try absent.durableObjectName()
                != literal.durableObjectName()
        )
    }

    @Test func nameLimitIsEnforced() throws {
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")

        #expect(throws: CloudflareDurableObjectNameError.self) {
            _ = try scope.durableObjectName(maximumBytes: 8)
        }
    }
}
