import Testing
@testable import CloudflareDurableObjectStorage

@Suite("Cloudflare Durable Object v1 Name Codec Tests")
struct CloudflareDurableObjectV1NameCodecTests {
    @Test func nameIsDeterministicAndVersioned() throws {
        let scope = try CloudflareDurableObjectStorageScope(
            databaseID: "main",
            tenantID: "tenant",
            workspaceID: "workspace"
        )
        let codec = CloudflareDurableObjectV1NameCodec()

        let first = try codec.routedName(for: scope)
        let second = try codec.routedName(for: scope)

        #expect(first == second)
        #expect(first.codecVersion == "v1")
        #expect(first.name == "storage-kit/cfdo/v1/database/bWFpbg/tenant/dGVuYW50/workspace/d29ya3NwYWNl")
    }

    @Test func optionalComponentsUseReservedEmptyMarker() throws {
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")
        let name = try CloudflareDurableObjectV1NameCodec().name(for: scope)

        #expect(name == "storage-kit/cfdo/v1/database/bWFpbg/tenant/_/workspace/_")
    }

    @Test func emptyMarkerDoesNotCollideWithLiteralUnderscore() throws {
        let absent = try CloudflareDurableObjectStorageScope(databaseID: "main")
        let literal = try CloudflareDurableObjectStorageScope(
            databaseID: "main",
            tenantID: "_",
            workspaceID: "_"
        )
        let codec = CloudflareDurableObjectV1NameCodec()

        #expect(try codec.name(for: absent) != codec.name(for: literal))
    }

    @Test func nameLimitIsEnforced() throws {
        let scope = try CloudflareDurableObjectStorageScope(databaseID: "main")
        let codec = CloudflareDurableObjectV1NameCodec(maxNameBytes: 8)

        #expect(throws: CloudflareDurableObjectNameCodecError.self) {
            _ = try codec.name(for: scope)
        }
    }
}
