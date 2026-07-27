import CloudflareDurableObjectStorageWire
import DatabaseTypes
import Testing

@Suite("Cloudflare Durable Object Storage Wire Tests")
struct StorageWireTests {
    @Test func writerAndReaderUseLittleEndianIntegers() throws {
        var writer = StorageWireWriter()
        writer.writeUInt8(0xAB)
        writer.writeUInt32(0x0102_0304)
        writer.writeInt64(0x0102_0304_0506_0708)

        var reader = StorageWireReader(writer.bytes)
        #expect(try reader.readUInt8() == 0xAB)
        #expect(try reader.readUInt32() == 0x0102_0304)
        #expect(try reader.readInt64() == 0x0102_0304_0506_0708)
        #expect(reader.remainingCount == 0)
    }

    @Test func primitiveReadsBorrowOwnerStorageOncePerValue() throws {
        let uint32Owner = WireBorrowCountingOwner(
            [0x04, 0x03, 0x02, 0x01]
        )
        var uint32Reader = StorageWireReader(
            ByteString(retaining: uint32Owner)
        )
        #expect(try uint32Reader.readUInt32() == 0x0102_0304)
        #expect(uint32Owner.borrowCount == 1)

        let uint64Owner = WireBorrowCountingOwner(
            [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
        )
        var uint64Reader = StorageWireReader(
            ByteString(retaining: uint64Owner)
        )
        #expect(try uint64Reader.readUInt64() == 0x0102_0304_0506_0708)
        #expect(uint64Owner.borrowCount == 1)
    }

    @Test func canonicalOrderingBorrowsEachOwnerOnce() {
        var left = [UInt8](repeating: 0x41, count: 16_384)
        var right = left
        left[left.count - 1] = 0x40
        right[right.count - 1] = 0x42
        let leftOwner = WireBorrowCountingOwner(left)
        let rightOwner = WireBorrowCountingOwner(right)

        let result = StorageWireByteOrdering.compare(
            ByteString(retaining: leftOwner),
            ByteString(retaining: rightOwner)
        )

        #expect(result == -1)
        #expect(leftOwner.borrowCount == 1)
        #expect(rightOwner.borrowCount == 1)
    }

    @Test func bytesRoundTripWithLengthPrefix() throws {
        let bytes = ByteString([0x00, 0xFF, 0x7F])
        var writer = StorageWireWriter()
        try writer.writeBytes(bytes)

        var reader = StorageWireReader(writer.bytes)
        let decoded = try reader.readByteRegion()

        #expect(decoded == bytes)
        #expect(reader.remainingCount == 0)
    }

    @Test func invalidUTF8StringIsRejected() throws {
        var writer = StorageWireWriter()
        try writer.writeBytes([0xFF])
        var reader = StorageWireReader(writer.bytes)

        #expect(throws: StorageWireError.self) {
            _ = try reader.readString()
        }
    }

    @Test func invalidBoolByteIsRejected() throws {
        var reader = StorageWireReader([0x02])

        #expect(throws: StorageWireError.self) {
            _ = try reader.readBool()
        }
    }

    @Test func trailingBytesAreRejectedByEnvelopeDecoder() throws {
        let scope = try StorageWireScope(databaseID: "main")
        let request = StorageWireRequest.readiness(
            StorageWireReadinessRequest(scope: scope)
        )
        var encoded =
            try StorageWire
            .encode(request)
            .copyBytes()
        encoded.append(0xFF)

        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.decodeRequest(encoded)
        }
    }

    @Test func removedCloudflareCompactionOperationIsRejected() throws {
        let version = StorageWire.protocolVersion

        do {
            _ = try StorageWire.decodeRequest(
                [version, 0x07]
            )
            Issue.record("Expected request operation 7 to be rejected")
        } catch {
            #expect(error == .unknownOperation(0x07))
        }

        do {
            _ = try StorageWire.decodeResponse(
                [version, 0x00, 0x07]
            )
            Issue.record("Expected response operation 7 to be rejected")
        } catch {
            #expect(error == .unknownOperation(0x07))
        }
    }

    @Test func writerRejectsCountsThatDoNotFitWireFormat() throws {
        var writer = StorageWireWriter()

        #expect(throws: StorageWireError.self) {
            try writer.writeCount(Int(UInt32.max) + 1)
        }
    }

    @Test func mutationTagRoundTripsWithoutCodable() throws {
        var writer = StorageWireWriter()
        StorageWireMutationType.compareAndClear.encode(into: &writer)
        var reader = StorageWireReader(writer.bytes)

        #expect(try StorageWireMutationType(from: &reader) == .compareAndClear)
        #expect(reader.remainingCount == 0)
    }

    @Test func cloudflareReadRequestEnvelopeRoundTrips() throws {
        let scope = try StorageWireScope(
            databaseID: "database",
            tenantID: "tenant",
            workspaceID: "workspace"
        )
        let request = StorageWireRequest.read(
            StorageWireReadRequest(
                scope: scope,
                key: [0x01, 0x02],
                snapshot: false,
                expectedReadVersion: 42
            )
        )

        let encoded = try StorageWire.encode(request)
        let decoded = try StorageWire.decodeRequest(encoded)

        #expect(decoded == request)
        #expect(scope.durableObjectName.hasPrefix("storage-kit/cfdo/v1/"))
    }

    @Test func scopePreservesIdentifiersAndProducesCanonicalName() throws {
        let scope = try StorageWireScope(
            databaseID: "MainDB",
            tenantID: "TenantA",
            workspaceID: "WorkspaceB"
        )

        #expect(scope.databaseID == "MainDB")
        #expect(scope.tenantID == "TenantA")
        #expect(scope.workspaceID == "WorkspaceB")
        #expect(
            scope.durableObjectName
                == "storage-kit/cfdo/v1/database/TWFpbkRC/tenant/VGVuYW50QQ/workspace/V29ya3NwYWNlQg"
        )
    }

    @Test func absentScopeComponentsCannotCollideWithLiteralMarker() throws {
        let absent = try StorageWireScope(databaseID: "main")
        let literal = try StorageWireScope(
            databaseID: "main",
            tenantID: "_",
            workspaceID: "_"
        )

        #expect(
            absent.durableObjectName
                == "storage-kit/cfdo/v1/database/bWFpbg/tenant/_/workspace/_"
        )
        #expect(absent.durableObjectName != literal.durableObjectName)
    }

    @Test func scopeRejectsBlankAndControlCharacterComponents() {
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWireScope(databaseID: " \t\n")
        }
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWireScope(databaseID: "main", tenantID: "")
        }
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWireScope(databaseID: "main", workspaceID: " ")
        }
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWireScope(databaseID: "main\u{0000}")
        }
    }

    @Test func cloudflareRangeResponseEnvelopeRoundTrips() throws {
        let response = StorageWireResponse.range(
            StorageWireRangeResponse(
                rows: [
                    StorageWireKeyValue(key: [0x01], value: [0x0A]),
                    StorageWireKeyValue(key: [0x02], value: [0x0B]),
                ],
                hasMore: true,
                currentCommitVersion: 7,
                readConflictRanges: [
                    StorageWireKeyRange(begin: [0x01], end: [0x03])
                ]
            )
        )

        let encoded = try StorageWire.encode(response)
        let decoded = try StorageWire.decodeResponse(encoded)

        #expect(decoded == response)
    }

    @Test func cloudflareCommitRequestRoundTripsMutations() throws {
        let scope = try StorageWireScope(databaseID: "main")
        let request = StorageWireRequest.commit(
            StorageWireCommitRequest(
                scope: scope,
                observedReadVersion: 3,
                mutations: [
                    .set(key: [0x01], value: [0x0A]),
                    .atomic(key: [0x01], param: [0x01], mutationType: .add),
                    .clearRange(begin: [0x10], end: [0x20]),
                ],
                readConflictRanges: [
                    StorageWireKeyRange.singleKey([0x01]),
                    StorageWireKeyRange(begin: [0x10], end: [0x20]),
                ]
            )
        )

        let decoded = try StorageWire.decodeRequest(
            StorageWire.encode(request)
        )

        #expect(decoded == request)
    }
}
