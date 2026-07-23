import Testing
import StorageKitEmbeddedCore
import CloudflareDurableObjectStorageEmbedded

@Suite("Cloudflare Durable Object Embedded Wire Codec Tests")
struct EmbeddedWireCodecTests {
    @Test func writerAndReaderUseLittleEndianIntegers() throws {
        var writer = EmbeddedWireWriter()
        writer.writeUInt8(0xAB)
        writer.writeUInt32(0x01020304)
        writer.writeInt64(0x0102030405060708)

        var reader = EmbeddedWireReader(writer.bytes)
        #expect(try reader.readUInt8() == 0xAB)
        #expect(try reader.readUInt32() == 0x01020304)
        #expect(try reader.readInt64() == 0x0102030405060708)
        #expect(reader.remainingCount == 0)
    }

    @Test func primitiveReadsBorrowOwnerStorageOncePerValue() throws {
        let uint32Owner = EmbeddedBorrowCountingOwner(
            [0x04, 0x03, 0x02, 0x01]
        )
        var uint32Reader = EmbeddedWireReader(
            EmbeddedBytes(retaining: uint32Owner)
        )
        #expect(try uint32Reader.readUInt32() == 0x01020304)
        #expect(uint32Owner.borrowCount == 1)

        let uint64Owner = EmbeddedBorrowCountingOwner(
            [0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]
        )
        var uint64Reader = EmbeddedWireReader(
            EmbeddedBytes(retaining: uint64Owner)
        )
        #expect(try uint64Reader.readUInt64() == 0x0102030405060708)
        #expect(uint64Owner.borrowCount == 1)
    }

    @Test func canonicalOrderingBorrowsEachOwnerOnce() {
        var left = [UInt8](repeating: 0x41, count: 16_384)
        var right = left
        left[left.count - 1] = 0x40
        right[right.count - 1] = 0x42
        let leftOwner = EmbeddedBorrowCountingOwner(left)
        let rightOwner = EmbeddedBorrowCountingOwner(right)

        let result = EmbeddedByteOrdering.compare(
            EmbeddedBytes(retaining: leftOwner),
            EmbeddedBytes(retaining: rightOwner)
        )

        #expect(result == -1)
        #expect(leftOwner.borrowCount == 1)
        #expect(rightOwner.borrowCount == 1)
    }

    @Test func embeddedBytesRoundTripWithLengthPrefix() throws {
        let bytes = EmbeddedBytes([0x00, 0xFF, 0x7F])
        var writer = EmbeddedWireWriter()
        try bytes.encode(into: &writer)

        var reader = EmbeddedWireReader(writer.bytes)
        let decoded = try EmbeddedBytes(from: &reader)

        #expect(decoded == bytes)
        #expect(reader.remainingCount == 0)
    }

    @Test func invalidUTF8StringIsRejected() throws {
        var writer = EmbeddedWireWriter()
        try writer.writeBytes([0xFF])
        var reader = EmbeddedWireReader(writer.bytes)

        #expect(throws: EmbeddedWireError.self) {
            _ = try reader.readString()
        }
    }

    @Test func invalidBoolByteIsRejected() throws {
        var reader = EmbeddedWireReader([0x02])

        #expect(throws: EmbeddedWireError.self) {
            _ = try reader.readBool()
        }
    }

    @Test func trailingBytesAreRejectedByEnvelopeDecoder() throws {
        let scope = try CloudflareDurableObjectEmbeddedScope(databaseID: "main")
        let request = CloudflareDurableObjectEmbeddedRequest.readiness(
            CloudflareDurableObjectEmbeddedReadinessRequest(scope: scope)
        )
        var encoded = try CloudflareDurableObjectStorageWireCodec
            .encode(request)
            .contiguousArray()
        encoded.append(0xFF)

        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeRequest(encoded)
        }
    }

    @Test func removedCloudflareCompactionOperationIsRejected() throws {
        let version = CloudflareDurableObjectStorageWireCodec.protocolVersion

        do {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeRequest(
                [version, 0x07]
            )
            Issue.record("Expected request operation 7 to be rejected")
        } catch {
            #expect(error == .unknownOperation(0x07))
        }

        do {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeResponse(
                [version, 0x00, 0x07]
            )
            Issue.record("Expected response operation 7 to be rejected")
        } catch {
            #expect(error == .unknownOperation(0x07))
        }
    }

    @Test func writerRejectsCountsThatDoNotFitWireFormat() throws {
        var writer = EmbeddedWireWriter()

        #expect(throws: EmbeddedWireError.self) {
            try writer.writeCount(Int(UInt32.max) + 1)
        }
    }

    @Test func embeddedMutationTypeRoundTripsWithoutCodable() throws {
        #expect(try CloudflareDurableObjectStorageWireCodec.validateMutationRoundTrip(.add) == .add)
        #expect(try CloudflareDurableObjectStorageWireCodec.validateMutationRoundTrip(.compareAndClear) == .compareAndClear)
    }

    @Test func embeddedMutationSemanticsMatchSharedAtomicBehavior() throws {
        #expect(try EmbeddedMutationType.add.apply(to: [0xFF], param: [0x01]) == .set([0x00]))
        #expect(try EmbeddedMutationType.max.apply(to: [0x00, 0x02], param: [0xFF, 0x01]) == .set([0x00, 0x02]))
    }

    @Test func embeddedMutationBorrowsEachLargeOperandOnce() throws {
        let existingOwner = EmbeddedBorrowCountingOwner(
            [UInt8](repeating: 0x55, count: 16_384)
        )
        let parameterOwner = EmbeddedBorrowCountingOwner(
            [UInt8](repeating: 0xaa, count: 16_384)
        )

        let result = try EmbeddedMutationType.bitOr.apply(
            to: EmbeddedBytes(retaining: existingOwner),
            param: EmbeddedBytes(retaining: parameterOwner)
        )

        #expect(existingOwner.borrowCount == 1)
        #expect(parameterOwner.borrowCount == 1)
        guard case .set(let bytes) = result else {
            Issue.record("Expected a set mutation result")
            return
        }
        #expect(bytes.count == 16_384)
        #expect(bytes.first == 0xff)
        #expect(bytes.last == 0xff)
    }

    @Test func rangeOverlayAppliesWritesBeforeReverseLimit() throws {
        let committed = [
            EmbeddedKeyValue(key: [0x01], value: [1]),
            EmbeddedKeyValue(key: [0x02], value: [2]),
            EmbeddedKeyValue(key: [0x03], value: [3])
        ]
        let writes: [EmbeddedWriteOperation] = [
            .atomic(key: [0x01], param: [4], mutationType: .add),
            .clear(key: [0x02]),
            .set(key: [0x04], value: [4])
        ]

        let rows = try CloudflareDurableObjectStorageWireCodec.apply(
            committedRows: committed,
            writes: writes,
            begin: EmbeddedKeySelector(key: [0x01], kind: .firstGreaterOrEqual),
            end: EmbeddedKeySelector(key: [0x05], kind: .firstGreaterOrEqual),
            reverse: true,
            limit: 2
        )

        #expect(rows.map(\.key) == [[0x04], [0x03]])
        #expect(rows.map(\.value) == [[4], [3]])
    }

    @Test func rangeOverlayPreservesAllKeySelectorKinds() throws {
        let committed = [
            EmbeddedKeyValue(key: [0x01], value: [1]),
            EmbeddedKeyValue(key: [0x03], value: [3]),
            EmbeddedKeyValue(key: [0x05], value: [5]),
            EmbeddedKeyValue(key: [0x07], value: [7])
        ]
        let cases: [
            (EmbeddedKeySelector, EmbeddedKeySelector, [EmbeddedBytes])
        ] = [
            (
                EmbeddedKeySelector(key: [0x03], kind: .firstGreaterOrEqual),
                EmbeddedKeySelector(key: [0x07], kind: .firstGreaterOrEqual),
                [[0x03], [0x05]]
            ),
            (
                EmbeddedKeySelector(key: [0x03], kind: .firstGreaterThan),
                EmbeddedKeySelector(key: [0x05], kind: .firstGreaterThan),
                [[0x05]]
            ),
            (
                EmbeddedKeySelector(key: [0x05], kind: .lastLessOrEqual),
                EmbeddedKeySelector(key: [0x07], kind: .firstGreaterThan),
                [[0x05], [0x07]]
            ),
            (
                EmbeddedKeySelector(key: [0x05], kind: .lastLessThan),
                EmbeddedKeySelector(key: [0x07], kind: .lastLessOrEqual),
                [[0x03], [0x05]]
            )
        ]

        for (begin, end, expectedKeys) in cases {
            let rows = try CloudflareDurableObjectStorageWireCodec.apply(
                committedRows: committed,
                writes: [],
                begin: begin,
                end: end,
                reverse: false,
                limit: 0
            )
            #expect(rows.map(\.key) == expectedKeys)
        }
    }

    @Test func cloudflareReadRequestEnvelopeRoundTrips() throws {
        let scope = try CloudflareDurableObjectEmbeddedScope(
            databaseID: "database",
            tenantID: "tenant",
            workspaceID: "workspace"
        )
        let request = CloudflareDurableObjectEmbeddedRequest.read(
            CloudflareDurableObjectEmbeddedReadRequest(
                scope: scope,
                key: [0x01, 0x02],
                snapshot: false,
                expectedReadVersion: 42
            )
        )

        let encoded = try CloudflareDurableObjectStorageWireCodec.encode(request)
        let decoded = try CloudflareDurableObjectStorageWireCodec.decodeRequest(encoded)

        #expect(decoded == request)
        #expect(CloudflareDurableObjectEmbeddedNameCodec.name(for: scope).hasPrefix("storage-kit/cfdo/v1/"))
    }

    @Test func cloudflareRangeResponseEnvelopeRoundTrips() throws {
        let response = CloudflareDurableObjectEmbeddedResponse.range(
            CloudflareDurableObjectEmbeddedRangeResponse(
                rows: [
                    EmbeddedKeyValue(key: [0x01], value: [0x0A]),
                    EmbeddedKeyValue(key: [0x02], value: [0x0B])
                ],
                hasMore: true,
                currentCommitVersion: 7,
                readConflictRanges: [
                    EmbeddedKeyRange(begin: [0x01], end: [0x03]),
                ]
            )
        )

        let encoded = try CloudflareDurableObjectStorageWireCodec.encode(response)
        let decoded = try CloudflareDurableObjectStorageWireCodec.decodeResponse(encoded)

        #expect(decoded == response)
    }

    @Test func cloudflareCommitRequestRoundTripsMutations() throws {
        let scope = try CloudflareDurableObjectEmbeddedScope(databaseID: "main")
        let request = CloudflareDurableObjectEmbeddedRequest.commit(
            CloudflareDurableObjectEmbeddedCommitRequest(
                scope: scope,
                observedReadVersion: 3,
                mutations: [
                    .set(key: [0x01], value: [0x0A]),
                    .atomic(key: [0x01], param: [0x01], mutationType: .add),
                    .clearRange(begin: [0x10], end: [0x20])
                ],
                readConflictRanges: [
                    EmbeddedKeyRange.singleKey([0x01]),
                    EmbeddedKeyRange(begin: [0x10], end: [0x20])
                ]
            )
        )

        let decoded = try CloudflareDurableObjectStorageWireCodec.decodeRequest(
            CloudflareDurableObjectStorageWireCodec.encode(request)
        )

        #expect(decoded == request)
    }
}
