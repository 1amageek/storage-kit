import Testing
import StorageKitEmbeddedCore
import CloudflareDurableObjectStorageEmbedded

@Suite("Cloudflare Durable Object Embedded Binary Codec Tests")
struct EmbeddedBinaryCodecTests {
    @Test func writerAndReaderUseLittleEndianIntegers() throws {
        var writer = EmbeddedBinaryWriter()
        writer.writeUInt8(0xAB)
        writer.writeUInt32(0x01020304)
        writer.writeInt64(0x0102030405060708)

        var reader = EmbeddedBinaryReader(writer.bytes)
        #expect(try reader.readUInt8() == 0xAB)
        #expect(try reader.readUInt32() == 0x01020304)
        #expect(try reader.readInt64() == 0x0102030405060708)
        #expect(reader.remainingCount == 0)
    }

    @Test func embeddedBytesRoundTripWithLengthPrefix() throws {
        let bytes = EmbeddedBytes([0x00, 0xFF, 0x7F])
        var writer = EmbeddedBinaryWriter()
        try bytes.encode(into: &writer)

        var reader = EmbeddedBinaryReader(writer.bytes)
        let decoded = try EmbeddedBytes(from: &reader)

        #expect(decoded == bytes)
        #expect(reader.remainingCount == 0)
    }

    @Test func invalidUTF8StringIsRejected() throws {
        var writer = EmbeddedBinaryWriter()
        try writer.writeBytes([0xFF])
        var reader = EmbeddedBinaryReader(writer.bytes)

        #expect(throws: EmbeddedWireError.self) {
            _ = try reader.readString()
        }
    }

    @Test func invalidBoolByteIsRejected() throws {
        var reader = EmbeddedBinaryReader([0x02])

        #expect(throws: EmbeddedWireError.self) {
            _ = try reader.readBool()
        }
    }

    @Test func trailingBytesAreRejectedByEnvelopeDecoder() throws {
        let scope = try CloudflareDurableObjectEmbeddedScope(databaseID: "main")
        let request = CloudflareDurableObjectEmbeddedRequest.readiness(
            CloudflareDurableObjectEmbeddedReadinessRequest(scope: scope)
        )
        var encoded = try CloudflareDurableObjectEmbeddedRuntime.encode(request)
        encoded.append(0xFF)

        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectEmbeddedRuntime.decodeRequest(encoded)
        }
    }

    @Test func unknownOperationIsRejectedByEnvelopeDecoder() throws {
        let encoded = [CloudflareDurableObjectEmbeddedRuntime.protocolVersion, 0xFF]

        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectEmbeddedRuntime.decodeRequest(encoded)
        }
    }

    @Test func writerRejectsCountsThatDoNotFitWireFormat() throws {
        var writer = EmbeddedBinaryWriter()

        #expect(throws: EmbeddedWireError.self) {
            try writer.writeCount(Int(UInt32.max) + 1)
        }
    }

    @Test func embeddedMutationTypeRoundTripsWithoutCodable() throws {
        #expect(try CloudflareDurableObjectEmbeddedRuntime.validateMutationRoundTrip(.add) == .add)
        #expect(try CloudflareDurableObjectEmbeddedRuntime.validateMutationRoundTrip(.compareAndClear) == .compareAndClear)
    }

    @Test func embeddedMutationSemanticsMatchSharedAtomicBehavior() throws {
        #expect(try EmbeddedMutationType.add.apply(to: [0xFF], param: [0x01]) == .set([0x00]))
        #expect(try EmbeddedMutationType.max.apply(to: [0x00, 0x02], param: [0xFF, 0x01]) == .set([0x00, 0x02]))
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

        let rows = try CloudflareDurableObjectEmbeddedRuntime.apply(
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
        let cases: [(EmbeddedKeySelector, EmbeddedKeySelector, [[UInt8]])] = [
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
            let rows = try CloudflareDurableObjectEmbeddedRuntime.apply(
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

        let encoded = try CloudflareDurableObjectEmbeddedRuntime.encode(request)
        let decoded = try CloudflareDurableObjectEmbeddedRuntime.decodeRequest(encoded)

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
                nextCursor: "2",
                currentCommitVersion: 7,
                conflictRange: EmbeddedKeyRange(begin: [0x01], end: [0x03])
            )
        )

        let encoded = try CloudflareDurableObjectEmbeddedRuntime.encode(response)
        let decoded = try CloudflareDurableObjectEmbeddedRuntime.decodeResponse(encoded)

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

        let decoded = try CloudflareDurableObjectEmbeddedRuntime.decodeRequest(
            CloudflareDurableObjectEmbeddedRuntime.encode(request)
        )

        #expect(decoded == request)
    }
}
