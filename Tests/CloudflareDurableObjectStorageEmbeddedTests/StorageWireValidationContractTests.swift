import CloudflareDurableObjectStorageEmbedded
import StorageKitEmbeddedCore
import Testing

@Suite("Storage Wire Validation Contract Tests")
struct StorageWireValidationContractTests {
    @Test func canonicalProtocolVersionIsOne() {
        #expect(CloudflareDurableObjectStorageWireCodec.protocolVersion == 1)
        #expect(
            CloudflareDurableObjectEmbeddedFailureStatus(rawValue: 0) == nil
        )
    }

    @Test func truncatedCollectionCountIsRejectedBeforeElementDecode() {
        let bytes = responsePrefix + [0x01, 0x00]

        do {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeResponse(bytes)
            Issue.record("Expected a truncated collection count error")
        } catch {
            #expect(error == .wire(.truncated))
        }
    }

    @Test func oversizedCollectionCountIsRejectedBeforeElementDecode() {
        let maximumRows = EmbeddedLimits.cloudflareDurableObject.maxRangeLimit
        var writer = EmbeddedWireWriter()
        writer.writeUInt32(UInt32(maximumRows + 1))
        let bytes = responsePrefix + writer.bytes

        do {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeResponse(bytes)
            Issue.record("Expected an oversized collection count error")
        } catch {
            // A truncated element error proves the decoder accepted the oversized count
            // and entered element decoding instead of enforcing its allocation bound.
            #expect(
                error != .wire(.truncated),
                "Oversized collection counts must be rejected before element decoding"
            )
        }
    }

    @Test func oversizedFrameIsRejectedBeforeEnvelopeDecode() {
        let bytes = Array(
            repeating: UInt8(0),
            count: EmbeddedLimits.cloudflareDurableObject.maxFrameBytes + 1
        )

        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeRequest(bytes)
        }
    }

    @Test func oversizedKeyAndValueAreRejectedBeforeEncoding() throws {
        let limits = EmbeddedLimits.cloudflareDurableObject
        let scope = try CloudflareDurableObjectEmbeddedScope(databaseID: "main")
        let read = CloudflareDurableObjectEmbeddedRequest.read(
            CloudflareDurableObjectEmbeddedReadRequest(
                scope: scope,
                key: EmbeddedBytes(
                    Array(repeating: 0, count: limits.maxKeyBytes + 1)
                ),
                snapshot: false
            )
        )
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.encode(read)
        }

        let response = CloudflareDurableObjectEmbeddedResponse.read(
            CloudflareDurableObjectEmbeddedReadResponse(
                value: EmbeddedBytes(
                    Array(repeating: 0, count: limits.maxValueBytes + 1)
                ),
                currentCommitVersion: 0
            )
        )
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.encode(response)
        }
    }

    @Test func excessiveSelectorOffsetAndInvalidRangeLimitAreRejected() throws {
        let limits = EmbeddedLimits.cloudflareDurableObject
        let scope = try CloudflareDurableObjectEmbeddedScope(databaseID: "main")
        let excessiveOffset = CloudflareDurableObjectEmbeddedRequest.range(
            CloudflareDurableObjectEmbeddedRangeRequest(
                scope: scope,
                begin: .selector(
                    EmbeddedKeySelector(
                        key: [0x01],
                        orEqual: false,
                        offset: limits.maxSelectorResolutionSteps + 1
                    )
                ),
                end: .unbounded,
                limit: 1,
                reverse: false,
                snapshot: false
            )
        )
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.encode(excessiveOffset)
        }

        let invalidLimit = CloudflareDurableObjectEmbeddedRequest.range(
            CloudflareDurableObjectEmbeddedRangeRequest(
                scope: scope,
                begin: .unbounded,
                end: .unbounded,
                limit: 0,
                reverse: false,
                snapshot: false
            )
        )
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.encode(invalidLimit)
        }
    }

    @Test func oversizedCursorKeyAndScopeAreRejected() {
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.encode(
                .range(
                    CloudflareDurableObjectEmbeddedRangeRequest(
                        scope: try CloudflareDurableObjectEmbeddedScope(
                            databaseID: "main"
                        ),
                        begin: .unbounded,
                        end: .unbounded,
                        limit: 1,
                        reverse: false,
                        snapshot: false,
                        cursorKey: EmbeddedBytes(
                            [UInt8](
                                repeating: 0x01,
                                count: EmbeddedLimits.cloudflareDurableObject.maxKeyBytes + 1
                            )
                        )
                    )
                )
            )
        }
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectEmbeddedScope(
                databaseID: String(repeating: "a", count: 400)
            )
        }
    }

    @Test func oversizedFailureMessageIsRejectedBeforeEncoding() {
        let response = CloudflareDurableObjectEmbeddedResponse.failure(
            status: .backendFailure,
            message: String(
                repeating: "x",
                count: EmbeddedLimits.cloudflareDurableObject.maxErrorMessageBytes + 1
            )
        )
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.encode(response)
        }
    }

    @Test func rangeMetricEncodersRejectInvalidValues() throws {
        let scope = try CloudflareDurableObjectEmbeddedScope(databaseID: "main")
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.encode(
                .rangeSize(
                    CloudflareDurableObjectEmbeddedRangeSizeRequest(
                        scope: scope,
                        begin: [0x02],
                        end: [0x01]
                    )
                )
            )
        }
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.encode(
                .rangeSplitPoints(
                    CloudflareDurableObjectEmbeddedRangeSplitPointsRequest(
                        scope: scope,
                        begin: [0x01],
                        end: [0x02],
                        chunkSize: 0
                    )
                )
            )
        }
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.encode(
                .rangeSize(
                    CloudflareDurableObjectEmbeddedRangeSizeResponse(
                        byteCount: -1,
                        currentCommitVersion: 0
                    )
                )
            )
        }
        let invalidPointSets: [[EmbeddedBytes]] = [
            [],
            [[0x01], [0x01]],
            [[0x02], [0x01]],
        ]
        for points in invalidPointSets {
            #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
                _ = try CloudflareDurableObjectStorageWireCodec.encode(
                    .rangeSplitPoints(
                        CloudflareDurableObjectEmbeddedRangeSplitPointsResponse(
                            splitPoints: points,
                            currentCommitVersion: 0
                        )
                    )
                )
            }
        }
    }

    @Test func rangeMetricDecodersRejectMalformedPayloads() throws {
        var negativeSize = EmbeddedWireWriter()
        negativeSize.writeInt64(-1)
        negativeSize.writeInt64(0)
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeResponse(
                successResponsePrefix(.rangeSize) + negativeSize.bytes
            )
        }

        var emptySplit = EmbeddedWireWriter()
        emptySplit.writeUInt32(0)
        emptySplit.writeInt64(0)
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeResponse(
                successResponsePrefix(.rangeSplitPoints) + emptySplit.bytes
            )
        }

        var duplicateSplit = EmbeddedWireWriter()
        duplicateSplit.writeUInt32(2)
        try duplicateSplit.writeBytes([0x01])
        try duplicateSplit.writeBytes([0x01])
        duplicateSplit.writeInt64(0)
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeResponse(
                successResponsePrefix(.rangeSplitPoints) + duplicateSplit.bytes
            )
        }

        var truncatedSplit = EmbeddedWireWriter()
        truncatedSplit.writeUInt32(1)
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeResponse(
                successResponsePrefix(.rangeSplitPoints) + truncatedSplit.bytes
            )
        }

        var oversizedSplit = EmbeddedWireWriter()
        oversizedSplit.writeUInt32(
            UInt32(EmbeddedLimits.cloudflareDurableObject.maxSplitPoints + 1)
        )
        do {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeResponse(
                successResponsePrefix(.rangeSplitPoints) + oversizedSplit.bytes
            )
            Issue.record("Expected an oversized split-point count error")
        } catch {
            #expect(error != .wire(.truncated))
        }
    }

    @Test func rangeMetricRequestDecoderRejectsBoundsAndChunkSize() throws {
        var reversedSize = try requestPrefix(.rangeSize)
        try reversedSize.writeBytes([0x02])
        try reversedSize.writeBytes([0x01])
        reversedSize.writeBool(false)
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeRequest(
                reversedSize.bytes
            )
        }

        var zeroChunk = try requestPrefix(.rangeSplitPoints)
        try zeroChunk.writeBytes([0x01])
        try zeroChunk.writeBytes([0x02])
        zeroChunk.writeInt64(0)
        zeroChunk.writeBool(false)
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeRequest(
                zeroChunk.bytes
            )
        }
    }

    private var responsePrefix: [UInt8] {
        successResponsePrefix(.range)
    }

    private func successResponsePrefix(
        _ operation: CloudflareDurableObjectEmbeddedOperation
    ) -> [UInt8] {
        [
            CloudflareDurableObjectStorageWireCodec.protocolVersion,
            CloudflareDurableObjectEmbeddedStatusCode.ok.rawValue,
            operation.rawValue,
        ]
    }

    private func requestPrefix(
        _ operation: CloudflareDurableObjectEmbeddedOperation
    ) throws -> EmbeddedWireWriter {
        var writer = EmbeddedWireWriter()
        writer.writeUInt8(
            CloudflareDurableObjectStorageWireCodec.protocolVersion
        )
        writer.writeUInt8(operation.rawValue)
        try writer.writeString("main")
        writer.writeBool(false)
        writer.writeBool(false)
        return writer
    }
}
