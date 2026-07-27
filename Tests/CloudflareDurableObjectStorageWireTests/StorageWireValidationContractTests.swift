import DatabaseTypes
import CloudflareDurableObjectStorageWire
import Testing

@Suite("Storage Wire Validation Contract Tests")
struct StorageWireValidationContractTests {
    @Test func canonicalProtocolVersionIsOne() {
        #expect(StorageWire.protocolVersion == 1)
        #expect(
            StorageWireFailureStatus(rawValue: 0) == nil
        )
    }

    @Test func truncatedCollectionCountIsRejectedBeforeElementDecode() {
        let bytes = responsePrefix + [0x01, 0x00]

        do {
            _ = try StorageWire.decodeResponse(bytes)
            Issue.record("Expected a truncated collection count error")
        } catch {
            #expect(error == .wire(.truncated))
        }
    }

    @Test func oversizedCollectionCountIsRejectedBeforeElementDecode() {
        let maximumRows = StorageWireLimits.cloudflareDurableObject.maxRangeLimit
        var writer = StorageWireWriter()
        writer.writeUInt32(UInt32(maximumRows + 1))
        let bytes = responsePrefix + writer.bytes

        do {
            _ = try StorageWire.decodeResponse(bytes)
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
            count: StorageWireLimits.cloudflareDurableObject.maxFrameBytes + 1
        )

        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.decodeRequest(bytes)
        }
    }

    @Test func oversizedKeyAndValueAreRejectedBeforeEncoding() throws {
        let limits = StorageWireLimits.cloudflareDurableObject
        let scope = try StorageWireScope(databaseID: "main")
        let read = StorageWireRequest.read(
            StorageWireReadRequest(
                scope: scope,
                key: ByteString(
                    Array(repeating: 0, count: limits.maxKeyBytes + 1)
                ),
                snapshot: false
            )
        )
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.encode(read)
        }

        let response = StorageWireResponse.read(
            StorageWireReadResponse(
                value: ByteString(
                    Array(repeating: 0, count: limits.maxValueBytes + 1)
                ),
                currentCommitVersion: 0
            )
        )
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.encode(response)
        }
    }

    @Test func excessiveSelectorOffsetAndInvalidRangeLimitAreRejected() throws {
        let limits = StorageWireLimits.cloudflareDurableObject
        let scope = try StorageWireScope(databaseID: "main")
        let excessiveOffset = StorageWireRequest.range(
            StorageWireRangeRequest(
                scope: scope,
                begin: .selector(
                    StorageWireKeySelector(
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
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.encode(excessiveOffset)
        }

        let invalidLimit = StorageWireRequest.range(
            StorageWireRangeRequest(
                scope: scope,
                begin: .unbounded,
                end: .unbounded,
                limit: 0,
                reverse: false,
                snapshot: false
            )
        )
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.encode(invalidLimit)
        }
    }

    @Test func oversizedCursorKeyAndScopeAreRejected() {
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.encode(
                .range(
                    StorageWireRangeRequest(
                        scope: try StorageWireScope(
                            databaseID: "main"
                        ),
                        begin: .unbounded,
                        end: .unbounded,
                        limit: 1,
                        reverse: false,
                        snapshot: false,
                        cursorKey: ByteString(
                            [UInt8](
                                repeating: 0x01,
                                count: StorageWireLimits.cloudflareDurableObject.maxKeyBytes + 1
                            )
                        )
                    )
                )
            )
        }
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWireScope(
                databaseID: String(repeating: "a", count: 400)
            )
        }
    }

    @Test func oversizedFailureMessageIsRejectedBeforeEncoding() {
        let response = StorageWireResponse.failure(
            status: .backendFailure,
            message: String(
                repeating: "x",
                count: StorageWireLimits.cloudflareDurableObject.maxErrorMessageBytes + 1
            )
        )
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.encode(response)
        }
    }

    @Test func rangeMetricEncodersRejectInvalidValues() throws {
        let scope = try StorageWireScope(databaseID: "main")
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.encode(
                .rangeSize(
                    StorageWireRangeSizeRequest(
                        scope: scope,
                        begin: [0x02],
                        end: [0x01]
                    )
                )
            )
        }
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.encode(
                .rangeSplitPoints(
                    StorageWireRangeSplitPointsRequest(
                        scope: scope,
                        begin: [0x01],
                        end: [0x02],
                        chunkSize: 0
                    )
                )
            )
        }
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.encode(
                .rangeSize(
                    StorageWireRangeSizeResponse(
                        byteCount: -1,
                        currentCommitVersion: 0
                    )
                )
            )
        }
        let invalidPointSets: [[ByteString]] = [
            [],
            [[0x01], [0x01]],
            [[0x02], [0x01]],
        ]
        for points in invalidPointSets {
            #expect(throws: StorageWireProtocolError.self) {
                _ = try StorageWire.encode(
                    .rangeSplitPoints(
                        StorageWireRangeSplitPointsResponse(
                            splitPoints: points,
                            currentCommitVersion: 0
                        )
                    )
                )
            }
        }
    }

    @Test func rangeMetricDecodersRejectMalformedPayloads() throws {
        var negativeSize = StorageWireWriter()
        negativeSize.writeInt64(-1)
        negativeSize.writeInt64(0)
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.decodeResponse(
                successResponsePrefix(.rangeSize) + negativeSize.bytes
            )
        }

        var emptySplit = StorageWireWriter()
        emptySplit.writeUInt32(0)
        emptySplit.writeInt64(0)
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.decodeResponse(
                successResponsePrefix(.rangeSplitPoints) + emptySplit.bytes
            )
        }

        var duplicateSplit = StorageWireWriter()
        duplicateSplit.writeUInt32(2)
        try duplicateSplit.writeBytes([0x01])
        try duplicateSplit.writeBytes([0x01])
        duplicateSplit.writeInt64(0)
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.decodeResponse(
                successResponsePrefix(.rangeSplitPoints) + duplicateSplit.bytes
            )
        }

        var truncatedSplit = StorageWireWriter()
        truncatedSplit.writeUInt32(1)
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.decodeResponse(
                successResponsePrefix(.rangeSplitPoints) + truncatedSplit.bytes
            )
        }

        var oversizedSplit = StorageWireWriter()
        oversizedSplit.writeUInt32(
            UInt32(StorageWireLimits.cloudflareDurableObject.maxSplitPoints + 1)
        )
        do {
            _ = try StorageWire.decodeResponse(
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
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.decodeRequest(
                reversedSize.bytes
            )
        }

        var zeroChunk = try requestPrefix(.rangeSplitPoints)
        try zeroChunk.writeBytes([0x01])
        try zeroChunk.writeBytes([0x02])
        zeroChunk.writeInt64(0)
        zeroChunk.writeBool(false)
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.decodeRequest(
                zeroChunk.bytes
            )
        }
    }

    private var responsePrefix: [UInt8] {
        successResponsePrefix(.range)
    }

    private func successResponsePrefix(
        _ operation: StorageWireOperation
    ) -> [UInt8] {
        [
            StorageWire.protocolVersion,
            StorageWireStatusCode.ok.rawValue,
            operation.rawValue,
        ]
    }

    private func requestPrefix(
        _ operation: StorageWireOperation
    ) throws -> StorageWireWriter {
        var writer = StorageWireWriter()
        writer.writeUInt8(
            StorageWire.protocolVersion
        )
        writer.writeUInt8(operation.rawValue)
        try writer.writeString("main")
        writer.writeBool(false)
        writer.writeBool(false)
        return writer
    }
}
