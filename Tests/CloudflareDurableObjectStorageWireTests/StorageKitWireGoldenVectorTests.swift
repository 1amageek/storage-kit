import DatabaseTypes
import Foundation
import Testing
import CloudflareDurableObjectStorageWire

@Suite("StorageKit Wire v1 Golden Vector Tests")
struct StorageKitWireGoldenVectorTests {
    @Test func encoderMatchesCanonicalVectors() throws {
        let vectors = try loadVectors()
        let scope = try StorageWireScope(databaseID: "main")

        let readiness = StorageWireRequest.readiness(
            StorageWireReadinessRequest(scope: scope)
        )
        #expect(try encodeHex(readiness) == vectors.readinessRequest)

        let range = StorageWireRequest.range(
            StorageWireRangeRequest(
                scope: scope,
                begin: .unbounded,
                end: .selector(
                    StorageWireKeySelector(key: [0x20], orEqual: true, offset: 1)
                ),
                limit: 2,
                reverse: false,
                snapshot: false,
                expectedReadVersion: 7,
                cursorKey: [0xFF]
            )
        )
        #expect(try encodeHex(range) == vectors.rangeRequest)

        let commit = StorageWireRequest.commit(
            StorageWireCommitRequest(
                scope: scope,
                observedReadVersion: 7,
                mutations: [
                    .set(key: [0x01], value: [0x0A, 0x0B]),
                    .clearRange(begin: [0x02], end: [0x04]),
                    .atomic(key: [0x05], param: [0x01], mutationType: .add),
                ],
                readConflictRanges: [
                    StorageWireKeyRange(begin: nil, end: [0x09]),
                ],
                writeConflictRanges: [
                    StorageWireKeyRange(begin: [0x05], end: [0x05, 0x00]),
                ]
            )
        )
        #expect(try encodeHex(commit) == vectors.commitRequest)

        let versionstampedCommit = StorageWireRequest
            .commit(
                StorageWireCommitRequest(
                    scope: scope,
                    observedReadVersion: 8,
                    mutations: [
                        .atomic(
                            key: [0x20],
                            param: [
                                0xAA,
                                0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                0xBB,
                                0x01, 0x00, 0x00, 0x00,
                            ],
                            mutationType: .setVersionstampedValue
                        ),
                    ]
                )
            )
        #expect(
            try encodeHex(versionstampedCommit)
                == vectors.versionstampedCommitRequest
        )

        let rangeResponse = StorageWireResponse.range(
            StorageWireRangeResponse(
                rows: [
                    StorageWireKeyValue(key: [0x01], value: [0x0A]),
                    StorageWireKeyValue(key: [0x02], value: [0x0B, 0x0C]),
                ],
                hasMore: true,
                currentCommitVersion: 8,
                readConflictRanges: [
                    StorageWireKeyRange(begin: [0x01], end: [0x02]),
                    StorageWireKeyRange(begin: [0x05], end: nil),
                ]
            )
        )
        #expect(try encodeHex(rangeResponse) == vectors.rangeResponse)

        let rangeSize = StorageWireRequest.rangeSize(
            StorageWireRangeSizeRequest(
                scope: scope,
                begin: [0x01],
                end: [0x04],
                expectedReadVersion: 7
            )
        )
        #expect(try encodeHex(rangeSize) == vectors.rangeSizeRequest)

        let rangeSizeResponse = StorageWireResponse
            .rangeSize(
                StorageWireRangeSizeResponse(
                    byteCount: 11,
                    currentCommitVersion: 8
                )
            )
        #expect(
            try encodeHex(rangeSizeResponse) == vectors.rangeSizeResponse
        )

        let splitPoints = StorageWireRequest
            .rangeSplitPoints(
                StorageWireRangeSplitPointsRequest(
                    scope: scope,
                    begin: [0x01],
                    end: [0x04],
                    chunkSize: 6,
                    expectedReadVersion: 7
                )
            )
        #expect(
            try encodeHex(splitPoints) == vectors.rangeSplitPointsRequest
        )

        let splitPointsResponse = StorageWireResponse
            .rangeSplitPoints(
                StorageWireRangeSplitPointsResponse(
                    splitPoints: [[0x01], [0x03], [0x04]],
                    currentCommitVersion: 8
                )
            )
        #expect(
            try encodeHex(splitPointsResponse)
                == vectors.rangeSplitPointsResponse
        )

        let failure = StorageWireResponse.failure(
            status: .transactionConflict,
            message: "conflict"
        )
        #expect(try encodeHex(failure) == vectors.failureResponse)

        let backendContractFailure = StorageWireResponse
            .failure(
                status: .backendContractViolation,
                message: "sqlite cursor contract"
            )
        #expect(
            try encodeHex(backendContractFailure)
                == vectors.backendContractFailureResponse
        )
    }

    @Test func canonicalVectorsDecodeAndReencodeWithoutChange() throws {
        let vectors = try loadVectors()
        for hex in [
            vectors.readinessRequest,
            vectors.rangeRequest,
            vectors.commitRequest,
            vectors.versionstampedCommitRequest,
            vectors.rangeSizeRequest,
            vectors.rangeSplitPointsRequest,
        ] {
            let bytes = try decodeHex(hex)
            let request = try StorageWire.decodeRequest(bytes)
            #expect(
                try StorageWire.encode(request)
                    == ByteString(bytes)
            )
        }
        for hex in [
            vectors.rangeResponse,
            vectors.rangeSizeResponse,
            vectors.rangeSplitPointsResponse,
            vectors.failureResponse,
            vectors.backendContractFailureResponse,
        ] {
            let bytes = try decodeHex(hex)
            let response = try StorageWire.decodeResponse(bytes)
            #expect(
                try StorageWire.encode(response)
                    == ByteString(bytes)
            )
        }
    }

    @Test func sharedInvalidSuccessFrameIsRejected() throws {
        let vectors = try loadVectors()
        for hex in [
            vectors.invalidSuccessWithoutOperation,
            vectors.invalidNegativeReadinessVersion,
            vectors.invalidEmptyRangeContinuation,
        ] {
            let bytes = try decodeHex(hex)
            #expect(throws: StorageWireProtocolError.self) {
                _ = try StorageWire.decodeResponse(
                    bytes
                )
            }
        }
        let invalidRequest = try decodeHex(
            vectors.invalidUnknownAtomicMutation
        )
        #expect(throws: StorageWireProtocolError.self) {
            _ = try StorageWire.decodeRequest(
                invalidRequest
            )
        }
    }

    private struct Vectors: Decodable {
        let readinessRequest: String
        let rangeRequest: String
        let commitRequest: String
        let versionstampedCommitRequest: String
        let rangeResponse: String
        let rangeSizeRequest: String
        let rangeSizeResponse: String
        let rangeSplitPointsRequest: String
        let rangeSplitPointsResponse: String
        let failureResponse: String
        let backendContractFailureResponse: String
        let invalidSuccessWithoutOperation: String
        let invalidNegativeReadinessVersion: String
        let invalidEmptyRangeContinuation: String
        let invalidUnknownAtomicMutation: String
    }

    private enum VectorError: Error {
        case missingGoldenVectorResource
        case invalidHex
    }

    private func loadVectors() throws -> Vectors {
        guard let url = Bundle.module.url(
            forResource: "StorageKitWireV1",
            withExtension: "json",
            subdirectory: "GoldenVectors"
        ) else {
            throw VectorError.missingGoldenVectorResource
        }
        return try JSONDecoder().decode(Vectors.self, from: Data(contentsOf: url))
    }

    private func encodeHex(
        _ request: StorageWireRequest
    ) throws -> String {
        hex(try StorageWire.encode(request))
    }

    private func encodeHex(
        _ response: StorageWireResponse
    ) throws -> String {
        hex(try StorageWire.encode(response))
    }

    private func hex<ByteString: Collection>(_ bytes: ByteString) -> String
    where ByteString.Element == UInt8 {
        let alphabet = Array("0123456789abcdef".utf8)
        var output: [UInt8] = []
        output.reserveCapacity(bytes.count * 2)
        for byte in bytes {
            output.append(alphabet[Int(byte >> 4)])
            output.append(alphabet[Int(byte & 0x0F)])
        }
        return String(decoding: output, as: UTF8.self)
    }

    private func decodeHex(_ value: String) throws -> [UInt8] {
        let bytes = Array(value.utf8)
        guard bytes.count.isMultiple(of: 2) else {
            throw VectorError.invalidHex
        }
        var output: [UInt8] = []
        output.reserveCapacity(bytes.count / 2)
        var index = 0
        while index < bytes.count {
            guard let high = nibble(bytes[index]),
                  let low = nibble(bytes[index + 1]) else {
                throw VectorError.invalidHex
            }
            output.append((high << 4) | low)
            index += 2
        }
        return output
    }

    private func nibble(_ byte: UInt8) -> UInt8? {
        switch byte {
        case 48...57:
            return byte - 48
        case 65...70:
            return byte - 65 + 10
        case 97...102:
            return byte - 97 + 10
        default:
            return nil
        }
    }
}
