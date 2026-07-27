import DatabaseTypes
import Foundation
import Testing
import CloudflareDurableObjectStorageEmbedded
import StorageKitEmbeddedCore

@Suite("StorageKit Wire v1 Golden Vector Tests")
struct StorageKitWireGoldenVectorTests {
    @Test func encoderMatchesCanonicalVectors() throws {
        let vectors = try loadVectors()
        let scope = try CloudflareDurableObjectEmbeddedScope(databaseID: "main")

        let readiness = CloudflareDurableObjectEmbeddedRequest.readiness(
            CloudflareDurableObjectEmbeddedReadinessRequest(scope: scope)
        )
        #expect(try encodeHex(readiness) == vectors.readinessRequest)

        let range = CloudflareDurableObjectEmbeddedRequest.range(
            CloudflareDurableObjectEmbeddedRangeRequest(
                scope: scope,
                begin: .unbounded,
                end: .selector(
                    EmbeddedKeySelector(key: [0x20], orEqual: true, offset: 1)
                ),
                limit: 2,
                reverse: false,
                snapshot: false,
                expectedReadVersion: 7,
                cursorKey: [0xFF]
            )
        )
        #expect(try encodeHex(range) == vectors.rangeRequest)

        let commit = CloudflareDurableObjectEmbeddedRequest.commit(
            CloudflareDurableObjectEmbeddedCommitRequest(
                scope: scope,
                observedReadVersion: 7,
                mutations: [
                    .set(key: [0x01], value: [0x0A, 0x0B]),
                    .clearRange(begin: [0x02], end: [0x04]),
                    .atomic(key: [0x05], param: [0x01], mutationType: .add),
                ],
                readConflictRanges: [
                    EmbeddedKeyRange(begin: nil, end: [0x09]),
                ],
                writeConflictRanges: [
                    EmbeddedKeyRange(begin: [0x05], end: [0x05, 0x00]),
                ]
            )
        )
        #expect(try encodeHex(commit) == vectors.commitRequest)

        let versionstampedCommit = CloudflareDurableObjectEmbeddedRequest
            .commit(
                CloudflareDurableObjectEmbeddedCommitRequest(
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

        let rangeResponse = CloudflareDurableObjectEmbeddedResponse.range(
            CloudflareDurableObjectEmbeddedRangeResponse(
                rows: [
                    EmbeddedKeyValue(key: [0x01], value: [0x0A]),
                    EmbeddedKeyValue(key: [0x02], value: [0x0B, 0x0C]),
                ],
                hasMore: true,
                currentCommitVersion: 8,
                readConflictRanges: [
                    EmbeddedKeyRange(begin: [0x01], end: [0x02]),
                    EmbeddedKeyRange(begin: [0x05], end: nil),
                ]
            )
        )
        #expect(try encodeHex(rangeResponse) == vectors.rangeResponse)

        let rangeSize = CloudflareDurableObjectEmbeddedRequest.rangeSize(
            CloudflareDurableObjectEmbeddedRangeSizeRequest(
                scope: scope,
                begin: [0x01],
                end: [0x04],
                expectedReadVersion: 7
            )
        )
        #expect(try encodeHex(rangeSize) == vectors.rangeSizeRequest)

        let rangeSizeResponse = CloudflareDurableObjectEmbeddedResponse
            .rangeSize(
                CloudflareDurableObjectEmbeddedRangeSizeResponse(
                    byteCount: 11,
                    currentCommitVersion: 8
                )
            )
        #expect(
            try encodeHex(rangeSizeResponse) == vectors.rangeSizeResponse
        )

        let splitPoints = CloudflareDurableObjectEmbeddedRequest
            .rangeSplitPoints(
                CloudflareDurableObjectEmbeddedRangeSplitPointsRequest(
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

        let splitPointsResponse = CloudflareDurableObjectEmbeddedResponse
            .rangeSplitPoints(
                CloudflareDurableObjectEmbeddedRangeSplitPointsResponse(
                    splitPoints: [[0x01], [0x03], [0x04]],
                    currentCommitVersion: 8
                )
            )
        #expect(
            try encodeHex(splitPointsResponse)
                == vectors.rangeSplitPointsResponse
        )

        let failure = CloudflareDurableObjectEmbeddedResponse.failure(
            status: .transactionConflict,
            message: "conflict"
        )
        #expect(try encodeHex(failure) == vectors.failureResponse)

        let backendContractFailure = CloudflareDurableObjectEmbeddedResponse
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
            let request = try CloudflareDurableObjectStorageWireCodec.decodeRequest(bytes)
            #expect(
                try CloudflareDurableObjectStorageWireCodec.encode(request)
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
            let response = try CloudflareDurableObjectStorageWireCodec.decodeResponse(bytes)
            #expect(
                try CloudflareDurableObjectStorageWireCodec.encode(response)
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
            #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
                _ = try CloudflareDurableObjectStorageWireCodec.decodeResponse(
                    bytes
                )
            }
        }
        let invalidRequest = try decodeHex(
            vectors.invalidUnknownAtomicMutation
        )
        #expect(throws: CloudflareDurableObjectEmbeddedError.self) {
            _ = try CloudflareDurableObjectStorageWireCodec.decodeRequest(
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
        _ request: CloudflareDurableObjectEmbeddedRequest
    ) throws -> String {
        hex(try CloudflareDurableObjectStorageWireCodec.encode(request))
    }

    private func encodeHex(
        _ response: CloudflareDurableObjectEmbeddedResponse
    ) throws -> String {
        hex(try CloudflareDurableObjectStorageWireCodec.encode(response))
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
