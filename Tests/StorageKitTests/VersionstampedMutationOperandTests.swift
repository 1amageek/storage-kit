import DatabaseTypes
import StorageKit
import Testing

@Suite("Versionstamped mutation operand")
struct VersionstampedMutationOperandTests {
    @Test("Decodes a valid operand and protects a prefix")
    func validOperand() throws {
        let operand = makeOperand(prefix: [1, 2, 3], offset: 3)
        let decoded = try VersionstampedMutationOperand(operand)

        #expect(decoded.payloadByteCount == 13)
        #expect(decoded.replacementOffset == 3)
        try decoded.validateReplacement(afterProtectedPrefixByteCount: 3)
    }

    @Test("Rejects a truncated operand")
    func truncatedOperand() {
        #expect(throws: VersionstampedMutationOperandError.self) {
            _ = try VersionstampedMutationOperand(
                ByteString([UInt8](repeating: 0xFF, count: 13))
            )
        }
    }

    @Test("Rejects an offset beyond the payload")
    func offsetBeyondPayload() {
        #expect(throws: VersionstampedMutationOperandError.self) {
            _ = try VersionstampedMutationOperand(
                makeOperand(prefix: [], offset: UInt32.max)
            )
        }
    }

    @Test("Rejects a non-placeholder replacement range")
    func invalidPlaceholder() {
        var bytes = [UInt8](repeating: 0xFF, count: 14)
        bytes[0] = 0
        #expect(throws: VersionstampedMutationOperandError.self) {
            _ = try VersionstampedMutationOperand(ByteString(bytes))
        }
    }

    @Test("Rejects replacement inside a protected prefix")
    func protectedPrefixOverlap() throws {
        let decoded = try VersionstampedMutationOperand(
            makeOperand(prefix: [0xFF, 0xFF, 0xFF], offset: 2)
        )
        #expect(throws: VersionstampedMutationOperandError.self) {
            try decoded.validateReplacement(afterProtectedPrefixByteCount: 3)
        }
    }

    private func makeOperand(
        prefix: [UInt8],
        offset: UInt32
    ) -> ByteString {
        var bytes = prefix + [UInt8](repeating: 0xFF, count: 10)
        let encodedOffset = offset.littleEndian
        withUnsafeBytes(of: encodedOffset) { bytes.append(contentsOf: $0) }
        return ByteString(bytes)
    }
}
