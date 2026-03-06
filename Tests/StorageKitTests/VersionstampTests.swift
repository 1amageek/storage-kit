import Testing
import Foundation
@testable import StorageKit

@Suite("Versionstamp Tests")
struct VersionstampTests {

    // MARK: - Construction

    @Test func completeVersionstamp() {
        let tv: Bytes = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]
        let vs = Versionstamp(transactionVersion: tv, userVersion: 42)

        #expect(vs.isComplete == true)
        #expect(vs.transactionVersion == tv)
        #expect(vs.userVersion == 42)
    }

    @Test func incompleteVersionstamp() {
        let vs = Versionstamp.incomplete(userVersion: 7)

        #expect(vs.isComplete == false)
        #expect(vs.transactionVersion == nil)
        #expect(vs.userVersion == 7)
    }

    @Test func incompleteVersionstampDefaultUserVersion() {
        let vs = Versionstamp.incomplete()

        #expect(vs.isComplete == false)
        #expect(vs.userVersion == 0)
    }

    @Test func completeVersionstampDefaultUserVersion() {
        let tv = Bytes(repeating: 0xAB, count: 10)
        let vs = Versionstamp(transactionVersion: tv)

        #expect(vs.userVersion == 0)
    }

    // MARK: - toBytes / fromBytes Round-Trip

    @Test func completeToBytesRoundTrip() throws {
        let tv: Bytes = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]
        let original = Versionstamp(transactionVersion: tv, userVersion: 1000)
        let bytes = original.toBytes()

        #expect(bytes.count == 12)
        // First 10 bytes: transaction version
        #expect(Array(bytes.prefix(10)) == tv)
        // Last 2 bytes: user version 1000 in big-endian = 0x03E8
        #expect(bytes[10] == 0x03)
        #expect(bytes[11] == 0xE8)

        let decoded = try Versionstamp.fromBytes(bytes)
        #expect(decoded.isComplete == true)
        #expect(decoded.transactionVersion == tv)
        #expect(decoded.userVersion == 1000)
    }

    @Test func incompleteToBytesRoundTrip() throws {
        let original = Versionstamp.incomplete(userVersion: 5)
        let bytes = original.toBytes()

        #expect(bytes.count == 12)
        // First 10 bytes: all 0xFF (incomplete placeholder)
        for i in 0..<10 {
            #expect(bytes[i] == 0xFF, "Byte \(i) should be 0xFF for incomplete")
        }
        // Last 2 bytes: user version 5 in big-endian = 0x0005
        #expect(bytes[10] == 0x00)
        #expect(bytes[11] == 0x05)

        let decoded = try Versionstamp.fromBytes(bytes)
        #expect(decoded.isComplete == false)
        #expect(decoded.transactionVersion == nil)
        #expect(decoded.userVersion == 5)
    }

    @Test func fromBytesInvalidLength() {
        let shortBytes: Bytes = [0x00, 0x01, 0x02]
        #expect(throws: TupleError.self) {
            _ = try Versionstamp.fromBytes(shortBytes)
        }

        let longBytes = Bytes(repeating: 0x00, count: 13)
        #expect(throws: TupleError.self) {
            _ = try Versionstamp.fromBytes(longBytes)
        }

        let emptyBytes: Bytes = []
        #expect(throws: TupleError.self) {
            _ = try Versionstamp.fromBytes(emptyBytes)
        }
    }

    @Test func userVersionBoundaryValues() throws {
        let tv = Bytes(repeating: 0x01, count: 10)

        // Min user version
        let vsMin = Versionstamp(transactionVersion: tv, userVersion: 0)
        let bytesMin = vsMin.toBytes()
        let decodedMin = try Versionstamp.fromBytes(bytesMin)
        #expect(decodedMin.userVersion == 0)

        // Max user version
        let vsMax = Versionstamp(transactionVersion: tv, userVersion: UInt16.max)
        let bytesMax = vsMax.toBytes()
        let decodedMax = try Versionstamp.fromBytes(bytesMax)
        #expect(decodedMax.userVersion == UInt16.max)
    }

    // MARK: - TupleElement Encoding/Decoding

    @Test func tupleElementRoundTrip() throws {
        let tv: Bytes = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]
        let original = Versionstamp(transactionVersion: tv, userVersion: 100)
        let encoded = original.encodeTuple()

        // Type code 0x33 + 12 bytes = 13 bytes total
        #expect(encoded.count == 13)
        #expect(encoded[0] == TupleTypeCode.versionstamp.rawValue)
        #expect(encoded[0] == 0x33)

        var offset = 1
        let decoded = try Versionstamp.decodeTuple(from: encoded, at: &offset)
        #expect(decoded == original)
        #expect(offset == 13) // fully consumed
    }

    @Test func incompleteTupleElementRoundTrip() throws {
        let original = Versionstamp.incomplete(userVersion: 0)
        let encoded = original.encodeTuple()

        #expect(encoded.count == 13)
        #expect(encoded[0] == 0x33)
        // Bytes 1-10 should be 0xFF (incomplete placeholder)
        for i in 1...10 {
            #expect(encoded[i] == 0xFF, "Byte \(i) should be 0xFF for incomplete versionstamp")
        }

        var offset = 1
        let decoded = try Versionstamp.decodeTuple(from: encoded, at: &offset)
        #expect(decoded.isComplete == false)
        #expect(decoded.userVersion == 0)
    }

    @Test func decodeTupleTruncatedData() {
        // Only 5 bytes of data after type code (need 12)
        let truncated: Bytes = [0x33, 0x00, 0x01, 0x02, 0x03, 0x04]
        var offset = 1
        #expect(throws: TupleError.self) {
            _ = try Versionstamp.decodeTuple(from: truncated, at: &offset)
        }
    }

    @Test func decodeTupleEmptyAfterTypeCode() {
        let bytes: Bytes = [0x33]
        var offset = 1
        #expect(throws: TupleError.self) {
            _ = try Versionstamp.decodeTuple(from: bytes, at: &offset)
        }
    }

    // MARK: - Tuple Pack/Unpack Integration

    @Test func tuplePackUnpackWithVersionstamp() throws {
        let tv: Bytes = [0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13]
        let vs = Versionstamp(transactionVersion: tv, userVersion: 99)
        let tuple = Tuple(vs)

        let packed = tuple.pack()
        let elements = try Tuple.unpack(from: packed)

        #expect(elements.count == 1)
        let decoded = elements[0] as? Versionstamp
        #expect(decoded != nil)
        #expect(decoded == vs)
    }

    @Test func tuplePackUnpackMixedWithVersionstamp() throws {
        let tv: Bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
        let vs = Versionstamp(transactionVersion: tv, userVersion: 1)
        let tuple = Tuple("prefix", Int64(42), vs, true)

        let packed = tuple.pack()
        let elements = try Tuple.unpack(from: packed)

        #expect(elements.count == 4)
        #expect(elements[0] as? String == "prefix")
        #expect(elements[1] as? Int64 == 42)
        let decodedVS = elements[2] as? Versionstamp
        #expect(decodedVS != nil)
        #expect(decodedVS?.transactionVersion == tv)
        #expect(decodedVS?.userVersion == 1)
        #expect(elements[3] as? Bool == true)
    }

    @Test func tupleSubscriptWithVersionstamp() throws {
        let tv = Bytes(repeating: 0xAA, count: 10)
        let vs = Versionstamp(transactionVersion: tv, userVersion: 50)
        let tuple = Tuple("key", vs)

        #expect(tuple.count == 2)
        #expect(tuple[0] as? String == "key")
        let element = tuple[1] as? Versionstamp
        #expect(element == vs)
    }

    @Test func tupleAppendVersionstamp() throws {
        let tv = Bytes(repeating: 0x01, count: 10)
        let vs = Versionstamp(transactionVersion: tv, userVersion: 0)
        let tuple = Tuple("base").appending(vs)

        let elements = try Tuple.unpack(from: tuple.pack())
        #expect(elements.count == 2)
        #expect(elements[0] as? String == "base")
        #expect(elements[1] as? Versionstamp == vs)
    }

    // MARK: - Comparable

    @Test func comparableOrdering() {
        // Earlier transaction version < later transaction version
        let tv1: Bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
        let tv2: Bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]

        let vs1 = Versionstamp(transactionVersion: tv1, userVersion: 0)
        let vs2 = Versionstamp(transactionVersion: tv2, userVersion: 0)

        #expect(vs1 < vs2)
        #expect(!(vs2 < vs1))
    }

    @Test func comparableOrderingByUserVersion() {
        // Same transaction version, different user version
        let tv: Bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]

        let vs1 = Versionstamp(transactionVersion: tv, userVersion: 0)
        let vs2 = Versionstamp(transactionVersion: tv, userVersion: 1)
        let vs3 = Versionstamp(transactionVersion: tv, userVersion: 65535)

        #expect(vs1 < vs2)
        #expect(vs2 < vs3)
        #expect(vs1 < vs3)
    }

    @Test func comparableIncomplete() {
        // Incomplete versionstamps (0xFF...) are "greater" than any complete one
        let tvMax: Bytes = [0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        let complete = Versionstamp(transactionVersion: tvMax, userVersion: UInt16.max)
        let incomplete = Versionstamp.incomplete(userVersion: 0)

        #expect(complete < incomplete)
    }

    @Test func lexicographicOrderingInTuple() {
        let tv1: Bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
        let tv2: Bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]

        let packed1 = Tuple(Versionstamp(transactionVersion: tv1, userVersion: 0)).pack()
        let packed2 = Tuple(Versionstamp(transactionVersion: tv2, userVersion: 0)).pack()

        #expect(compareBytes(packed1, packed2) < 0)
    }

    // MARK: - Equatable / Hashable

    @Test func equality() {
        let tv: Bytes = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]
        let vs1 = Versionstamp(transactionVersion: tv, userVersion: 42)
        let vs2 = Versionstamp(transactionVersion: tv, userVersion: 42)
        let vs3 = Versionstamp(transactionVersion: tv, userVersion: 43)

        #expect(vs1 == vs2)
        #expect(vs1 != vs3)
    }

    @Test func equalityIncomplete() {
        let vs1 = Versionstamp.incomplete(userVersion: 0)
        let vs2 = Versionstamp.incomplete(userVersion: 0)
        let vs3 = Versionstamp.incomplete(userVersion: 1)

        #expect(vs1 == vs2)
        #expect(vs1 != vs3)
    }

    @Test func hashable() {
        let tv: Bytes = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]
        let vs1 = Versionstamp(transactionVersion: tv, userVersion: 42)
        let vs2 = Versionstamp(transactionVersion: tv, userVersion: 42)

        var set = Set<Versionstamp>()
        set.insert(vs1)
        set.insert(vs2)
        #expect(set.count == 1)
    }

    // MARK: - Tuple Equality with Versionstamp

    @Test func tupleEqualityWithVersionstamp() {
        let tv: Bytes = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]
        let vs = Versionstamp(transactionVersion: tv, userVersion: 0)

        let t1 = Tuple("key", vs)
        let t2 = Tuple("key", vs)

        #expect(t1 == t2)
    }

    // MARK: - Nested Tuple with Versionstamp

    @Test func nestedTupleWithVersionstamp() throws {
        let tv: Bytes = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]
        let vs = Versionstamp(transactionVersion: tv, userVersion: 10)
        let inner = Tuple(vs, Int64(7))
        let outer = Tuple("outer", inner)

        let packed = outer.pack()
        let elements = try Tuple.unpack(from: packed)

        #expect(elements.count == 2)
        #expect(elements[0] as? String == "outer")

        let decodedInner = elements[1] as? Tuple
        #expect(decodedInner != nil)

        let innerPacked = decodedInner!.pack()
        let innerElements = try Tuple.unpack(from: innerPacked)
        #expect(innerElements.count == 2)

        let decodedVS = innerElements[0] as? Versionstamp
        #expect(decodedVS != nil)
        #expect(decodedVS?.transactionVersion == tv)
        #expect(decodedVS?.userVersion == 10)
        #expect(innerElements[1] as? Int64 == 7)
    }

    // MARK: - CustomStringConvertible

    @Test func descriptionComplete() {
        let tv: Bytes = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]
        let vs = Versionstamp(transactionVersion: tv, userVersion: 42)

        #expect(vs.description == "Versionstamp(tr:00010203040506070809, user:42)")
    }

    @Test func descriptionIncomplete() {
        let vs = Versionstamp.incomplete(userVersion: 0)
        #expect(vs.description == "Versionstamp(incomplete, user:0)")
    }

    // MARK: - Binary Format Verification

    @Test func typeCode() {
        #expect(TupleTypeCode.versionstamp.rawValue == 0x33)
    }

    @Test func encodedLayout() {
        let tv: Bytes = [0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09]
        let vs = Versionstamp(transactionVersion: tv, userVersion: 256) // 0x0100
        let encoded = vs.encodeTuple()

        // [0x33] [10 bytes TV] [2 bytes UV big-endian]
        #expect(encoded[0] == 0x33)
        #expect(Array(encoded[1...10]) == tv)
        #expect(encoded[11] == 0x01) // 256 >> 8
        #expect(encoded[12] == 0x00) // 256 & 0xFF
    }

    @Test func zeroTransactionVersion() throws {
        let tv = Bytes(repeating: 0x00, count: 10)
        let vs = Versionstamp(transactionVersion: tv, userVersion: 0)

        let bytes = vs.toBytes()
        let decoded = try Versionstamp.fromBytes(bytes)
        #expect(decoded.isComplete == true) // all-zero is complete (not all-0xFF)
        #expect(decoded.transactionVersion == tv)

        // Tuple round-trip
        let encoded = vs.encodeTuple()
        var offset = 1
        let tupleDecoded = try Versionstamp.decodeTuple(from: encoded, at: &offset)
        #expect(tupleDecoded == vs)
    }

    // MARK: - Constants Verification

    @Test func sizeConstants() {
        #expect(Versionstamp.transactionVersionSize == 10)
        #expect(Versionstamp.userVersionSize == 2)
        #expect(Versionstamp.totalSize == 12)
    }
}
