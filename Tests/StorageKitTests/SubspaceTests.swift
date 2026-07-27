import DatabaseTypes
import Testing
import Foundation
@testable import StorageKit

@Suite("Subspace Tests")
struct SubspaceTests {

    @Test func subspaceNesting() throws {
        let root = Subspace(prefix: [])
        let child = root.subspace("users")
        let grandchild = child.subspace(Int64(42))

        let expectedPrefix = concatenate(
            Tuple("users").pack(),
            Tuple(Int64(42)).pack()
        )
        #expect(grandchild.prefix == expectedPrefix)
    }

    @Test func packUnpack() throws {
        let space = Subspace("myapp", "data")
        let tuple = Tuple("key1", Int64(100))
        let key = space.pack(tuple)

        #expect(key.starts(with: space.prefix))

        let unpacked = try space.unpack(key)
        let elements = try Tuple.unpack(from: unpacked.pack())
        #expect(elements[0] as? String == "key1")
        #expect(elements[1] as? Int64 == 100)
    }

    @Test func contains() throws {
        let space = Subspace("test")
        let key = space.pack(Tuple("inner"))
        let otherKey = Subspace("other").pack(Tuple("inner"))

        #expect(space.contains(key))
        #expect(!space.contains(otherKey))
        #expect(!space.contains([]))
    }

    @Test func range() throws {
        let space = Subspace("users")
        let (begin, end) = space.range()

        #expect(begin == concatenate(space.prefix, [0x00]))
        let expected = try strinc(space.prefix)
        #expect(end == expected)
    }

    @Test func rangeFromTo() throws {
        let space = Subspace("data")
        let start = Tuple(Int64(10))
        let end = Tuple(Int64(20))
        let (beginKey, endKey) = space.range(from: start, to: end)

        #expect(beginKey == concatenate(space.prefix, start.pack()))
        #expect(endKey == concatenate(space.prefix, end.pack()))
    }

    @Test func prefixRange() throws {
        let space = Subspace("test")
        let (begin, end) = try space.prefixRange()

        #expect(begin == space.prefix)
        let expected = try strinc(space.prefix)
        #expect(end == expected)
    }

    @Test func subscriptAccess() throws {
        let root = Subspace(prefix: [])
        let nested = root["app"]["users"][Int64(1)]

        let expected = concatenate(
            Tuple("app").pack(),
            Tuple("users").pack(),
            Tuple(Int64(1)).pack()
        )
        #expect(nested.prefix == expected)
    }

    @Test func unpackWithWrongPrefix() throws {
        let space = Subspace("correct")
        let wrongKey = Subspace("wrong").pack(Tuple("data"))

        #expect(throws: TupleError.self) {
            try space.unpack(wrongKey)
        }
    }

    private func concatenate(
        _ parts: ByteString...
    ) -> ByteString {
        ByteString(parts.flatMap { $0 })
    }
}
