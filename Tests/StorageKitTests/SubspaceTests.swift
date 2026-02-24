import Testing
import Foundation
@testable import StorageKit

@Suite("Subspace Tests")
struct SubspaceTests {

    @Test func subspaceNesting() throws {
        let root = Subspace(prefix: [])
        let child = root.subspace("users")
        let grandchild = child.subspace(Int64(42))

        // grandchild のプレフィックスは "users" + 42 のエンコード結果
        let expectedPrefix = Tuple("users").pack() + Tuple(Int64(42)).pack()
        #expect(grandchild.prefix == expectedPrefix)
    }

    @Test func packUnpack() throws {
        let space = Subspace("myapp", "data")
        let tuple = Tuple("key1", Int64(100))
        let key = space.pack(tuple)

        // キーは prefix + tuple.pack()
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

        // begin は prefix + 0x00
        #expect(begin == space.prefix + [0x00])
        // end は strinc(prefix)
        let expected = try strinc(space.prefix)
        #expect(end == expected)
    }

    @Test func rangeFromTo() throws {
        let space = Subspace("data")
        let start = Tuple(Int64(10))
        let end = Tuple(Int64(20))
        let (beginKey, endKey) = space.range(from: start, to: end)

        #expect(beginKey == space.prefix + start.pack())
        #expect(endKey == space.prefix + end.pack())
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

        let expected = Tuple("app").pack() + Tuple("users").pack() + Tuple(Int64(1)).pack()
        #expect(nested.prefix == expected)
    }

    @Test func unpackWithWrongPrefix() throws {
        let space = Subspace("correct")
        let wrongKey = Subspace("wrong").pack(Tuple("data"))

        #expect(throws: TupleError.self) {
            try space.unpack(wrongKey)
        }
    }
}
