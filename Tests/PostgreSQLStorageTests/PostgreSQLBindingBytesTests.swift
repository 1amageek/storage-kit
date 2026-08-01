import DatabaseTypes
import Synchronization
import Testing
@testable import PostgreSQLStorage

extension SerializedPostgreSQLStorageTests {
    @Suite("PostgreSQL Binding Byte Ownership Tests")
    struct PostgreSQLBindingBytesTests {
        @Test func copiesOnceIntoIndependentDriverStorage() throws {
            let sourceOwner = BorrowCountingByteStringOwner(
                bytes: [0x10, 0x20, 0x30, 0x40]
            )
            let source = ByteString(retaining: sourceOwner)

            let destination = PostgreSQLBindingBytes.copyToOwnedBuffer(source)
            let observation = sourceOwner.observation
            let destinationAddress = destination.withUnsafeReadableBytes {
                UInt(bitPattern: $0.baseAddress)
            }

            #expect(observation.borrowCount == 1)
            #expect(observation.address != 0)
            #expect(destinationAddress != 0)
            #expect(destinationAddress != observation.address)
            #expect(Array(destination.readableBytesView) == sourceOwner.bytes)
        }
    }
}

private final class BorrowCountingByteStringOwner: ByteStringOwner {
    struct Observation: Sendable {
        var borrowCount = 0
        var address: UInt = 0
    }

    let bytes: [UInt8]
    private let state = Mutex(Observation())

    init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }
    var retainedByteCount: Int? { bytes.capacity }
    var isStorageSelfContained: Bool { true }

    var observation: Observation {
        state.withLock { $0 }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes { source in
            state.withLock { observation in
                observation.borrowCount += 1
                observation.address = UInt(bitPattern: source.baseAddress)
            }
            try body(source)
        }
    }
}
