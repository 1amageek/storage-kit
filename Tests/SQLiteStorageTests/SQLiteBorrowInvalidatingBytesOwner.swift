import StorageKit
import Synchronization

/// Poisons its allocation when a borrow ends so tests can detect escaped pointers.
final class SQLiteBorrowInvalidatingBytesOwner: BytesOwner {
    let count: Int

    private struct State {
        let address: UInt
        var borrowCount: Int
        var activeBorrowCount: Int
    }

    private let state: Mutex<State>

    init(_ bytes: [UInt8]) {
        precondition(!bytes.isEmpty)
        let pointer = UnsafeMutableRawPointer.allocate(
            byteCount: bytes.count,
            alignment: MemoryLayout<UInt8>.alignment
        )
        bytes.withUnsafeBytes { source in
            guard let sourceAddress = source.baseAddress else {
                preconditionFailure("Non-empty test bytes have no address")
            }
            pointer.copyMemory(from: sourceAddress, byteCount: source.count)
        }
        self.count = bytes.count
        self.state = Mutex(
            State(
                address: UInt(bitPattern: pointer),
                borrowCount: 0,
                activeBorrowCount: 0
            )
        )
    }

    deinit {
        state.withLock { state in
            Self.pointer(at: state.address).deallocate()
        }
    }

    var borrowCount: Int {
        state.withLock { $0.borrowCount }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        let address = state.withLock { state in
            state.borrowCount += 1
            state.activeBorrowCount += 1
            return state.address
        }
        defer {
            state.withLock { state in
                state.activeBorrowCount -= 1
                guard state.activeBorrowCount == 0 else {
                    return
                }
                let pointer = Self.pointer(at: state.address)
                pointer.initializeMemory(
                    as: UInt8.self,
                    repeating: 0xEE,
                    count: count
                )
            }
        }
        let pointer = Self.pointer(at: address)
        try body(UnsafeRawBufferPointer(start: pointer, count: count))
    }

    private static func pointer(at address: UInt) -> UnsafeMutableRawPointer {
        guard let pointer = UnsafeMutableRawPointer(bitPattern: address) else {
            preconditionFailure("Test byte owner has an invalid address")
        }
        return pointer
    }
}
