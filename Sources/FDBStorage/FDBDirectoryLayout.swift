import DatabaseTypes
import FoundationDB
import StorageKit

/// Layout V1 of StorageKit Directories over the native FoundationDB
/// Directory Layer (SPEC §7.3).
///
/// Every StorageKit node is one native directory below the engine's
/// configured root path (`FDBStorageEngine.Configuration.rootPath`). The node kind is carried twice: as the native layer
/// type, which every open verifies, and as a one-character prefix of the
/// native name, so a Directory and a Partition with equal bytes never collide
/// and listings can filter by kind without reading node metadata.
///
/// ```text
/// StorageKit address                         native path
/// root                                       rootPath
/// /directory("app")                          rootPath + ["dapp"]
/// /directory("app")/partition(0x01 0xFF)     rootPath + ["dapp", "p01ff"]
/// ```
///
/// The layer type doubles as the layout marker: a root or child whose native
/// type is absent or foreign is rejected as `incompatibleStorageLayout`,
/// never adopted.
enum FDBDirectoryLayout {
    /// Native layer type of the root and of every Directory node.
    static let directoryType = DirectoryType.custom("storage-kit.directory.v1")

    /// Native layer type of every Partition node. Partitions are custom-typed
    /// directories rather than native partitions because native partitions
    /// cannot nest and StorageKit Partitions may.
    static let partitionType = DirectoryType.custom("storage-kit.partition.v1")

    /// Decoded form of one native child name below a StorageKit node.
    enum NativeComponent: Equatable {
        case directory(String)
        case partition(PartitionID)
        /// A native child that StorageKit did not create; listings skip it and
        /// removal treats it as a child node.
        case foreign(String)
        /// A Partition name whose identifier bytes are not decodable.
        case corrupt(String)
    }

    private static let directoryPrefix: UInt8 = UInt8(ascii: "d")
    private static let partitionPrefix: UInt8 = UInt8(ascii: "p")

    static func nativeType(for step: StorageAddressStep) -> DirectoryType {
        switch step {
        case .directory:
            return directoryType
        case .partition:
            return partitionType
        }
    }

    static func nativeComponent(for step: StorageAddressStep) -> String {
        switch step {
        case .directory(let name):
            return "d" + name
        case .partition(let id):
            return "p" + hexEncode(id.bytes)
        }
    }

    static func nativePath(rootPath: [String], address: StorageAddress) -> [String] {
        var path = rootPath
        path.reserveCapacity(rootPath.count + address.steps.count)
        for step in address.steps {
            path.append(nativeComponent(for: step))
        }
        return path
    }

    static func decode(_ component: String) -> NativeComponent {
        let utf8 = component.utf8
        guard let kind = utf8.first else {
            return .foreign(component)
        }
        let payload = component.dropFirst()
        switch kind {
        case directoryPrefix:
            return .directory(String(payload))
        case partitionPrefix:
            guard let bytes = hexDecode(payload) else {
                return .corrupt(component)
            }
            switch Result(catching: { () throws(DirectoryAddressError) in try PartitionID(ByteString(bytes)) }) {
            case .success(let id):
                return .partition(id)
            case .failure:
                return .corrupt(component)
            }
        default:
            return .foreign(component)
        }
    }

    // MARK: - Hex

    private static let hexDigits: [UInt8] = Array("0123456789abcdef".utf8)

    private static func hexEncode(_ bytes: ByteString) -> String {
        var encoded: [UInt8] = []
        encoded.reserveCapacity(bytes.count * 2)
        for byte in bytes {
            encoded.append(hexDigits[Int(byte >> 4)])
            encoded.append(hexDigits[Int(byte & 0x0F)])
        }
        return String(decoding: encoded, as: UTF8.self)
    }

    private static func hexDecode(_ text: Substring) -> [UInt8]? {
        let utf8 = text.utf8
        guard utf8.count.isMultiple(of: 2) else {
            return nil
        }
        var bytes: [UInt8] = []
        bytes.reserveCapacity(utf8.count / 2)
        var high: UInt8?
        for digit in utf8 {
            guard let nibble = nibble(of: digit) else {
                return nil
            }
            if let pending = high {
                bytes.append((pending << 4) | nibble)
                high = nil
            } else {
                high = nibble
            }
        }
        return bytes
    }

    private static func nibble(of digit: UInt8) -> UInt8? {
        switch digit {
        case UInt8(ascii: "0")...UInt8(ascii: "9"):
            return digit - UInt8(ascii: "0")
        case UInt8(ascii: "a")...UInt8(ascii: "f"):
            return digit - UInt8(ascii: "a") + 10
        default:
            return nil
        }
    }
}
