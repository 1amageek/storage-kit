import DatabaseTypes

/// Padding-free base64url encoding for storage names and host boundary strings.
public enum StorageWireBase64URL {
    private static let alphabet = Array("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".utf8)

    public static func encode<Source: Collection>(
        _ bytes: Source
    ) -> String where Source.Element == UInt8 {
        if bytes.isEmpty {
            return ""
        }
        var output: [UInt8] = []
        output.reserveCapacity(((bytes.count + 2) / 3) * 4)

        var iterator = bytes.makeIterator()
        while let first = iterator.next() {
            let second = iterator.next()
            let third = iterator.next()

            output.append(alphabet[Int(first >> 2)])
            output.append(alphabet[Int(
                ((first & 0x03) << 4) | ((second ?? 0) >> 4)
            )])
            if let second {
                output.append(alphabet[Int(
                    ((second & 0x0f) << 2) | ((third ?? 0) >> 6)
                )])
            }
            if let third {
                output.append(alphabet[Int(third & 0x3f)])
            }
        }

        return String(decoding: output, as: UTF8.self)
    }

    public static func decode(
        _ value: String
    ) throws(StorageWireError) -> ByteString {
        if value.isEmpty {
            return []
        }
        guard value.utf8.count % 4 != 1 else {
            throw StorageWireError.invalidCursor
        }
        var output: [UInt8] = []
        output.reserveCapacity((value.utf8.count * 3) / 4)

        var buffer: UInt32 = 0
        var bitCount = 0
        for byte in value.utf8 {
            guard let sixBits = decode(byte) else {
                throw StorageWireError.invalidCursor
            }
            buffer = (buffer << 6) | UInt32(sixBits)
            bitCount += 6
            while bitCount >= 8 {
                bitCount -= 8
                output.append(UInt8(truncatingIfNeeded: buffer >> UInt32(bitCount)))
                if bitCount > 0 {
                    buffer &= (1 << UInt32(bitCount)) - 1
                } else {
                    buffer = 0
                }
            }
        }

        guard encode(output) == value else {
            throw StorageWireError.invalidCursor
        }
        return ByteString(output)
    }

    private static func decode(_ byte: UInt8) -> UInt8? {
        switch byte {
        case 65...90:
            return byte - 65
        case 97...122:
            return byte - 97 + 26
        case 48...57:
            return byte - 48 + 52
        case 45:
            return 62
        case 95:
            return 63
        default:
            return nil
        }
    }
}
