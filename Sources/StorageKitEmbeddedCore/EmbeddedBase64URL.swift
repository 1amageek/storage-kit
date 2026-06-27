/// Padding-free base64url codec for diagnostic names and host boundary strings.
public enum EmbeddedBase64URL {
    private static let alphabet = Array("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".utf8)

    public static func encode(_ bytes: [UInt8]) -> String {
        if bytes.isEmpty {
            return ""
        }
        var output: [UInt8] = []
        output.reserveCapacity(((bytes.count + 2) / 3) * 4)

        var index = 0
        while index < bytes.count {
            let first = bytes[index]
            let hasSecond = index + 1 < bytes.count
            let hasThird = index + 2 < bytes.count
            let second = hasSecond ? bytes[index + 1] : 0
            let third = hasThird ? bytes[index + 2] : 0

            output.append(alphabet[Int(first >> 2)])
            output.append(alphabet[Int(((first & 0x03) << 4) | (second >> 4))])
            if hasSecond {
                output.append(alphabet[Int(((second & 0x0f) << 2) | (third >> 6))])
            }
            if hasThird {
                output.append(alphabet[Int(third & 0x3f)])
            }
            index += 3
        }

        if let value = String(validating: output, as: UTF8.self) {
            return value
        }
        return ""
    }

    public static func decode(_ value: String) throws(EmbeddedWireError) -> [UInt8] {
        let input = Array(value.utf8)
        if input.isEmpty {
            return []
        }
        var output: [UInt8] = []
        output.reserveCapacity((input.count * 3) / 4)

        var buffer: UInt32 = 0
        var bitCount = 0
        for byte in input {
            guard let sixBits = decode(byte) else {
                throw EmbeddedWireError.invalidCursor
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

        return output
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
