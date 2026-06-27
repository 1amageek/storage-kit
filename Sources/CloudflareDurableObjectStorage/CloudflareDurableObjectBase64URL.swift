enum CloudflareDurableObjectBase64URL {
    private static let alphabet = Array("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".utf8)

    static func encode(_ bytes: [UInt8]) -> String {
        var output: [UInt8] = []
        output.reserveCapacity(((bytes.count + 2) / 3) * 4)

        var index = 0
        while index + 3 <= bytes.count {
            let value = (UInt32(bytes[index]) << 16)
                | (UInt32(bytes[index + 1]) << 8)
                | UInt32(bytes[index + 2])
            output.append(alphabet[Int((value >> 18) & 0x3F)])
            output.append(alphabet[Int((value >> 12) & 0x3F)])
            output.append(alphabet[Int((value >> 6) & 0x3F)])
            output.append(alphabet[Int(value & 0x3F)])
            index += 3
        }

        let remaining = bytes.count - index
        if remaining == 1 {
            let value = UInt32(bytes[index]) << 16
            output.append(alphabet[Int((value >> 18) & 0x3F)])
            output.append(alphabet[Int((value >> 12) & 0x3F)])
        } else if remaining == 2 {
            let value = (UInt32(bytes[index]) << 16) | (UInt32(bytes[index + 1]) << 8)
            output.append(alphabet[Int((value >> 18) & 0x3F)])
            output.append(alphabet[Int((value >> 12) & 0x3F)])
            output.append(alphabet[Int((value >> 6) & 0x3F)])
        }

        return String(decoding: output, as: UTF8.self)
    }

    static func decode(_ value: String) throws -> [UInt8] {
        var sextets: [UInt8] = []
        sextets.reserveCapacity(value.utf8.count)
        for byte in value.utf8 {
            guard let sextet = decodeSextet(byte) else {
                throw CloudflareDurableObjectNameCodecError.invalidBase64URL
            }
            sextets.append(sextet)
        }

        var output: [UInt8] = []
        output.reserveCapacity((sextets.count * 3) / 4)

        var index = 0
        while index + 4 <= sextets.count {
            let value = (UInt32(sextets[index]) << 18)
                | (UInt32(sextets[index + 1]) << 12)
                | (UInt32(sextets[index + 2]) << 6)
                | UInt32(sextets[index + 3])
            output.append(UInt8((value >> 16) & 0xFF))
            output.append(UInt8((value >> 8) & 0xFF))
            output.append(UInt8(value & 0xFF))
            index += 4
        }

        let remaining = sextets.count - index
        if remaining == 1 {
            throw CloudflareDurableObjectNameCodecError.invalidBase64URL
        }
        if remaining == 2 {
            let value = (UInt32(sextets[index]) << 18) | (UInt32(sextets[index + 1]) << 12)
            output.append(UInt8((value >> 16) & 0xFF))
        } else if remaining == 3 {
            let value = (UInt32(sextets[index]) << 18)
                | (UInt32(sextets[index + 1]) << 12)
                | (UInt32(sextets[index + 2]) << 6)
            output.append(UInt8((value >> 16) & 0xFF))
            output.append(UInt8((value >> 8) & 0xFF))
        }

        return output
    }

    private static func decodeSextet(_ byte: UInt8) -> UInt8? {
        switch byte {
        case 65...90:
            return byte - 65
        case 97...122:
            return byte - 71
        case 48...57:
            return byte + 4
        case 45:
            return 62
        case 95:
            return 63
        default:
            return nil
        }
    }
}
