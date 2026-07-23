enum CloudflareDurableObjectBase64URL {
    private static let alphabet = Array(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".utf8
    )

    static func encode<ByteCollection: Collection>(
        _ bytes: ByteCollection
    ) -> String where ByteCollection.Element == UInt8 {
        let fullGroupCount = bytes.count / 3
        let remainder = bytes.count % 3
        let encodedCount = fullGroupCount * 4 + (remainder == 0 ? 0 : remainder + 1)

        return String(unsafeUninitializedCapacity: encodedCount) { output in
            var iterator = bytes.makeIterator()
            var outputOffset = 0

            for _ in 0..<fullGroupCount {
                let first = iterator.next()!
                let second = iterator.next()!
                let third = iterator.next()!
                let value = (UInt32(first) << 16)
                    | (UInt32(second) << 8)
                    | UInt32(third)
                output[outputOffset] = alphabet[Int((value >> 18) & 0x3F)]
                output[outputOffset + 1] = alphabet[Int((value >> 12) & 0x3F)]
                output[outputOffset + 2] = alphabet[Int((value >> 6) & 0x3F)]
                output[outputOffset + 3] = alphabet[Int(value & 0x3F)]
                outputOffset += 4
            }

            if remainder == 1 {
                let value = UInt32(iterator.next()!) << 16
                output[outputOffset] = alphabet[Int((value >> 18) & 0x3F)]
                output[outputOffset + 1] = alphabet[Int((value >> 12) & 0x3F)]
            } else if remainder == 2 {
                let value = (UInt32(iterator.next()!) << 16)
                    | (UInt32(iterator.next()!) << 8)
                output[outputOffset] = alphabet[Int((value >> 18) & 0x3F)]
                output[outputOffset + 1] = alphabet[Int((value >> 12) & 0x3F)]
                output[outputOffset + 2] = alphabet[Int((value >> 6) & 0x3F)]
            }
            return encodedCount
        }
    }

}
