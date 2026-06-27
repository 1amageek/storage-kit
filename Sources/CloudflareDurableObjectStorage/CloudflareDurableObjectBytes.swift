import StorageKit

/// Codable diagnostic representation of a byte array.
public struct CloudflareDurableObjectBytes: Sendable, Hashable, Codable {
    public let rawValue: Bytes

    public init(_ rawValue: Bytes) {
        self.rawValue = rawValue
    }

    public init(base64url: String) throws {
        self.rawValue = try CloudflareDurableObjectBase64URL.decode(base64url)
    }

    public var base64url: String {
        CloudflareDurableObjectBase64URL.encode(rawValue)
    }

    private enum CodingKeys: String, CodingKey {
        case base64url
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let encoded = try container.decode(String.self, forKey: .base64url)
        do {
            try self.init(base64url: encoded)
        } catch {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "Invalid base64url byte field",
                    underlyingError: error
                )
            )
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(base64url, forKey: .base64url)
    }
}
