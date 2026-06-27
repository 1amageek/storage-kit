/// Half-open key range used for Durable Object read conflict tracking.
public struct CloudflareDurableObjectConflictRange: Sendable, Hashable, Codable {
    public let begin: CloudflareDurableObjectBytes?
    public let end: CloudflareDurableObjectBytes?

    public init(
        begin: CloudflareDurableObjectBytes?,
        end: CloudflareDurableObjectBytes?
    ) {
        self.begin = begin
        self.end = end
    }

    public static func singleKey(_ key: CloudflareDurableObjectBytes) -> CloudflareDurableObjectConflictRange {
        CloudflareDurableObjectConflictRange(
            begin: key,
            end: CloudflareDurableObjectBytes(key.rawValue + [0x00])
        )
    }
}
