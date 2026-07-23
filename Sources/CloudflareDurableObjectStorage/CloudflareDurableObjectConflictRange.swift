/// Half-open key range used for Durable Object read conflict tracking.
public struct CloudflareDurableObjectConflictRange: Sendable, Hashable {
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

    func detached() -> CloudflareDurableObjectConflictRange {
        CloudflareDurableObjectConflictRange(
            begin: begin.map {
                CloudflareDurableObjectBytes($0.rawValue.detached())
            },
            end: end.map {
                CloudflareDurableObjectBytes($0.rawValue.detached())
            }
        )
    }
}
