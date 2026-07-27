import DatabaseTypes

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
        let end = ByteString.copying(
            count: key.rawValue.count + 1
        ) { destination in
            key.rawValue.withUnsafeBytes { source in
                destination.copyMemory(from: source)
            }
            destination[key.rawValue.count] = 0
        }
        return CloudflareDurableObjectConflictRange(
            begin: key,
            end: CloudflareDurableObjectBytes(end)
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
