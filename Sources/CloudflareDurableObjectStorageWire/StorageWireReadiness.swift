
public struct StorageWireReadinessRequest: Sendable, Hashable {
    public let scope: StorageWireScope

    public init(scope: StorageWireScope) {
        self.scope = scope
    }

    func encode(into writer: inout StorageWireWriter) throws(StorageWireProtocolError) {
        try scope.encode(into: &writer)
    }

    init(from reader: inout StorageWireReader) throws(StorageWireProtocolError) {
        self.scope = try StorageWireScope(from: &reader)
    }
}
