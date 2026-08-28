import DatabaseTypes

/// A resolved Directory: its engine domain, address, and opaque root Subspace.
///
/// The generation of a Directory is `root.prefix`; a Directory that is removed
/// and recreated at the same address receives a different prefix.
public struct Directory: Sendable, Hashable {
    public let domain: StorageTransactionDomain
    public let address: StorageAddress
    public let root: Subspace

    public init(
        domain: StorageTransactionDomain,
        address: StorageAddress,
        root: Subspace
    ) {
        self.domain = domain
        self.address = address
        self.root = root
    }

    /// Opaque generation identity of this resolution.
    public var generation: ByteString { root.prefix }

    public static func == (lhs: Directory, rhs: Directory) -> Bool {
        lhs.domain === rhs.domain
            && lhs.address == rhs.address
            && lhs.root == rhs.root
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(domain))
        hasher.combine(address)
        hasher.combine(root)
    }
}
