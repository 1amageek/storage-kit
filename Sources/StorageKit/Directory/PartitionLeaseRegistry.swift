import Synchronization

/// Process-local registry of active Partition leases and pending subtree
/// intents for one storage domain.
///
/// A lease registration blocks operations 4 and 5 whenever the affected
/// subtree intersects the leased subtree, and a subtree intent, registered by
/// a move or removal issued through a transaction, blocks lease issuance for
/// an intersecting address until the owning transaction completes.
///
/// Both guards test subtree intersection rather than one direction of
/// ancestry, because a lease covers the whole subtree under its Partition and
/// a move or removal covers the whole subtree under its node. Testing only
/// "the lease lies inside the affected subtree" would admit removing a node
/// beneath an active lease, and testing only "the intent covers the lease
/// address" would admit leasing a Partition that a pending removal is about
/// to delete from beneath.
///
/// Intents are keyed by the identity of the transaction object; every
/// production transaction is a class, so identity is stable for its lifetime.
/// A transaction releases its own intents when it commits or cancels, so the
/// weak reference is a safety net for an abandoned transaction rather than the
/// primary release point.
package final class PartitionLeaseRegistry: Sendable {
    private struct Intent: @unchecked Sendable {
        // Identity-only reference: the registry never dereferences the
        // transaction; it reads `ObjectIdentifier` and liveness under the
        // registry Mutex, so the weak reference is safe to move across
        // isolation domains without the transaction being Sendable.
        weak var transaction: AnyObject?
        let address: StorageAddress
    }

    private struct State {
        var isShutdownRequested = false
        var nextRegistrationID: UInt64 = 1
        var registrations: [UInt64: StorageAddress] = [:]
        var intents: [ObjectIdentifier: [Intent]] = [:]

        mutating func pruneIntents() {
            for (key, entries) in intents {
                let live = entries.filter { $0.transaction != nil }
                if live.isEmpty {
                    intents[key] = nil
                } else if live.count != entries.count {
                    intents[key] = live
                }
            }
        }

        func hasIntent(intersecting subtree: StorageAddress) -> Bool {
            intents.values.contains { entries in
                entries.contains {
                    $0.transaction != nil && $0.address.subtreeIntersects(subtree)
                }
            }
        }

        func hasRegistration(intersecting subtree: StorageAddress) -> Bool {
            registrations.values.contains { $0.subtreeIntersects(subtree) }
        }
    }

    private let state = Mutex(State())

    package init() {}

    /// Rejects every later reservation; existing registrations stay valid
    /// until released so in-flight bindings finish deterministically.
    package func requestShutdown() {
        state.withLock { $0.isShutdownRequested = true }
    }

    /// Reserves a lease registration for `address` before the caller validates
    /// the Partition against the catalog.
    package func reserve(
        _ address: StorageAddress,
        backend: StorageBackend
    ) throws -> LeaseRegistration {
        let id: UInt64 = try state.withLock { state in
            guard !state.isShutdownRequested else {
                throw StorageError(
                    code: .resourceUnavailable,
                    operation: .open,
                    backend: backend,
                    message: "Partition leases cannot be issued after shutdown was requested"
                )
            }
            state.pruneIntents()
            guard !state.hasIntent(intersecting: address) else {
                throw StorageError.staleLease(
                    "A pending move or removal covers the Partition address",
                    operation: .open,
                    backend: backend
                )
            }
            let id = state.nextRegistrationID
            state.nextRegistrationID += 1
            state.registrations[id] = address
            return id
        }
        return LeaseRegistration(id: id, address: address, registry: self)
    }

    /// Reports whether an active registration's subtree intersects `subtree`.
    package func isLeased(intersecting subtree: StorageAddress) -> Bool {
        state.withLock { $0.hasRegistration(intersecting: subtree) }
    }

    /// Registers a move or removal intent over `subtree` for `transaction`.
    package func registerIntent(
        covering subtree: StorageAddress,
        transaction: any TransactionReadAccess,
        operation: StorageOperation,
        backend: StorageBackend
    ) throws {
        let owner = transaction as AnyObject
        let ownerID = ObjectIdentifier(owner)
        let intent = Intent(transaction: owner, address: subtree)
        try state.withLock { state in
            guard !state.hasRegistration(intersecting: subtree) else {
                throw StorageError.directoryLeased(
                    "An active Partition lease covers the affected subtree",
                    operation: operation,
                    backend: backend
                )
            }
            state.pruneIntents()
            state.intents[ownerID, default: []].append(intent)
        }
    }

    /// Releases every intent registered by `transaction`.
    ///
    /// Releasing is idempotent, so every terminal path of a transaction can
    /// call it without coordinating with the others. A transaction that
    /// registered nothing costs one uncontended lock and no scan, because the
    /// sweep for abandoned transactions only pays off after a removal.
    package func releaseIntents(for transaction: AnyObject) {
        let ownerID = ObjectIdentifier(transaction)
        state.withLock { state in
            guard state.intents.removeValue(forKey: ownerID) != nil else {
                return
            }
            state.pruneIntents()
        }
    }

    func release(registrationID: UInt64) {
        state.withLock { $0.registrations[registrationID] = nil }
    }
}

extension TransactionReadAccess where Self: AnyObject {
    /// Releases every move or removal intent this transaction registered.
    ///
    /// A transaction calls this once it reaches a terminal outcome. An intent
    /// exists to stop a lease from being issued over a mutation that is still
    /// pending in an uncommitted transaction; once the transaction has
    /// committed, cancelled, or failed, nothing is pending and the invariant
    /// that remains is L-3, which every issuance already enforces by
    /// re-resolving the address in the caller's own transaction.
    ///
    /// The `AnyObject` constraint is the contract: intents are keyed by object
    /// identity, so a value-type transaction would register under one box and
    /// release under another, silently leaking the intent.
    package func releaseSubtreeIntents() {
        transactionDomain.leases.releaseIntents(for: self)
    }
}
