import Synchronization

/// Process-local registry of active Partition leases and pending subtree
/// intents for one storage domain.
///
/// A lease registration blocks operations 7 and 8 on any ancestor-or-self
/// address for as long as it is active. A subtree intent, registered by a
/// move or removal issued through a transaction, blocks new lease issuance
/// under that subtree until the owning transaction completes or is released.
/// Intents are keyed by the identity of the transaction object; every
/// production transaction is a class, so identity is stable for its lifetime.
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

        func hasIntent(covering address: StorageAddress) -> Bool {
            intents.values.contains { entries in
                entries.contains { $0.transaction != nil && $0.address.isAncestorOrSelf(of: address) }
            }
        }

        func hasRegistration(within subtree: StorageAddress) -> Bool {
            registrations.values.contains { subtree.isAncestorOrSelf(of: $0) }
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
            guard !state.hasIntent(covering: address) else {
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

    package func isLeased(within subtree: StorageAddress) -> Bool {
        state.withLock { $0.hasRegistration(within: subtree) }
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
            guard !state.hasRegistration(within: subtree) else {
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
    package func releaseIntents(for transaction: AnyObject) {
        let ownerID = ObjectIdentifier(transaction)
        state.withLock { state in
            state.intents[ownerID] = nil
            state.pruneIntents()
        }
    }

    func release(registrationID: UInt64) {
        state.withLock { $0.registrations[registrationID] = nil }
    }
}
