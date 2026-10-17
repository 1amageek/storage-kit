import Synchronization

/// An opaque marker belonging to one transaction attempt.
public struct TransactionCheckpoint: Sendable, Hashable {
    let attempt: TransactionCheckpointIdentity
    let identifier: UInt64

    public static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.attempt === rhs.attempt && lhs.identifier == rhs.identifier
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(attempt))
        hasher.combine(identifier)
    }
}

final class TransactionCheckpointIdentity: Sendable {}
