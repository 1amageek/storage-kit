export const storageKitWireLimits = Object.freeze({
  maxFrameBytes: 16 * 1024 * 1024,
  maxRangeResponseBytes: 8 * 1024 * 1024,
  // The host is backed by SQLite-backed Durable Object storage. Cloudflare
  // documents a 2,000,000-byte combined key and value limit. Either component
  // may consume that budget alone; stored pairs are validated separately.
  maxKeyBytes: 2_000_000,
  maxVersionstampedKeyOperandBytes: 2_000_004,
  maxBoundaryBytes: 2_000_001,
  maxValueBytes: 2_000_000,
  maxStoredKeyValueBytes: 2_000_000,
  maxVersionstampedValueOperandBytes: 2_000_004,
  maxPartitionIdentityComponentBytes: 512,
  maxCanonicalPartitionIdentityNameBytes: 512,
  maxErrorMessageBytes: 4_096,
  // Database-level mutations expand into physical item and index writes.
  // maxFrameBytes remains the aggregate byte admission limit.
  maxMutationsPerCommit: 10_000,
  maxConflictRangesPerCommit: 10_000,
  maxRangeLimit: 1_000,
  maxSplitPoints: 10_000,
  maxSelectorResolutionSteps: 10_000n,
  // Hard bounds on retained write-conflict history, enforced by
  // StorageKitSQLiteStore.pruneConflictRanges. maxConflictEntries equals
  // conflictVersionWindow (4096) * maximumPersistedConflictRangesPerCommit
  // (256); change them together.
  maxConflictEntries: 1_048_576,
  maxConflictBytes: 256 * 1024 * 1024,
});
