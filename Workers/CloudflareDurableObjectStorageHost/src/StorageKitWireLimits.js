export const storageKitWireLimits = Object.freeze({
  maxFrameBytes: 16 * 1024 * 1024,
  maxRangeResponseBytes: 8 * 1024 * 1024,
  maxKeyBytes: 1_024,
  maxVersionstampedKeyOperandBytes: 1_028,
  maxBoundaryBytes: 1_025,
  maxValueBytes: 1_048_576,
  maxVersionstampedValueOperandBytes: 1_048_580,
  maxScopeComponentBytes: 512,
  maxCanonicalScopeNameBytes: 512,
  maxErrorMessageBytes: 4_096,
  // Database-level mutations expand into physical item and index writes.
  // maxFrameBytes remains the aggregate byte admission limit.
  maxMutationsPerCommit: 10_000,
  maxConflictRangesPerCommit: 10_000,
  maxRangeLimit: 1_000,
  maxSplitPoints: 10_000,
  maxSelectorResolutionSteps: 10_000n,
  maxConflictEntries: 65_536,
  maxConflictBytes: 32 * 1024 * 1024,
});
