export const protocolVersion = 1;

export const operation = Object.freeze({
  readiness: 1,
  read: 2,
  range: 3,
  commit: 4,
});

export const statusCode = Object.freeze({
  ok: 0,
  transactionConflict: 1,
  invalidOperation: 2,
  backendFailure: 3,
  resourceUnavailable: 4,
});

export const keySelectorKind = Object.freeze({
  firstGreaterOrEqual: 1,
  firstGreaterThan: 2,
  lastLessOrEqual: 3,
  lastLessThan: 4,
});

export const mutationType = Object.freeze({
  add: 1,
  setVersionstampedKey: 2,
  setVersionstampedValue: 3,
  bitOr: 4,
  bitAnd: 5,
  bitXor: 6,
  max: 7,
  min: 8,
  compareAndClear: 9,
});
