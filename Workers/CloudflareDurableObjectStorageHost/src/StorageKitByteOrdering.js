export function compareBytes(lhs, rhs) {
  const left = lhs instanceof Uint8Array ? lhs : new Uint8Array(lhs);
  const right = rhs instanceof Uint8Array ? rhs : new Uint8Array(rhs);
  const count = Math.min(left.length, right.length);
  for (let index = 0; index < count; index += 1) {
    if (left[index] !== right[index]) {
      return left[index] < right[index] ? -1 : 1;
    }
  }
  if (left.length === right.length) {
    return 0;
  }
  return left.length < right.length ? -1 : 1;
}

export function equalBytes(lhs, rhs) {
  return compareBytes(lhs, rhs) === 0;
}
