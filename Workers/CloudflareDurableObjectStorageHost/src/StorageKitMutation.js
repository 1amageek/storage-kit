import { compareBytes, equalBytes } from "./StorageKitByteOrdering.js";
import { mutationType } from "./StorageKitWireConstants.js";
import { StorageKitWireError } from "./StorageKitWireError.js";

export function applyMutation(existing, param, type) {
  switch (type) {
    case mutationType.add:
      return { kind: "set", value: add(existing ?? new Uint8Array(), param) };
    case mutationType.bitAnd:
      return { kind: "set", value: bitAnd(existing, param) };
    case mutationType.bitOr:
      return { kind: "set", value: bitOr(existing ?? new Uint8Array(), param) };
    case mutationType.bitXor:
      return { kind: "set", value: bitXor(existing ?? new Uint8Array(), param) };
    case mutationType.max:
      return { kind: "set", value: max(existing ?? new Uint8Array(), param) };
    case mutationType.min:
      return { kind: "set", value: min(existing, param) };
    case mutationType.compareAndClear:
      return existing !== null && equalBytes(existing, param) ? { kind: "clear" } : { kind: "unchanged" };
    case mutationType.setVersionstampedKey:
    case mutationType.setVersionstampedValue:
      throw StorageKitWireError.invalidOperation("Versionstamp mutations require commit version support");
    default:
      throw StorageKitWireError.unknownMutationType(type);
  }
}

function add(existing, param) {
  const result = adjusted(existing, param.length);
  let carry = 0;
  for (let index = 0; index < param.length; index += 1) {
    const sum = result[index] + param[index] + carry;
    result[index] = sum & 0xff;
    carry = sum >> 8;
  }
  return result;
}

function bitAnd(existing, param) {
  if (existing === null) {
    return new Uint8Array(param);
  }
  const result = adjusted(existing, param.length);
  for (let index = 0; index < param.length; index += 1) {
    result[index] &= param[index];
  }
  return result;
}

function bitOr(existing, param) {
  const result = adjusted(existing, param.length);
  for (let index = 0; index < param.length; index += 1) {
    result[index] |= param[index];
  }
  return result;
}

function bitXor(existing, param) {
  const result = adjusted(existing, param.length);
  for (let index = 0; index < param.length; index += 1) {
    result[index] ^= param[index];
  }
  return result;
}

function max(existing, param) {
  const current = adjusted(existing, param.length);
  return compareLittleEndian(current, param) >= 0 ? current : new Uint8Array(param);
}

function min(existing, param) {
  if (existing === null) {
    return new Uint8Array(param);
  }
  const current = adjusted(existing, param.length);
  return compareLittleEndian(current, param) <= 0 ? current : new Uint8Array(param);
}

function adjusted(value, length) {
  if (value.length === length) {
    return new Uint8Array(value);
  }
  if (value.length > length) {
    return value.slice(0, length);
  }
  const result = new Uint8Array(length);
  result.set(value);
  return result;
}

function compareLittleEndian(lhs, rhs) {
  return compareBytes(reverse(lhs), reverse(rhs));
}

function reverse(value) {
  const result = new Uint8Array(value.length);
  for (let index = 0; index < value.length; index += 1) {
    result[index] = value[value.length - index - 1];
  }
  return result;
}
