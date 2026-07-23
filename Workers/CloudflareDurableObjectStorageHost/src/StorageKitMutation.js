import { equalBytes } from "./StorageKitByteOrdering.js";
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
    return param;
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
  return compareAdjustedLittleEndian(existing, param) >= 0
    ? adjustedWinner(existing, param.length)
    : param;
}

function min(existing, param) {
  if (existing === null) {
    return param;
  }
  return compareAdjustedLittleEndian(existing, param) <= 0
    ? adjustedWinner(existing, param.length)
    : param;
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

function adjustedWinner(value, length) {
  if (value.length === length) {
    return value;
  }
  if (value.length > length) {
    return value.subarray(0, length);
  }
  const result = new Uint8Array(length);
  result.set(value);
  return result;
}

function compareAdjustedLittleEndian(existing, param) {
  for (let index = param.length - 1; index >= 0; index -= 1) {
    const currentByte = index < existing.length ? existing[index] : 0;
    if (currentByte !== param[index]) {
      return currentByte < param[index] ? -1 : 1;
    }
  }
  return 0;
}
