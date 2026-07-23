import { mutationType } from "./StorageKitWireConstants.js";
import { StorageKitWireError } from "./StorageKitWireError.js";
import { storageKitWireLimits } from "./StorageKitWireLimits.js";

export function materializeMutation(mutation, committedVersion) {
  if (mutation.tag !== 4) {
    return mutation;
  }
  switch (mutation.mutationType) {
    case mutationType.setVersionstampedKey:
      return {
        tag: 1,
        key: materializeOperand(
          mutation.key,
          committedVersion,
          storageKitWireLimits.maxKeyBytes,
          "Versionstamped key"
        ),
        value: mutation.param,
      };
    case mutationType.setVersionstampedValue:
      return {
        tag: 1,
        key: mutation.key,
        value: materializeOperand(
          mutation.param,
          committedVersion,
          storageKitWireLimits.maxValueBytes,
          "Versionstamped value"
        ),
      };
    default:
      return mutation;
  }
}

export function versionstampForCommitVersion(committedVersion) {
  const version = BigInt(committedVersion);
  if (version < 0n || version > 0x7fff_ffff_ffff_ffffn) {
    throw StorageKitWireError.invalidOperation(
      "Commit version cannot form a versionstamp"
    );
  }
  const result = new Uint8Array(10);
  let remaining = version;
  for (let index = 7; index >= 0; index -= 1) {
    result[index] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
  return result;
}

function materializeOperand(
  operand,
  committedVersion,
  maximumResultBytes,
  field
) {
  const bytes = byteView(operand);
  guardMinimumLength(bytes, field);
  const payloadCount = bytes.byteLength - 4;
  const offset = bytes[payloadCount]
    | (bytes[payloadCount + 1] << 8)
    | (bytes[payloadCount + 2] << 16)
    | (bytes[payloadCount + 3] << 24);
  const unsignedOffset = offset >>> 0;
  if (unsignedOffset > payloadCount - 10) {
    throw StorageKitWireError.invalidOperation(
      `${field} offset does not identify ten payload bytes`
    );
  }
  if (payloadCount > maximumResultBytes) {
    throw StorageKitWireError.limitExceeded(field, maximumResultBytes);
  }
  const result = bytes.slice(0, payloadCount);
  result.set(
    versionstampForCommitVersion(committedVersion),
    unsignedOffset
  );
  return result;
}

function guardMinimumLength(bytes, field) {
  if (bytes.byteLength < 14) {
    throw StorageKitWireError.invalidOperation(
      `${field} operand must contain a ten-byte target and four-byte offset`
    );
  }
}

function byteView(value) {
  if (value instanceof Uint8Array) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw StorageKitWireError.invalidOperation(
    "Versionstamp operand must be binary"
  );
}
