import { StorageKitWireReader } from "./StorageKitWireReader.js";
import { StorageKitWireWriter } from "./StorageKitWireWriter.js";
import { compareBytes } from "./StorageKitByteOrdering.js";
import { nameForPartitionIdentity, validatePartitionIdentity } from "./StorageKitPartitionIdentity.js";
import {
  mutationType,
  operation,
  protocolVersion,
  statusCode,
} from "./StorageKitWireConstants.js";
import { StorageKitWireError } from "./StorageKitWireError.js";
import { storageKitWireLimits } from "./StorageKitWireLimits.js";

const utf8Encoder = new TextEncoder();

export class StorageKitWire {
  static decodeRoutingPartitionIdentity(bytes) {
    const reader = new StorageKitWireReader(bytes);
    const version = reader.readUInt8();
    if (version !== protocolVersion) {
      throw StorageKitWireError.unsupportedProtocolVersion(version);
    }
    const op = reader.readUInt8();
    if (!Object.values(operation).includes(op)) {
      throw StorageKitWireError.unknownOperation(op);
    }
    return {
      operation: op,
      partitionIdentity: readPartitionIdentity(reader),
    };
  }

  static decodeRequest(bytes) {
    const reader = new StorageKitWireReader(bytes);
    const version = reader.readUInt8();
    if (version !== protocolVersion) {
      throw StorageKitWireError.unsupportedProtocolVersion(version);
    }
    const op = reader.readUInt8();
    let request;
    switch (op) {
      case operation.readiness:
        request = { operation: op, partitionIdentity: readPartitionIdentity(reader) };
        break;
      case operation.read:
        request = {
          operation: op,
          partitionIdentity: readPartitionIdentity(reader),
          key: readKey(reader),
          snapshot: reader.readBool(),
          expectedReadVersion: readOptionalInt64(reader),
        };
        break;
      case operation.range:
        request = {
          operation: op,
          partitionIdentity: readPartitionIdentity(reader),
          begin: readRangeBoundary(reader),
          end: readRangeBoundary(reader),
          limit: readRangeLimit(reader),
          reverse: reader.readBool(),
          snapshot: reader.readBool(),
          expectedReadVersion: readOptionalInt64(reader),
          cursorKey: readOptionalKey(reader),
        };
        break;
      case operation.commit:
        request = {
          operation: op,
          partitionIdentity: readPartitionIdentity(reader),
          observedReadVersion: readOptionalInt64(reader),
          mutations: readMutations(reader),
          readConflictRanges: readKeyRanges(reader),
          writeConflictRanges: readKeyRanges(reader),
        };
        break;
      case operation.rangeSize:
        request = {
          operation: op,
          partitionIdentity: readPartitionIdentity(reader),
          begin: readBoundary(reader),
          end: readBoundary(reader),
          expectedReadVersion: readOptionalInt64(reader),
        };
        validateOrderedRange(request.begin, request.end);
        break;
      case operation.rangeSplitPoints:
        request = {
          operation: op,
          partitionIdentity: readPartitionIdentity(reader),
          begin: readBoundary(reader),
          end: readBoundary(reader),
          chunkSize: readPositiveInt64(reader, "Split point chunk size"),
          expectedReadVersion: readOptionalInt64(reader),
        };
        validateOrderedRange(request.begin, request.end);
        break;
      default:
        throw StorageKitWireError.unknownOperation(op);
    }
    reader.ensureFullyRead();
    return request;
  }

  static encodeRequest(request) {
    return StorageKitWireWriter.encodeExact((writer) => {
      writer.writeUInt8(protocolVersion);
      writer.writeUInt8(request.operation);
      writeRequestPayload(writer, request);
    });
  }

  static decodeResponse(bytes) {
    const reader = new StorageKitWireReader(bytes);
    const version = reader.readUInt8();
    if (version !== protocolVersion) {
      throw StorageKitWireError.unsupportedProtocolVersion(version);
    }
    const status = reader.readUInt8();
    if (!Object.values(statusCode).includes(status)) {
      throw StorageKitWireError.unknownStatus(status);
    }
    if (status !== statusCode.ok) {
      const response = {
        status,
        message: reader.readString(
          storageKitWireLimits.maxErrorMessageBytes,
          "Error message bytes"
        ),
      };
      reader.ensureFullyRead();
      return response;
    }
    const op = reader.readUInt8();
    let response;
    switch (op) {
      case operation.readiness:
        response = {
          status,
          operation: op,
          schemaVersion: reader.readUInt32(),
          commitVersion: readVersion(reader),
          metadataInitialized: reader.readBool(),
        };
        break;
      case operation.read:
        response = {
          status,
          operation: op,
          value: readOptionalBytes(reader),
          currentCommitVersion: readVersion(reader),
        };
        break;
      case operation.range: {
        const rows = readRows(reader);
        const hasMore = reader.readBool();
        validateRangeContinuation(rows, hasMore);
        response = {
          status,
          operation: op,
          rows,
          hasMore,
          currentCommitVersion: readVersion(reader),
          readConflictRanges: readKeyRanges(reader),
        };
        break;
      }
      case operation.commit:
        response = {
          status,
          operation: op,
          committedVersion: readVersion(reader),
        };
        break;
      case operation.rangeSize:
        response = {
          status,
          operation: op,
          byteCount: readNonNegativeInt64(reader, "Range byte count"),
          currentCommitVersion: readVersion(reader),
        };
        break;
      case operation.rangeSplitPoints:
        response = {
          status,
          operation: op,
          splitPoints: readSplitPoints(reader),
          currentCommitVersion: readVersion(reader),
        };
        break;
      default:
        throw StorageKitWireError.unknownOperation(op);
    }
    reader.ensureFullyRead();
    return response;
  }

  static encodeResponse(response) {
    return StorageKitWireWriter.encodeExact((writer) => {
      writer.writeUInt8(protocolVersion);
      const status = response.status ?? statusCode.ok;
      if (!Object.values(statusCode).includes(status)) {
        throw StorageKitWireError.unknownStatus(status);
      }
      writer.writeUInt8(status);
      if (status !== statusCode.ok) {
        writer.writeString(boundedErrorMessage(response.message));
        return;
      }
      writer.writeUInt8(response.operation);
      writeResponsePayload(writer, response);
    });
  }

  static encodeFailure(status, message) {
    return this.encodeResponse({ status, message });
  }
}

function writeRequestPayload(writer, request) {
  switch (request.operation) {
    case operation.readiness:
      writePartitionIdentity(writer, request.partitionIdentity);
      break;
    case operation.read:
      writePartitionIdentity(writer, request.partitionIdentity);
      writeKey(writer, request.key);
      writer.writeBool(request.snapshot);
      writeOptionalInt64(writer, request.expectedReadVersion);
      break;
    case operation.range:
      writePartitionIdentity(writer, request.partitionIdentity);
      writeRangeBoundary(writer, request.begin);
      writeRangeBoundary(writer, request.end);
      writer.writeInt32(validateRangeLimit(request.limit));
      writer.writeBool(request.reverse);
      writer.writeBool(request.snapshot);
      writeOptionalInt64(writer, request.expectedReadVersion);
      writeOptionalKey(writer, request.cursorKey);
      break;
    case operation.commit:
      writePartitionIdentity(writer, request.partitionIdentity);
      writeOptionalInt64(writer, request.observedReadVersion);
      validateCollectionCount(
        request.mutations.length,
        storageKitWireLimits.maxMutationsPerCommit,
        "Mutation count"
      );
      writer.writeUInt32(request.mutations.length);
      for (const mutation of request.mutations) {
        writeMutation(writer, mutation);
      }
      validateCollectionCount(
        request.readConflictRanges?.length ?? 0,
        storageKitWireLimits.maxConflictRangesPerCommit,
        "Read conflict range count"
      );
      writer.writeUInt32(request.readConflictRanges?.length ?? 0);
      for (const range of request.readConflictRanges ?? []) {
        writeKeyRange(writer, range);
      }
      validateCollectionCount(
        request.writeConflictRanges?.length ?? 0,
        storageKitWireLimits.maxConflictRangesPerCommit,
        "Write conflict range count"
      );
      writer.writeUInt32(request.writeConflictRanges?.length ?? 0);
      for (const range of request.writeConflictRanges ?? []) {
        writeKeyRange(writer, range);
      }
      break;
    case operation.rangeSize:
      writePartitionIdentity(writer, request.partitionIdentity);
      validateOrderedRange(request.begin, request.end);
      writeBoundary(writer, request.begin);
      writeBoundary(writer, request.end);
      writeOptionalInt64(writer, request.expectedReadVersion);
      break;
    case operation.rangeSplitPoints:
      writePartitionIdentity(writer, request.partitionIdentity);
      validateOrderedRange(request.begin, request.end);
      writeBoundary(writer, request.begin);
      writeBoundary(writer, request.end);
      writer.writeInt64(validatedPositiveInt64(
        request.chunkSize,
        "Split point chunk size"
      ));
      writeOptionalInt64(writer, request.expectedReadVersion);
      break;
    default:
      throw StorageKitWireError.unknownOperation(request.operation);
  }
}

function writeResponsePayload(writer, response) {
  switch (response.operation) {
    case operation.readiness:
      writer.writeUInt32(response.schemaVersion);
      writeVersion(writer, response.commitVersion);
      writer.writeBool(response.metadataInitialized);
      break;
    case operation.read:
      writeOptionalValue(writer, response.value);
      writeVersion(writer, response.currentCommitVersion);
      break;
    case operation.range:
      validateCollectionCount(
        response.rows.length,
        storageKitWireLimits.maxRangeLimit,
        "Range row count"
      );
      writer.writeUInt32(response.rows.length);
      for (const row of response.rows) {
        writeKey(writer, row.key);
        writeValue(writer, row.value);
      }
      validateRangeContinuation(response.rows, response.hasMore);
      writer.writeBool(response.hasMore);
      writeVersion(writer, response.currentCommitVersion);
      validateCollectionCount(
        response.readConflictRanges?.length ?? 0,
        storageKitWireLimits.maxConflictRangesPerCommit,
        "Read conflict range count"
      );
      writer.writeUInt32(response.readConflictRanges?.length ?? 0);
      for (const range of response.readConflictRanges ?? []) {
        writeKeyRange(writer, range);
      }
      break;
    case operation.commit:
      writeVersion(writer, response.committedVersion);
      break;
    case operation.rangeSize:
      writer.writeInt64(validatedNonNegativeInt64(
        response.byteCount,
        "Range byte count"
      ));
      writeVersion(writer, response.currentCommitVersion);
      break;
    case operation.rangeSplitPoints:
      writeSplitPoints(writer, response.splitPoints);
      writeVersion(writer, response.currentCommitVersion);
      break;
    default:
      throw StorageKitWireError.unknownOperation(response.operation);
  }
}

function readPartitionIdentity(reader) {
  const partitionIdentity = validatePartitionIdentity({
    databaseID: reader.readString(
      storageKitWireLimits.maxPartitionIdentityComponentBytes,
      "Database ID bytes"
    ),
    tenantID: readOptionalString(
      reader,
      storageKitWireLimits.maxPartitionIdentityComponentBytes,
      "Tenant ID bytes"
    ),
    workspaceID: readOptionalString(
      reader,
      storageKitWireLimits.maxPartitionIdentityComponentBytes,
      "Workspace ID bytes"
    ),
  });
  nameForPartitionIdentity(partitionIdentity);
  return partitionIdentity;
}

function writePartitionIdentity(writer, partitionIdentity) {
  const validated = validatePartitionIdentity(partitionIdentity);
  nameForPartitionIdentity(validated);
  writer.writeString(validated.databaseID);
  writeOptionalString(writer, validated.tenantID);
  writeOptionalString(writer, validated.workspaceID);
}

function readOptionalString(
  reader,
  maximum = storageKitWireLimits.maxPartitionIdentityComponentBytes,
  field = "Optional string bytes"
) {
  return reader.readBool() ? reader.readString(maximum, field) : null;
}

function writeOptionalString(
  writer,
  value,
  maximum = storageKitWireLimits.maxPartitionIdentityComponentBytes,
  field = "Optional string bytes"
) {
  writer.writeBool(value !== null && value !== undefined);
  if (value !== null && value !== undefined) {
    const byteLength = utf8Encoder.encode(value).byteLength;
    if (byteLength > maximum) {
      throw StorageKitWireError.limitExceeded(field, maximum);
    }
    writer.writeString(value);
  }
}

function readOptionalInt64(reader) {
  return reader.readBool() ? readVersion(reader) : null;
}

function writeOptionalInt64(writer, value) {
  writer.writeBool(value !== null && value !== undefined);
  if (value !== null && value !== undefined) {
    writeVersion(writer, value);
  }
}

function readVersion(reader) {
  return validatedNonNegativeInt64(reader.readInt64(), "Version");
}

function writeVersion(writer, value) {
  writer.writeInt64(validatedNonNegativeInt64(value, "Version"));
}

function readNonNegativeInt64(reader, field) {
  return validatedNonNegativeInt64(reader.readInt64(), field);
}

function readPositiveInt64(reader, field) {
  return validatedPositiveInt64(reader.readInt64(), field);
}

function validatedNonNegativeInt64(value, field) {
  let result;
  try {
    result = BigInt(value);
  } catch {
    throw StorageKitWireError.invalidOperation(
      `${field} must be a non-negative Int64`
    );
  }
  if (result < 0n || result > 0x7fff_ffff_ffff_ffffn) {
    throw StorageKitWireError.invalidOperation(
      `${field} must be a non-negative Int64`
    );
  }
  return result;
}

function validatedPositiveInt64(value, field) {
  const result = validatedNonNegativeInt64(value, field);
  if (result === 0n) {
    throw StorageKitWireError.invalidOperation(
      `${field} must be positive`
    );
  }
  return result;
}

function readOptionalKey(reader) {
  return reader.readBool() ? readKey(reader) : null;
}

function writeOptionalKey(writer, value) {
  writer.writeBool(value !== null && value !== undefined);
  if (value !== null && value !== undefined) {
    writeKey(writer, value);
  }
}

function readOptionalBytes(reader) {
  return reader.readBool() ? readValue(reader) : null;
}

function readKeySelector(reader) {
  const selector = {
    key: readKey(reader),
    orEqual: reader.readBool(),
    offset: reader.readInt64(),
  };
  if (selector.offset < -storageKitWireLimits.maxSelectorResolutionSteps
      || selector.offset > storageKitWireLimits.maxSelectorResolutionSteps) {
    throw StorageKitWireError.limitExceeded(
      "Key selector offset",
      storageKitWireLimits.maxSelectorResolutionSteps
    );
  }
  return selector;
}

function writeKeySelector(writer, selector) {
  const offset = BigInt(selector.offset);
  if (offset < -storageKitWireLimits.maxSelectorResolutionSteps
      || offset > storageKitWireLimits.maxSelectorResolutionSteps) {
    throw StorageKitWireError.limitExceeded(
      "Key selector offset",
      storageKitWireLimits.maxSelectorResolutionSteps
    );
  }
  writeKey(writer, selector.key);
  writer.writeBool(selector.orEqual);
  writer.writeInt64(offset);
}

function readRangeBoundary(reader) {
  const tag = reader.readUInt8();
  switch (tag) {
    case 0:
      return null;
    case 1:
      return readKeySelector(reader);
    default:
      throw StorageKitWireError.unknownRangeBoundary(tag);
  }
}

function writeRangeBoundary(writer, boundary) {
  if (boundary === null || boundary === undefined) {
    writer.writeUInt8(0);
    return;
  }
  writer.writeUInt8(1);
  writeKeySelector(writer, boundary);
}

function readKeyRange(reader) {
  return {
    begin: readOptionalBoundary(reader),
    end: readOptionalBoundary(reader),
  };
}

function writeKeyRange(writer, range) {
  writeOptionalBoundary(writer, range.begin ?? null);
  writeOptionalBoundary(writer, range.end ?? null);
}

function readKeyRanges(
  reader,
  maximum = storageKitWireLimits.maxConflictRangesPerCommit
) {
  const count = reader.readUInt32();
  validateCollectionCount(count, maximum, "Conflict range count");
  const ranges = [];
  for (let index = 0; index < count; index += 1) {
    ranges.push(readKeyRange(reader));
  }
  return ranges;
}

function readRows(reader) {
  const count = reader.readUInt32();
  validateCollectionCount(count, storageKitWireLimits.maxRangeLimit, "Range row count");
  const rows = [];
  for (let index = 0; index < count; index += 1) {
    const key = readKey(reader);
    const value = readValue(reader);
    validateStoredPairByteLengths(key.byteLength, value.byteLength);
    rows.push({ key, value });
  }
  return rows;
}

function validateRangeContinuation(rows, hasMore) {
  if (typeof hasMore !== "boolean") {
    throw StorageKitWireError.invalidRangeContinuation();
  }
  if (hasMore && rows.length === 0) {
    throw StorageKitWireError.invalidRangeContinuation();
  }
}

function readSplitPoints(reader) {
  const count = reader.readUInt32();
  validateCollectionCount(
    count,
    storageKitWireLimits.maxSplitPoints,
    "Split point count"
  );
  const points = [];
  for (let index = 0; index < count; index += 1) {
    points.push(readBoundary(reader));
  }
  validateSplitPoints(points);
  return points;
}

function writeSplitPoints(writer, points) {
  validateCollectionCount(
    points.length,
    storageKitWireLimits.maxSplitPoints,
    "Split point count"
  );
  validateSplitPoints(points);
  writer.writeUInt32(points.length);
  for (const point of points) {
    writeBoundary(writer, point);
  }
}

function validateSplitPoints(points) {
  if (points.length === 0) {
    throw StorageKitWireError.invalidOperation(
      "Split points must include the requested range boundaries"
    );
  }
  for (let index = 1; index < points.length; index += 1) {
    if (compareBytes(points[index - 1], points[index]) >= 0) {
      throw StorageKitWireError.invalidOperation(
        "Split points must be strictly ordered"
      );
    }
  }
}

function validateOrderedRange(begin, end) {
  validateByteLength(
    begin,
    storageKitWireLimits.maxBoundaryBytes,
    "Begin boundary bytes"
  );
  validateByteLength(
    end,
    storageKitWireLimits.maxBoundaryBytes,
    "End boundary bytes"
  );
  if (compareBytes(begin, end) > 0) {
    throw StorageKitWireError.invalidOperation(
      "Range boundaries are not ordered"
    );
  }
}

function readMutations(reader) {
  const count = reader.readUInt32();
  validateCollectionCount(
    count,
    storageKitWireLimits.maxMutationsPerCommit,
    "Mutation count"
  );
  const mutations = [];
  for (let index = 0; index < count; index += 1) {
    mutations.push(readMutation(reader));
  }
  return mutations;
}

function readMutation(reader) {
  const tag = reader.readUInt8();
  switch (tag) {
    case 1: {
      const key = readKey(reader);
      const value = readValue(reader);
      validateStoredPairByteLengths(key.byteLength, value.byteLength);
      return { tag, key, value };
    }
    case 2:
      return { tag, key: readKey(reader) };
    case 3:
      return { tag, begin: readBoundary(reader), end: readBoundary(reader) };
    case 4: {
      const key = reader.readBytes(
        storageKitWireLimits.maxVersionstampedKeyOperandBytes,
        "Atomic key operand bytes"
      );
      const param = reader.readBytes(
        storageKitWireLimits.maxVersionstampedValueOperandBytes,
        "Atomic value operand bytes"
      );
      const type = reader.readUInt8();
      if (!Object.values(mutationType).includes(type)) {
        throw StorageKitWireError.unknownMutationType(type);
      }
      validateAtomicOperands(key, param, type);
      return { tag, key, param, mutationType: type };
    }
    default:
      throw StorageKitWireError.unknownWriteOperation(tag);
  }
}

function writeMutation(writer, mutation) {
  writer.writeUInt8(mutation.tag);
  switch (mutation.tag) {
    case 1:
      validateByteLength(
        mutation.key,
        storageKitWireLimits.maxKeyBytes,
        "Key bytes"
      );
      validateByteLength(
        mutation.value,
        storageKitWireLimits.maxValueBytes,
        "Value bytes"
      );
      validateStoredPairByteLengths(
        binaryByteLength(mutation.key),
        binaryByteLength(mutation.value)
      );
      writeKey(writer, mutation.key);
      writeValue(writer, mutation.value);
      break;
    case 2:
      writeKey(writer, mutation.key);
      break;
    case 3:
      writeBoundary(writer, mutation.begin);
      writeBoundary(writer, mutation.end);
      break;
    case 4:
      if (!Object.values(mutationType).includes(mutation.mutationType)) {
        throw StorageKitWireError.unknownMutationType(
          mutation.mutationType
        );
      }
      validateAtomicOperands(
        mutation.key,
        mutation.param,
        mutation.mutationType
      );
      writer.writeBytes(mutation.key);
      writer.writeBytes(mutation.param);
      writer.writeUInt8(mutation.mutationType);
      break;
    default:
      throw StorageKitWireError.unknownWriteOperation(mutation.tag);
  }
}

function validateAtomicOperands(key, param, type) {
  const maximumKey = type === mutationType.setVersionstampedKey
    ? storageKitWireLimits.maxVersionstampedKeyOperandBytes
    : storageKitWireLimits.maxKeyBytes;
  const maximumParam = type === mutationType.setVersionstampedValue
    ? storageKitWireLimits.maxVersionstampedValueOperandBytes
    : storageKitWireLimits.maxValueBytes;
  validateByteLength(key, maximumKey, "Atomic key operand bytes");
  validateByteLength(param, maximumParam, "Atomic value operand bytes");
  const keyBytes = binaryByteLength(key);
  const paramBytes = binaryByteLength(param);
  if (type === mutationType.setVersionstampedKey && keyBytes >= 4) {
    validateStoredPairByteLengths(keyBytes - 4, paramBytes);
  } else if (type === mutationType.setVersionstampedValue
      && paramBytes >= 4) {
    validateStoredPairByteLengths(keyBytes, paramBytes - 4);
  } else if (type !== mutationType.compareAndClear) {
    validateStoredPairByteLengths(keyBytes, paramBytes);
  }
}

function validateStoredPairByteLengths(keyBytes, valueBytes) {
  const total = keyBytes + valueBytes;
  if (!Number.isSafeInteger(total)
      || total > storageKitWireLimits.maxStoredKeyValueBytes) {
    throw StorageKitWireError.limitExceeded(
      "Stored key and value bytes",
      storageKitWireLimits.maxStoredKeyValueBytes
    );
  }
}

function binaryByteLength(value) {
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    return value.byteLength;
  }
  return new Uint8Array(value).byteLength;
}

function readKey(reader) {
  return reader.readBytes(storageKitWireLimits.maxKeyBytes, "Key bytes");
}

function readValue(reader) {
  return reader.readBytes(storageKitWireLimits.maxValueBytes, "Value bytes");
}

function readBoundary(reader) {
  return reader.readBytes(storageKitWireLimits.maxBoundaryBytes, "Boundary bytes");
}

function readOptionalBoundary(reader) {
  return reader.readBool() ? readBoundary(reader) : null;
}

function writeKey(writer, value) {
  validateByteLength(value, storageKitWireLimits.maxKeyBytes, "Key bytes");
  writer.writeBytes(value);
}

function writeValue(writer, value) {
  validateByteLength(value, storageKitWireLimits.maxValueBytes, "Value bytes");
  writer.writeBytes(value);
}

function writeBoundary(writer, value) {
  validateByteLength(
    value,
    storageKitWireLimits.maxBoundaryBytes,
    "Boundary bytes"
  );
  writer.writeBytes(value);
}

function writeOptionalBoundary(writer, value) {
  writer.writeBool(value !== null && value !== undefined);
  if (value !== null && value !== undefined) {
    validateByteLength(value, storageKitWireLimits.maxBoundaryBytes, "Boundary bytes");
    writer.writeBytes(value);
  }
}

function writeOptionalValue(writer, value) {
  writer.writeBool(value !== null && value !== undefined);
  if (value !== null && value !== undefined) {
    writeValue(writer, value);
  }
}

function readRangeLimit(reader) {
  return validateRangeLimit(reader.readInt32());
}

function validateRangeLimit(limit) {
  if (!Number.isInteger(limit)
      || limit <= 0
      || limit > storageKitWireLimits.maxRangeLimit) {
    throw StorageKitWireError.limitExceeded(
      "Range limit",
      storageKitWireLimits.maxRangeLimit
    );
  }
  return limit;
}

function validateCollectionCount(count, maximum, field) {
  if (!Number.isInteger(count) || count < 0 || count > maximum) {
    throw StorageKitWireError.limitExceeded(field, maximum);
  }
}

function validateByteLength(value, maximum, field) {
  const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
  if (bytes.byteLength > maximum) {
    throw StorageKitWireError.limitExceeded(field, maximum);
  }
}

function boundedErrorMessage(message) {
  const value = typeof message === "string" ? message : "StorageKit host failure";
  if (utf8Encoder.encode(value).byteLength <= storageKitWireLimits.maxErrorMessageBytes) {
    return value;
  }
  return "StorageKit host failure exceeded the error message limit";
}
