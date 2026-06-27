import { StorageKitBinaryReader } from "./StorageKitBinaryReader.js";
import { StorageKitBinaryWriter } from "./StorageKitBinaryWriter.js";
import { validateScope } from "./StorageKitScope.js";
import {
  keySelectorKind,
  mutationType,
  operation,
  protocolVersion,
  statusCode,
} from "./StorageKitWireConstants.js";
import { StorageKitWireError } from "./StorageKitWireError.js";

export class StorageKitWireCodec {
  static decodeRequest(bytes) {
    const reader = new StorageKitBinaryReader(bytes);
    const version = reader.readUInt8();
    if (version !== protocolVersion) {
      throw StorageKitWireError.unsupportedProtocolVersion(version);
    }
    const op = reader.readUInt8();
    let request;
    switch (op) {
      case operation.readiness:
        request = { operation: op, scope: readScope(reader) };
        break;
      case operation.read:
        request = {
          operation: op,
          scope: readScope(reader),
          key: reader.readBytes(),
          snapshot: reader.readBool(),
          expectedReadVersion: readOptionalInt64(reader),
        };
        break;
      case operation.range:
        request = {
          operation: op,
          scope: readScope(reader),
          begin: readKeySelector(reader),
          end: readKeySelector(reader),
          limit: reader.readInt32(),
          reverse: reader.readBool(),
          snapshot: reader.readBool(),
          expectedReadVersion: readOptionalInt64(reader),
          cursor: readOptionalString(reader),
        };
        break;
      case operation.commit:
        request = {
          operation: op,
          scope: readScope(reader),
          observedReadVersion: readOptionalInt64(reader),
          mutations: readMutations(reader),
          readConflictRanges: readKeyRanges(reader),
        };
        break;
      default:
        throw StorageKitWireError.unknownOperation(op);
    }
    reader.ensureFullyRead();
    return request;
  }

  static encodeRequest(request) {
    const writer = new StorageKitBinaryWriter();
    writer.writeUInt8(protocolVersion);
    writer.writeUInt8(request.operation);
    writeRequestPayload(writer, request);
    return writer.toBytes();
  }

  static decodeResponse(bytes) {
    const reader = new StorageKitBinaryReader(bytes);
    const version = reader.readUInt8();
    if (version !== protocolVersion) {
      throw StorageKitWireError.unsupportedProtocolVersion(version);
    }
    const status = reader.readUInt8();
    if (status !== statusCode.ok) {
      const response = {
        status,
        message: reader.readString(),
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
          commitVersion: reader.readInt64(),
          metadataInitialized: reader.readBool(),
        };
        break;
      case operation.read:
        response = {
          status,
          operation: op,
          value: readOptionalBytes(reader),
          currentCommitVersion: reader.readInt64(),
        };
        break;
      case operation.range:
        response = {
          status,
          operation: op,
          rows: readRows(reader),
          nextCursor: readOptionalString(reader),
          currentCommitVersion: reader.readInt64(),
          conflictRange: readOptionalKeyRange(reader),
        };
        break;
      case operation.commit:
        response = {
          status,
          operation: op,
          committedVersion: reader.readInt64(),
        };
        break;
      default:
        throw StorageKitWireError.unknownOperation(op);
    }
    reader.ensureFullyRead();
    return response;
  }

  static encodeResponse(response) {
    const writer = new StorageKitBinaryWriter();
    writer.writeUInt8(protocolVersion);
    writer.writeUInt8(response.status ?? statusCode.ok);
    if ((response.status ?? statusCode.ok) !== statusCode.ok) {
      writer.writeString(response.message ?? "StorageKit host failure");
      return writer.toBytes();
    }
    writer.writeUInt8(response.operation);
    writeResponsePayload(writer, response);
    return writer.toBytes();
  }

  static encodeFailure(status, message) {
    return this.encodeResponse({ status, message });
  }
}

function writeRequestPayload(writer, request) {
  switch (request.operation) {
    case operation.readiness:
      writeScope(writer, request.scope);
      break;
    case operation.read:
      writeScope(writer, request.scope);
      writer.writeBytes(request.key);
      writer.writeBool(request.snapshot);
      writeOptionalInt64(writer, request.expectedReadVersion);
      break;
    case operation.range:
      writeScope(writer, request.scope);
      writeKeySelector(writer, request.begin);
      writeKeySelector(writer, request.end);
      writer.writeInt32(request.limit);
      writer.writeBool(request.reverse);
      writer.writeBool(request.snapshot);
      writeOptionalInt64(writer, request.expectedReadVersion);
      writeOptionalString(writer, request.cursor);
      break;
    case operation.commit:
      writeScope(writer, request.scope);
      writeOptionalInt64(writer, request.observedReadVersion);
      writer.writeUInt32(request.mutations.length);
      for (const mutation of request.mutations) {
        writeMutation(writer, mutation);
      }
      writer.writeUInt32(request.readConflictRanges?.length ?? 0);
      for (const range of request.readConflictRanges ?? []) {
        writeKeyRange(writer, range);
      }
      break;
    default:
      throw StorageKitWireError.unknownOperation(request.operation);
  }
}

function writeResponsePayload(writer, response) {
  switch (response.operation) {
    case operation.readiness:
      writer.writeUInt32(response.schemaVersion);
      writer.writeInt64(response.commitVersion);
      writer.writeBool(response.metadataInitialized);
      break;
    case operation.read:
      writeOptionalBytes(writer, response.value);
      writer.writeInt64(response.currentCommitVersion);
      break;
    case operation.range:
      writer.writeUInt32(response.rows.length);
      for (const row of response.rows) {
        writer.writeBytes(row.key);
        writer.writeBytes(row.value);
      }
      writeOptionalString(writer, response.nextCursor);
      writer.writeInt64(response.currentCommitVersion);
      writeOptionalKeyRange(writer, response.conflictRange ?? null);
      break;
    case operation.commit:
      writer.writeInt64(response.committedVersion);
      break;
    default:
      throw StorageKitWireError.unknownOperation(response.operation);
  }
}

function readScope(reader) {
  return validateScope({
    databaseID: reader.readString(),
    tenantID: readOptionalString(reader),
    workspaceID: readOptionalString(reader),
  });
}

function writeScope(writer, scope) {
  const validated = validateScope(scope);
  writer.writeString(validated.databaseID);
  writeOptionalString(writer, validated.tenantID);
  writeOptionalString(writer, validated.workspaceID);
}

function readOptionalString(reader) {
  return reader.readBool() ? reader.readString() : null;
}

function writeOptionalString(writer, value) {
  writer.writeBool(value !== null && value !== undefined);
  if (value !== null && value !== undefined) {
    writer.writeString(value);
  }
}

function readOptionalInt64(reader) {
  return reader.readBool() ? reader.readInt64() : null;
}

function writeOptionalInt64(writer, value) {
  writer.writeBool(value !== null && value !== undefined);
  if (value !== null && value !== undefined) {
    writer.writeInt64(value);
  }
}

function readOptionalBytes(reader) {
  return reader.readBool() ? reader.readBytes() : null;
}

function writeOptionalBytes(writer, value) {
  writer.writeBool(value !== null && value !== undefined);
  if (value !== null && value !== undefined) {
    writer.writeBytes(value);
  }
}

function readOptionalKeyRange(reader) {
  return reader.readBool() ? readKeyRange(reader) : null;
}

function writeOptionalKeyRange(writer, range) {
  writer.writeBool(range !== null && range !== undefined);
  if (range !== null && range !== undefined) {
    writeKeyRange(writer, range);
  }
}

function readKeySelector(reader) {
  const kind = reader.readUInt8();
  if (!Object.values(keySelectorKind).includes(kind)) {
    throw StorageKitWireError.unknownKeySelector(kind);
  }
  return {
    kind,
    key: reader.readBytes(),
  };
}

function writeKeySelector(writer, selector) {
  if (!Object.values(keySelectorKind).includes(selector.kind)) {
    throw StorageKitWireError.unknownKeySelector(selector.kind);
  }
  writer.writeUInt8(selector.kind);
  writer.writeBytes(selector.key);
}

function readKeyRange(reader) {
  return {
    begin: readOptionalBytes(reader),
    end: readOptionalBytes(reader),
  };
}

function writeKeyRange(writer, range) {
  writeOptionalBytes(writer, range.begin ?? null);
  writeOptionalBytes(writer, range.end ?? null);
}

function readKeyRanges(reader) {
  const count = reader.readUInt32();
  const ranges = [];
  for (let index = 0; index < count; index += 1) {
    ranges.push(readKeyRange(reader));
  }
  return ranges;
}

function readRows(reader) {
  const count = reader.readUInt32();
  const rows = [];
  for (let index = 0; index < count; index += 1) {
    rows.push({
      key: reader.readBytes(),
      value: reader.readBytes(),
    });
  }
  return rows;
}

function readMutations(reader) {
  const count = reader.readUInt32();
  const mutations = [];
  for (let index = 0; index < count; index += 1) {
    mutations.push(readMutation(reader));
  }
  return mutations;
}

function readMutation(reader) {
  const tag = reader.readUInt8();
  switch (tag) {
    case 1:
      return { tag, key: reader.readBytes(), value: reader.readBytes() };
    case 2:
      return { tag, key: reader.readBytes() };
    case 3:
      return { tag, begin: reader.readBytes(), end: reader.readBytes() };
    case 4: {
      const key = reader.readBytes();
      const param = reader.readBytes();
      const type = reader.readUInt8();
      if (!Object.values(mutationType).includes(type)) {
        throw StorageKitWireError.unknownMutationType(type);
      }
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
      writer.writeBytes(mutation.key);
      writer.writeBytes(mutation.value);
      break;
    case 2:
      writer.writeBytes(mutation.key);
      break;
    case 3:
      writer.writeBytes(mutation.begin);
      writer.writeBytes(mutation.end);
      break;
    case 4:
      writer.writeBytes(mutation.key);
      writer.writeBytes(mutation.param);
      writer.writeUInt8(mutation.mutationType);
      break;
    default:
      throw StorageKitWireError.unknownWriteOperation(mutation.tag);
  }
}
