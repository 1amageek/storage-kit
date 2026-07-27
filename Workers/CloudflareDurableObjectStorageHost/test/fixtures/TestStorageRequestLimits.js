import { storageKitWireLimits } from "../../src/StorageKitWireLimits.js";

export const defaultTestStorageRequestByteLimit = 4 * 1024 * 1024;

export class TestStoragePayloadTooLargeError extends Error {
  constructor(limit) {
    super(`StorageKit wire request exceeds ${limit} bytes`);
    this.name = "TestStoragePayloadTooLargeError";
    this.limit = limit;
  }
}

export class TestStorageInvalidContentLengthError extends Error {
  constructor() {
    super("Invalid Content-Length");
    this.name = "TestStorageInvalidContentLengthError";
  }
}

export class TestStorageConfigurationError extends Error {
  constructor(message) {
    super(message);
    this.name = "TestStorageConfigurationError";
  }
}

export function testStorageRequestByteLimit(env) {
  const configured = env?.STORAGEKIT_MAX_REQUEST_BYTES;
  if (configured === undefined || configured === null || configured === "") {
    return defaultTestStorageRequestByteLimit;
  }
  const value = Number(configured);
  if (!Number.isInteger(value)
      || value <= 0
      || value > storageKitWireLimits.maxFrameBytes) {
    throw new TestStorageConfigurationError(
      `STORAGEKIT_MAX_REQUEST_BYTES must be an integer from 1 through ${storageKitWireLimits.maxFrameBytes}`
    );
  }
  return value;
}

export function rejectOversizedContentLength(request, limit) {
  const contentLength = parseContentLength(request);
  if (contentLength === null) {
    return null;
  }
  if (contentLength instanceof TestStorageInvalidContentLengthError) {
    return invalidContentLengthResponse();
  }
  return contentLength > limit ? payloadTooLargeResponse(limit) : null;
}

export async function readBoundedRequestBytes(request, limit) {
  const contentLength = parseContentLength(request);
  if (contentLength instanceof TestStorageInvalidContentLengthError) {
    throw contentLength;
  }
  if (contentLength !== null && contentLength > limit) {
    throw new TestStoragePayloadTooLargeError(limit);
  }
  if (request.body === null) {
    return new Uint8Array();
  }

  const reader = request.body.getReader();
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
      total += chunk.byteLength;
      if (total > limit) {
        await cancelReader(reader);
        throw new TestStoragePayloadTooLargeError(limit);
      }
      chunks.push(chunk);
    }
  } finally {
    reader.releaseLock();
  }

  if (chunks.length === 0) {
    return new Uint8Array();
  }
  if (chunks.length === 1) {
    return chunks[0];
  }

  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

export function payloadTooLargeResponse(limit) {
  return new Response(`StorageKit wire request exceeds ${limit} bytes`, { status: 413 });
}

export function invalidContentLengthResponse() {
  return new Response("Invalid Content-Length", { status: 400 });
}

export function hostConfigurationErrorResponse(error) {
  return new Response(error.message, { status: 500 });
}

export function unsupportedMediaTypeResponse() {
  return new Response("Content-Type must be application/octet-stream", {
    status: 415,
  });
}

export function hasStorageKitWireContentType(request) {
  const value = request.headers.get("content-type");
  if (value === null) {
    return false;
  }
  return value.split(";", 1)[0].trim().toLowerCase()
    === "application/octet-stream";
}

function parseContentLength(request) {
  const header = request.headers.get("content-length");
  if (header === null) {
    return null;
  }
  const value = Number(header);
  if (!Number.isInteger(value) || value < 0) {
    return new TestStorageInvalidContentLengthError();
  }
  return value;
}

async function cancelReader(reader) {
  try {
    await reader.cancel();
  } catch {
    // The payload limit error is the authoritative failure for this request.
  }
}
