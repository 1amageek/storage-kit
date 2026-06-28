export const defaultStorageKitMaxRequestBytes = 4 * 1024 * 1024;

export class StorageKitPayloadTooLargeError extends Error {
  constructor(limit) {
    super(`StorageKit wire request exceeds ${limit} bytes`);
    this.name = "StorageKitPayloadTooLargeError";
    this.limit = limit;
  }
}

export class StorageKitInvalidContentLengthError extends Error {
  constructor() {
    super("Invalid Content-Length");
    this.name = "StorageKitInvalidContentLengthError";
  }
}

export function storageKitMaxRequestBytes(env) {
  const configured = env?.STORAGEKIT_MAX_REQUEST_BYTES;
  if (configured === undefined || configured === null || configured === "") {
    return defaultStorageKitMaxRequestBytes;
  }
  const value = Number(configured);
  if (!Number.isInteger(value) || value <= 0 || value > 0xffff_ffff) {
    return defaultStorageKitMaxRequestBytes;
  }
  return value;
}

export function rejectOversizedContentLength(request, limit) {
  const contentLength = parseContentLength(request);
  if (contentLength === null) {
    return null;
  }
  if (contentLength instanceof StorageKitInvalidContentLengthError) {
    return invalidContentLengthResponse();
  }
  return contentLength > limit ? payloadTooLargeResponse(limit) : null;
}

export async function readBoundedRequestBytes(request, limit) {
  const contentLength = parseContentLength(request);
  if (contentLength instanceof StorageKitInvalidContentLengthError) {
    throw contentLength;
  }
  if (contentLength !== null && contentLength > limit) {
    throw new StorageKitPayloadTooLargeError(limit);
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
        throw new StorageKitPayloadTooLargeError(limit);
      }
      chunks.push(chunk);
    }
  } finally {
    reader.releaseLock();
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

function parseContentLength(request) {
  const header = request.headers.get("content-length");
  if (header === null) {
    return null;
  }
  const value = Number(header);
  if (!Number.isInteger(value) || value < 0) {
    return new StorageKitInvalidContentLengthError();
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
