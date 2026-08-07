import {
  invalidContentLengthResponse,
  hostConfigurationErrorResponse,
  hasStorageKitWireContentType,
  payloadTooLargeResponse,
  readBoundedRequestBytes,
  rejectOversizedContentLength,
  testStorageRequestByteLimit,
  TestStorageInvalidContentLengthError,
  TestStorageConfigurationError,
  unsupportedMediaTypeResponse,
  TestStoragePayloadTooLargeError,
} from "./TestStorageRequestLimits.js";
import { TestStorageRequestAuthorizer } from "./TestStorageRequestAuthorizer.js";
import { nameForPartitionIdentity } from "../../src/StorageKitPartitionIdentity.js";
import { statusCode } from "../../src/StorageKitWireConstants.js";
import { StorageKitWire } from "../../src/StorageKitWire.js";

const durableObjectBindingName = "STORAGEKIT_DURABLE_OBJECT";

export async function handleTestStorageRequest(request, env) {
  if (request.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405 });
  }

  const authorization = await new TestStorageRequestAuthorizer(
    env?.STORAGEKIT_ACCESS_TOKEN
  ).authorize(request);
  if (!authorization.allowed) {
    return authorization.response;
  }
  if (!hasStorageKitWireContentType(request)) {
    return unsupportedMediaTypeResponse();
  }

  let limit;
  try {
    limit = testStorageRequestByteLimit(env);
  } catch (error) {
    if (error instanceof TestStorageConfigurationError) {
      return hostConfigurationErrorResponse(error);
    }
    throw error;
  }
  const oversizedResponse = rejectOversizedContentLength(request, limit);
  if (oversizedResponse !== null) {
    return oversizedResponse;
  }

  let requestBytes;
  try {
    requestBytes = await readBoundedRequestBytes(request, limit);
  } catch (error) {
    if (error instanceof TestStoragePayloadTooLargeError) {
      return payloadTooLargeResponse(error.limit);
    }
    if (error instanceof TestStorageInvalidContentLengthError) {
      return invalidContentLengthResponse();
    }
    throw error;
  }
  let decodedRequest;
  try {
    decodedRequest = StorageKitWire.decodeRoutingPartitionIdentity(requestBytes);
  } catch (error) {
    return storageWireResponse(StorageKitWire.encodeFailure(
      statusCode.invalidOperation,
      errorMessage(error)
    ));
  }

  const namespace = env?.[durableObjectBindingName];
  if (namespace === undefined || namespace === null) {
    return storageWireResponse(StorageKitWire.encodeFailure(
      statusCode.resourceUnavailable,
      "Cloudflare Durable Object binding is not configured"
    ));
  }

  let stub;
  try {
    const durableObjectName = nameForPartitionIdentity(decodedRequest.partitionIdentity);
    const id = namespace.idFromName(durableObjectName);
    stub = namespace.get(id);
  } catch (error) {
    return storageWireResponse(StorageKitWire.encodeFailure(
      statusCode.invalidOperation,
      errorMessage(error)
    ));
  }

  const responseBytes = await stub.execute(requestBytes);
  return storageWireResponse(responseBytes);
}

export default {
  fetch: handleTestStorageRequest,
};

function storageWireResponse(bytes) {
  return new Response(bytes, {
    headers: {
      "content-type": "application/octet-stream",
    },
  });
}

function errorMessage(error) {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
