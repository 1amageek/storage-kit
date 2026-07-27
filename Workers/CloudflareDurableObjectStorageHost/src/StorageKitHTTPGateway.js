import {
  invalidContentLengthResponse,
  hostConfigurationErrorResponse,
  hasStorageKitWireContentType,
  payloadTooLargeResponse,
  readBoundedRequestBytes,
  rejectOversizedContentLength,
  storageKitMaxRequestBytes,
  StorageKitInvalidContentLengthError,
  StorageKitHostConfigurationError,
  unsupportedMediaTypeResponse,
  StorageKitPayloadTooLargeError,
} from "./StorageKitHostLimits.js";
import { StorageKitRequestAuthorizer } from "./StorageKitRequestAuthorizer.js";
import { nameForScope } from "./StorageKitScope.js";
import { statusCode } from "./StorageKitWireConstants.js";
import { StorageKitWireCodec } from "./StorageKitWireCodec.js";

const durableObjectBindingName = "STORAGEKIT_DURABLE_OBJECT";

export async function handleStorageKitHTTPRequest(request, env) {
  if (request.method !== "POST") {
    return new Response("Method Not Allowed", { status: 405 });
  }

  const authorization = await new StorageKitRequestAuthorizer(
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
    limit = storageKitMaxRequestBytes(env);
  } catch (error) {
    if (error instanceof StorageKitHostConfigurationError) {
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
    if (error instanceof StorageKitPayloadTooLargeError) {
      return payloadTooLargeResponse(error.limit);
    }
    if (error instanceof StorageKitInvalidContentLengthError) {
      return invalidContentLengthResponse();
    }
    throw error;
  }
  let decodedRequest;
  try {
    decodedRequest = StorageKitWireCodec.decodeRoutingScope(requestBytes);
  } catch (error) {
    return storageWireResponse(StorageKitWireCodec.encodeFailure(
      statusCode.invalidOperation,
      errorMessage(error)
    ));
  }

  const namespace = env?.[durableObjectBindingName];
  if (namespace === undefined || namespace === null) {
    return storageWireResponse(StorageKitWireCodec.encodeFailure(
      statusCode.resourceUnavailable,
      "Cloudflare Durable Object binding is not configured"
    ));
  }

  let stub;
  try {
    const durableObjectName = nameForScope(decodedRequest.scope);
    const id = namespace.idFromName(durableObjectName);
    stub = namespace.get(id);
  } catch (error) {
    return storageWireResponse(StorageKitWireCodec.encodeFailure(
      statusCode.invalidOperation,
      errorMessage(error)
    ));
  }

  const responseBytes = await stub.execute(requestBytes);
  return storageWireResponse(responseBytes);
}

export default {
  fetch: handleStorageKitHTTPRequest,
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
