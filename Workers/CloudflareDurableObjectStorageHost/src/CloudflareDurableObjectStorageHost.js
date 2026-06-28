import { StorageKitDurableObjectHost } from "./StorageKitDurableObjectHost.js";
import {
  invalidContentLengthResponse,
  payloadTooLargeResponse,
  readBoundedRequestBytes,
  rejectOversizedContentLength,
  storageKitMaxRequestBytes,
  StorageKitInvalidContentLengthError,
  StorageKitPayloadTooLargeError,
} from "./StorageKitHostLimits.js";
import { StorageKitRequestAuthorizer } from "./StorageKitRequestAuthorizer.js";
import { nameForScope } from "./StorageKitScope.js";
import { StorageKitWasmBridge } from "./StorageKitWasmBridge.js";
import { statusCode } from "./StorageKitWireConstants.js";
import { StorageKitWireCodec } from "./StorageKitWireCodec.js";

const durableObjectBindingName = "STORAGEKIT_DURABLE_OBJECT";

export class CloudflareDurableObjectStorageHost {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.host = new StorageKitDurableObjectHost(
      ctx.storage.sql,
      (callback) => ctx.storage.transactionSync(callback)
    );
    this.bridgePromise = null;
  }

  async fetch(request) {
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    let requestBytes;
    try {
      requestBytes = await readBoundedRequestBytes(request, storageKitMaxRequestBytes(this.env));
    } catch (error) {
      if (error instanceof StorageKitPayloadTooLargeError) {
        return payloadTooLargeResponse(error.limit);
      }
      if (error instanceof StorageKitInvalidContentLengthError) {
        return invalidContentLengthResponse();
      }
      throw error;
    }
    const responseBytes = await this.dispatch(requestBytes);
    return new Response(responseBytes, {
      headers: {
        "content-type": "application/octet-stream",
      },
    });
  }

  async dispatch(requestBytes) {
    if (this.env?.STORAGEKIT_WASM === undefined || this.env?.STORAGEKIT_WASM === null) {
      return this.host.dispatchBytes(requestBytes);
    }
    const bridge = await this.bridge();
    return bridge.dispatch(requestBytes);
  }

  bridge() {
    if (this.bridgePromise === null) {
      this.bridgePromise = StorageKitWasmBridge.instantiate(this.env.STORAGEKIT_WASM, this.host)
        .then((bridge) => {
          this.host.setMutationApplier(bridge);
          return bridge;
        });
    }
    return this.bridgePromise;
  }
}

export default {
  async fetch(request, env) {
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    const authorization = await new StorageKitRequestAuthorizer(env?.STORAGEKIT_ACCESS_TOKEN).authorize(request);
    if (!authorization.allowed) {
      return authorization.response;
    }

    const limit = storageKitMaxRequestBytes(env);
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
      decodedRequest = StorageKitWireCodec.decodeRequest(requestBytes);
    } catch (error) {
      return binaryResponse(StorageKitWireCodec.encodeFailure(
        statusCode.invalidOperation,
        errorMessage(error)
      ));
    }

    const namespace = env?.[durableObjectBindingName];
    if (namespace === undefined || namespace === null) {
      return binaryResponse(StorageKitWireCodec.encodeFailure(
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
      return binaryResponse(StorageKitWireCodec.encodeFailure(
        statusCode.invalidOperation,
        errorMessage(error)
      ));
    }

    return stub.fetch(new Request(request.url, {
      method: "POST",
      headers: {
        "content-type": "application/octet-stream",
      },
      body: requestBytes,
    }));
  },
};

function binaryResponse(bytes) {
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
