import wasmModule from "./CloudflareDurableObjectStorageWasm.wasm";
import baseWorker, {
  CloudflareDurableObjectStorageHost as BaseCloudflareDurableObjectStorageHost,
} from "./CloudflareDurableObjectStorageHost.js";

export class CloudflareDurableObjectStorageHost extends BaseCloudflareDurableObjectStorageHost {
  constructor(ctx, env) {
    super(ctx, Object.assign({}, env, {
      STORAGEKIT_WASM: env?.STORAGEKIT_WASM ?? wasmModule,
    }));
  }
}

export default baseWorker;
