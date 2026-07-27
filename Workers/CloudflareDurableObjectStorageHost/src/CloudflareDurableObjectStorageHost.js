import { DurableObject } from "cloudflare:workers";
import { StorageKitDurableObjectHost } from "./StorageKitDurableObjectHost.js";

export class CloudflareDurableObjectStorageHost extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.host = new StorageKitDurableObjectHost(
      ctx.storage.sql,
      (operation) => ctx.storage.transactionSync(operation)
    );
    ctx.blockConcurrencyWhile(async () => {
      this.host.migrate();
    });
  }

  execute(requestBytes) {
    return this.host.dispatchBytes(requestBytes);
  }
}
