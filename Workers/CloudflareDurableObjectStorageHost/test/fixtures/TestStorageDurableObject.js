import { DurableObject } from "cloudflare:workers";
import { StorageKitDurableObjectHost } from "../../src/StorageKitDurableObjectHost.js";

export class TestStorageDurableObject extends DurableObject {
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
