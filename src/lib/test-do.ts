import { DurableObject } from "cloudflare:workers";
import { DOLTXStore } from "./ltx-webdav";
import { DOS3Store } from "./s3-do-backend";

export class Test extends DurableObject {
  ltxStore() {
    return new DOLTXStore(this.ctx);
  }

  s3Store() {
    return new DOS3Store(this.ctx.storage);
  }
}
