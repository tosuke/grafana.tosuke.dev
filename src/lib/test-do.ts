import { DurableObject } from "cloudflare:workers";
import { DOLTXStore } from "./ltx-webdav";

export class Test extends DurableObject {
  ltxStore() {
    return new DOLTXStore(this.ctx);
  }
}
