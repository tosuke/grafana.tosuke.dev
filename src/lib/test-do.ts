import { DurableObject } from "cloudflare:workers";
import { DOLTXStore } from "./ltx-webdav";

export class Test extends DurableObject {
  get ltxStore() {
    return new DOLTXStore(this.ctx);
  }
}
