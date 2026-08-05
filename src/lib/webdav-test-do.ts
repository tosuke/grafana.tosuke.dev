import { DurableObject } from "cloudflare:workers";
import { webdav } from "./webdav";

/** A lightweight Durable Object used to integration-test the WebDAV SQLite backend. */
export class WebDavTest extends DurableObject {
  private readonly app = webdav(this.ctx.storage);

  override async fetch(request: Request): Promise<Response> {
    return await this.app.fetch(request);
  }
}
