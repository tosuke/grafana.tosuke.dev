import { DurableObject } from "cloudflare:workers";
import { LTXStorage } from "./ltx";

export class LtxTest extends DurableObject {
  private readonly ltx = new LTXStorage(this.ctx);

  override async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket")
      return new Response("Not found", { status: 404 });
    const webSocket = this.ltx.accept();
    return new Response(null, { status: 101, webSocket });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.ltx.handleMessage(ws, message);
  }

  webSocketClose(ws: WebSocket): void {
    this.ltx.handleClose(ws);
  }
}
