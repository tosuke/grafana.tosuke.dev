import { Container } from "@cloudflare/containers";
import { env } from "cloudflare:workers";
import { LTXStorage } from "./lib/ltx";
import { ai } from "./lib/ai";

const REPLICA_HOST = "replica.worker";
const AI_HOST = "ai.worker";

export function grafana(): DurableObjectStub<Grafana> {
  return env.GRAFANA.getByName("grafana");
}

export class Grafana extends Container {
  defaultPort = 3000;
  sleepAfter = "5m";
  enableInternet = false;
  private ltxStorage = new LTXStorage(this.ctx);
  #live2live = new Map<WebSocket, WebSocket>();

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.hostname === REPLICA_HOST) {
      const ws = this.ltxStorage.accept();
      return new Response(null, { status: 101, webSocket: ws });
    }

    if (request.method === "GET" && url.pathname === "/api/live/ws") {
      const container = this.ctx.container;
      if (!container) {
        console.error("Container is not available");
        return new Response("Container is not available", { status: 500 });
      }
      if (!container.running) {
        await super.start();
      }
      await super.waitForPort({
        signal: request.signal,
        portToCheck: this.defaultPort,
        waitInterval: 1000,
        retries: 60,
      });
      const resp = await this.ctx.container?.getTcpPort(this.defaultPort).fetch(
        new Request(request.url.replace(/^https/, "http"), {
          method: "GET",
          headers: request.headers,
        }),
      );
      if (!resp) {
        console.error("Failed to connect to Grafana on port", this.defaultPort);
        return new Response("Failed to connect to Grafana", { status: 500 });
      }
      const liveWs = resp.webSocket;
      if (!liveWs) {
        return resp;
      }
      liveWs.accept();
      const { 0: client, 1: server } = new WebSocketPair();
      this.ctx.acceptWebSocket(server, ["grafana-live"]);
      this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("{}", ""));
      this.#live2live.set(server, liveWs);

      liveWs.addEventListener("message", (event) => {
        if (typeof event.data === "string") {
          if (event.data === "{}") {
            liveWs.send("{}");
            return;
          }
          const json = JSON.parse(event.data);
          if (json.connect != null) {
            server.send(
              JSON.stringify({
                ...json,
                connect: {
                  ...json.connect,
                  ping: 120,
                },
              }),
            );
            return;
          }
        }
        server.send(event.data);
      });

      if ((await this.listSchedules("ping")).length === 0) {
        await this.schedule(120, "ping");
      }

      return new Response(null, { status: 101, webSocket: client, headers: resp.headers });
    }

    return super.fetch(request);
  }

  onStart() {
    console.log("Grafana started");
    for (const ws of this.ctx.getWebSockets("grafana-live")) {
      ws.close(1000, "Grafana restarted");
    }
  }

  override async onActivityExpired() {
    console.log("Grafana activity expired, closing live connections");
    for (const ws of this.#live2live.values()) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.close(1000, "Grafana activity expired");
      }
    }
    await this.stop();
  }

  async ping() {
    let hasLive = false;
    for (const ws of this.ctx.getWebSockets("grafana-live")) {
      if (ws.readyState === WebSocket.OPEN) {
        hasLive = true;
        ws.send("{}");
      }
    }
    this.deleteSchedules("ping");
    if (hasLive) {
      await this.schedule(120, "ping");
    }
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.ltxStorage.handleMessage(ws, message);

    if (this.ctx.getTags(ws).includes("grafana-live")) {
      const liveWs = this.#live2live.get(ws);
      if (!liveWs || liveWs.readyState !== WebSocket.OPEN) {
        console.error("Live WebSocket is not open for message:", message);
        ws.close(1011, "Live WebSocket is not open");
        return;
      }
      liveWs.send(message);
    }
  }

  async webSocketClose(ws: WebSocket): Promise<void> {
    this.ltxStorage.handleClose(ws);

    if (this.ctx.getTags(ws).includes("grafana-live")) {
      const liveWs = this.#live2live.get(ws);
      this.#live2live.delete(ws);
      if (liveWs && liveWs.readyState === WebSocket.OPEN) {
        liveWs.close(1000, "Client disconnected");
      }
    }
  }
}

Grafana.outboundByHost = {
  [REPLICA_HOST]: (req, env, ctx) =>
    env.GRAFANA.get(env.GRAFANA.idFromString(ctx.containerId)).fetch(req),
  [AI_HOST]: (req) => ai.fetch(req),
};
