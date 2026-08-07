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
  sleepAfter = env.ENV === "prod" ? "5m" : "30s";
  enableInternet = false;
  private ltxStorage = new LTXStorage(this.ctx);
  #live2live = new WeakMap<WebSocket, WebSocket>();

  override async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).hostname === REPLICA_HOST) {
      const ws = this.ltxStorage.accept();
      return new Response(null, { status: 101, webSocket: ws });
    }

    await super.startAndWaitForPorts({
      ports: 3000,
      cancellationOptions: {
        abort: request.signal,
        instanceGetTimeoutMS: 60_000,
        portReadyTimeoutMS: 60_000,
      },
    });

    if (request.method === "GET" && new URL(request.url).pathname === "/api/live/ws") {
      const resp = await this.ctx.container?.getTcpPort(this.defaultPort).fetch(request);
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

      if (!(await this.getSchedule("live-ping"))) {
        await this.schedule(120, "ping", "live-ping");
      }

      return new Response(null, { status: 101, webSocket: client, headers: resp.headers });
    }

    return super.fetch(request);
  }

  onStart() {
    for (const ws of this.ctx.getWebSockets("grafana-live")) {
      ws.close(1000, "Grafana restarted");
    }
  }

  async ping() {
    let hasLive = false;
    for (const ws of this.ctx.getWebSockets("grafana-live")) {
      if (ws.readyState === WebSocket.OPEN) {
        hasLive = true;
        ws.send("{}");
      }
    }
    if (hasLive) {
      await this.schedule(120, "ping");
    }
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.ltxStorage.handleMessage(ws, message);

    if (this.ctx.getTags(ws).includes("grafana-live")) {
      if (message === "{}") return;
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
  }
}

Grafana.outboundByHost = {
  [REPLICA_HOST]: (req, env, ctx) =>
    env.GRAFANA.get(env.GRAFANA.idFromString(ctx.containerId)).fetch(req),
  [AI_HOST]: (req) => ai.fetch(req),
};
