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
  sleepAfter = "1m";
  enableInternet = false;
  private ltxStorage = new LTXStorage(this.ctx);

  private acceptReplica(): Response {
    const ws = this.ltxStorage.accept();
    return new Response(null, { status: 101, webSocket: ws });
  }

  override async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).hostname === REPLICA_HOST) return this.acceptReplica();

    await super.startAndWaitForPorts({
      ports: 3000,
      cancellationOptions: {
        abort: request.signal,
        instanceGetTimeoutMS: 60_000,
        portReadyTimeoutMS: 60_000,
      },
    });
    return super.fetch(request);
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.ltxStorage.handleMessage(ws, message);
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
