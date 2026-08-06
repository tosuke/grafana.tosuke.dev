import { Container } from "@cloudflare/containers";
import { env } from "cloudflare:workers";
import { newWebSocketRpcSession } from "capnweb";
import { LTXStorage } from "./lib/ltx2";

const REPLICA_HOST = "replica.worker";

export function grafana(): DurableObjectStub<Grafana> {
  return env.GRAFANA.getByName("grafana");
}

export class Grafana extends Container {
  defaultPort = 3000;
  sleepAfter = "1m";
  enableInternet = false;
  private replicaSocket: WebSocket | undefined;
  private replicaSession: ReturnType<typeof newWebSocketRpcSession> | undefined;
  private ltxStorage = new LTXStorage(this.ctx);

  private closeReplica(): void {
    const session = this.replicaSession;
    if (session) {
      const dispose = (session as unknown as { [key: symbol]: unknown })[Symbol.for("dispose")];
      if (typeof dispose === "function") dispose.call(session);
    }
    this.replicaSocket?.close(1000, "Grafana container stopped");
    this.replicaSocket = undefined;
    this.replicaSession = undefined;
  }

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
};
