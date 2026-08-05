import { Container } from "@cloudflare/containers";
import { env } from "cloudflare:workers";
import { webdav } from "./lib/webdav";

export function grafana(): DurableObjectStub<Grafana> {
  return env.GRAFANA.getByName("grafana");
}

export class Grafana extends Container {
  defaultPort = 3000;
  sleepAfter = "1m";
  enableInternet = false;
  private readonly webdavApp = webdav(this.ctx.storage);

  override async fetch(request: Request): Promise<Response> {
    console.log(`Grafana.fetch: ${request.method} ${request.url}`);
    if (new URL(request.url).hostname === "webdav.worker") return this.webdavApp.fetch(request);
    await super.startAndWaitForPorts(3000, {
      abort: request.signal,
      instanceGetTimeoutMS: 60_000,
      portReadyTimeoutMS: 60_000,
    });
    return super.fetch(request);
  }
}

Grafana.outboundByHost = {
  "webdav.worker": (req, env, ctx) =>
    env.GRAFANA.get(env.GRAFANA.idFromString(ctx.containerId)).fetch(new Request(req)),
};
