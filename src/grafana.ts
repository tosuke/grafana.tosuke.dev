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

  override async fetch(request: Request): Promise<Response> {
    await super.startAndWaitForPorts(3000, {
      abort: request.signal,
      instanceGetTimeoutMS: 60_000,
      portReadyTimeoutMS: 60_000,
    });
    return super.fetch(request);
  }
}

Grafana.outboundByHost = {
  "r2dav.worker": async (req, env) => {
    const resp = await webdav(env.GRAFANA_LITESTREAM_BUCKET).fetch(req, env);
    console.log("r2dav.worker", req.method, req.url, resp.status);
    return resp;
  },
};
