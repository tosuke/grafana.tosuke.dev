import { Container } from "@cloudflare/containers";
import { env, tracing, waitUntil } from "cloudflare:workers";
import { createWorkersAI } from "workers-ai-provider";
import { decodeWebDAVMethod, DOLTXStore, webdavApp } from "./lib/ltx-webdav";
import { createAi } from "./lib/ai";
import { Litestream } from "./lib/litestream";
import {
  attributes,
  environmentResourceAttributes,
  exportMetricsAsOTLPHTTP,
  scrapeMetrics,
} from "./lib/metric";
import { ATTR_SERVICE_INSTANCE_ID } from "@opentelemetry/semantic-conventions";
import {
  ExportMetricsServiceRequestSchema,
  MetricsService,
} from "@buf/opentelemetry_opentelemetry.bufbuild_es/opentelemetry/proto/collector/metrics/v1/metrics_service_pb";
import { create } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { compressionGzip, createGrpcWebTransport } from "./lib/grpc";

interface GrafanaRPC extends Rpc.DurableObjectBranded {
  ltxStore(): DOLTXStore;
  litestream(): Litestream;
}

export function grafana(): DurableObjectStub<GrafanaRPC> {
  return env.GRAFANA.getByName("grafana");
}

export class Grafana extends Container implements GrafanaRPC {
  defaultPort = 3000;
  sleepAfter = "5m";
  enableInternet = false;
  #live2live = new Map<WebSocket, WebSocket>();

  override async fetch(request: Request): Promise<Response> {
    return super.fetch(request);
  }

  async onStart() {
    console.log("Grafana started");

    for (const ws of this.ctx.getWebSockets("grafana-live")) {
      ws.close(1000, "Grafana restarted");
    }

    await this.scheduleScrapeMetrics();
  }

  async onStop() {
    console.log("Grafana stopped");
    await this.stopScrapeMetrics();
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
    if (!this.ctx.getTags(ws).includes("grafana-live")) return;
    const liveWs = this.#live2live.get(ws);
    if (!liveWs || liveWs.readyState !== WebSocket.OPEN) {
      console.error("Live WebSocket is not open for message:", message);
      ws.close(1011, "Live WebSocket is not open");
      return;
    }
    liveWs.send(message);
  }

  async webSocketClose(ws: WebSocket): Promise<void> {
    if (this.ctx.getTags(ws).includes("grafana-live")) {
      const liveWs = this.#live2live.get(ws);
      this.#live2live.delete(ws);
      if (liveWs && liveWs.readyState === WebSocket.OPEN) {
        liveWs.close(1000, "Client disconnected");
      }
    }
  }

  ltxStore() {
    return new DOLTXStore(this.ctx);
  }

  litestream() {
    return Litestream.create(
      this.ctx,
      "/usr/local/bin/litestream",
      "/tmp/litestream.sock",
      "/etc/litestream.yaml",
    );
  }

  async scheduleScrapeMetrics() {
    if ((await this.listSchedules("scrapeMetrics")).length === 0) {
      await this.schedule(15, "scrapeMetrics");
    }
  }

  async stopScrapeMetrics() {
    this.deleteSchedules("scrapeMetrics");
  }

  async scrapeMetrics() {
    const container = this.ctx.container;
    if (!container || !container.running) {
      console.warn("Container is not running, skipping scrapeMetrics");
      return;
    }
    await this.schedule(15, "scrapeMetrics");

    const signal = AbortSignal.timeout(10 * 1000); // 10s

    const scrape = async (target: string, port: number) =>
      tracing.enterSpan("scrape metrics", async (span) => {
        span.setAttribute("scrape.target", target);
        const metrics = [];
        for await (const metric of scrapeMetrics("http://localhost/metrics", {
          fetcher: (req) => container.getTcpPort(port).fetch(req),
          signal,
        })) {
          metrics.push(metric);
        }
        span.setAttribute("metrics.count", metrics.length);
        return metrics;
      });

    const [grafanaMetrics, liteStreamMetrics] = await Promise.all([
      scrape("grafana", 3000),
      scrape("litestream", 9091),
    ]);

    const resouceAttributes = [
      ...environmentResourceAttributes(),
      ...attributes({
        [ATTR_SERVICE_INSTANCE_ID]: `${this.ctx.id.toString()}-${this.env.CF_VERSION_METADATA.id}`,
      }),
    ];

    const exportRequest = create(ExportMetricsServiceRequestSchema, {
      resourceMetrics: [
        {
          resource: {
            attributes: [...resouceAttributes, ...attributes({ app: "grafana" })],
          },
          scopeMetrics: [
            {
              metrics: grafanaMetrics,
            },
          ],
        },
        {
          resource: {
            attributes: [...resouceAttributes, ...attributes({ app: "litestream" })],
          },
          scopeMetrics: [
            {
              metrics: liteStreamMetrics,
            },
          ],
        },
      ],
    });

    const tasks = [];
    tasks.push(
      tracing.enterSpan("export metrics", async (span) => {
        span.setAttribute("export.target", "prometheus");

        const result = await exportMetricsAsOTLPHTTP(
          "http://localhost/api/v1/otlp/v1/metrics",
          exportRequest,
          {
            signal,
            fetcher: (req) => env.HOME_PROMETHEUS.fetch(req),
            compression: "gzip",
          },
        );

        span.setAttribute(
          "export.metrics.rejected",
          Number(result.partialSuccess?.rejectedDataPoints ?? 0n),
        );
        if (result.partialSuccess) {
          console.error("Failed to export: ", result.partialSuccess.errorMessage);
        }
      }),
    );
    if (env.MACKEREL_APIKEY) {
      const mackerelAPIKey = env.MACKEREL_APIKEY;
      tasks.push(
        tracing.enterSpan("export metrics", async (span) => {
          span.setAttribute("export.target", "mackerel");

          const client = createClient(
            MetricsService,
            createGrpcWebTransport({
              baseUrl: "https://otlp.mackerelio.com:4317",
              fetcher: (req) => env.GRPC.fetch(req),
              sendCompression: compressionGzip,
              acceptCompression: [compressionGzip],
            }),
          );

          const result = await client.export(exportRequest, {
            headers: {
              "Mackerel-Api-Key": mackerelAPIKey,
            },
          });

          span.setAttribute(
            "export.metrics.rejected",
            Number(result.partialSuccess?.rejectedDataPoints ?? 0n),
          );
          if (result.partialSuccess?.errorMessage) {
            console.error("Failed to export: ", result.partialSuccess.errorMessage);
          }
        }),
      );
    }
    await Promise.all(tasks);
  }
}

const webdav = webdavApp(() => grafana().ltxStore());
const workersAI = createWorkersAI({ binding: env.AI });
const ai = createAi(
  (modelId, { reasoningEffort }) => {
    const enableThinking = reasoningEffort != null && reasoningEffort != "none";
    return workersAI.chat(modelId, {
      reasoning_effort: enableThinking ? reasoningEffort : null,
      chat_template_kwargs: {
        enable_thinking: enableThinking,
      },
    });
  },
  async () => (await env.AI.models({ task: "Text Generation" })).map((model) => model.name),
);

Grafana.outboundByHost = {
  "replica.worker": async (req, env) => {
    req = decodeWebDAVMethod(req);
    const response = await webdav.fetch(req, env, {
      waitUntil,
      passThroughOnException() {},
      props: {},
    });
    return response;
  },
  "ai.worker": (req) => ai.fetch(req),
  "home-prometheus.worker": (req) => env.HOME_PROMETHEUS.fetch(req),
};
