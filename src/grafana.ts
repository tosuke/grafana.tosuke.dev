import { Container } from "@cloudflare/containers";
import { env, tracing } from "cloudflare:workers";
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
import * as z from "zod/mini";

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

  override async fetch(request: Request): Promise<Response> {
    const reqURL = new URL(request.url);
    if (request.method === "GET" && reqURL.pathname === "/api/live/ws") {
      return handleLiveRequest(this, this.ctx, request);
    } else if (request.method === "GET" && reqURL.pathname === "/api/frontend/assets") {
      return handleFrontendAssetsRequest((req) => super.fetch(req), this.ctx, request);
    } else {
      return super.fetch(request);
    }
  }

  async onStart() {
    console.log("Grafana started");

    for (const ws of this.ctx.getWebSockets("fake-live")) {
      ws.close(1000, "");
    }

    await this.scheduleScrapeMetrics();
  }

  async onStop() {
    console.log("Grafana stopped");
    await this.stopScrapeMetrics();
  }

  async webSocketMessage(ws: WebSocket, rawMessage: string | ArrayBuffer): Promise<void> {
    if (this.ctx.getTags(ws).includes(FAKE_LIVE_TAG)) {
      await handleFakeLiveMessage(this, ws, rawMessage);
    }
  }

  async pingFakeLive() {
    await handlePingFakeLive(this, this.ctx);
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
const ai = createAi(
  (modelId, { reasoningEffort }) => {
    const workersAI = createWorkersAI({ binding: env.AI });
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
    const response = await webdav.fetch(req, env);
    return response;
  },
  "ai.worker": (req) => ai.fetch(req),
  "home-prometheus.worker": (req) => env.HOME_PROMETHEUS.fetch(req),
};

const FAKE_LIVE_PING_INTERVAL_SECONDS = 120;
const FAKE_LIVE_TAG = "fake-live";

const FakeLiveSocketSchema = z.object({
  state: z.enum(["connecting", "connected", "ping-sent"]),
});

const LiveMessageSchema = z.union([
  z.object({
    id: z.number(),
    connect: z.object({
      name: z.string(),
    }),
  }),
  z.object({
    id: z.number(),
    subscribe: z.object({
      channel: z.string(),
    }),
  }),
  z.object({
    id: z.number(),
    unsubscribe: z.object({}),
  }),
  z.object({}),
]);

interface FakeLiveContainer extends Container {
  pingFakeLive(): Promise<void>;
}

async function handleLiveRequest(
  container: FakeLiveContainer,
  ctx: DurableObjectState,
  request: Request,
): Promise<Response> {
  const state = await container.getState();
  if (state.status === "healthy" || state.status === "running") {
    const port = container.defaultPort ?? 3000;
    await container.waitForPort({
      portToCheck: port,
      signal: request.signal,
    });
    const url = new URL(request.url);
    url.protocol = "http:";
    return (
      ctx.container?.getTcpPort(port).fetch(new Request(url, request)) ??
      new Response(null, { status: 503 })
    );
  } else {
    if (
      request.headers.get("Connection") !== "Upgrade" ||
      request.headers.get("Upgrade") !== "websocket"
    ) {
      return new Response(null, { status: 404 });
    }
    const { 0: client, 1: server } = new WebSocketPair();
    server.serializeAttachment({
      state: "connecting",
    });
    ctx.acceptWebSocket(server, [FAKE_LIVE_TAG]);
    if ((await container.listSchedules("pingFakeLive")).length === 0) {
      await container.schedule(FAKE_LIVE_PING_INTERVAL_SECONDS, "pingFakeLive");
    }
    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }
}

async function handlePingFakeLive(container: FakeLiveContainer, ctx: DurableObjectState) {
  try {
    const wss = ctx.getWebSockets(FAKE_LIVE_TAG);
    for (const ws of wss) {
      const socketState = FakeLiveSocketSchema.parse(ws.deserializeAttachment());
      switch (socketState.state) {
        case "connecting":
          break;
        case "connected":
          ws.send("{}");
          ws.serializeAttachment({ ...socketState, state: "ping-sent" });
          break;
        case "ping-sent":
          ws.close(1000, "");
          break;
        default:
          const _exhaustiveCheck: never = socketState.state;
          break;
      }
    }
  } finally {
    if (ctx.getWebSockets(FAKE_LIVE_TAG).length > 0) {
      await container.schedule(FAKE_LIVE_PING_INTERVAL_SECONDS, "pingFakeLive");
    }
  }
}

async function handleFakeLiveMessage(
  container: FakeLiveContainer,
  ws: WebSocket,
  rawMessage: string | ArrayBuffer,
) {
  if (typeof rawMessage !== "string") return;

  const socketStateParsed = FakeLiveSocketSchema.safeParse(ws.deserializeAttachment());
  if (!socketStateParsed.success) return;
  let socketState = socketStateParsed.data;

  let needContainer = false;
  for (const msg of rawMessage.split("\n")) {
    let json: unknown;
    try {
      json = JSON.parse(msg);
    } catch {
      json = {};
    }

    const messageParsed = LiveMessageSchema.safeParse(json);
    if (messageParsed.success) {
      const message = messageParsed.data;
      if ("connect" in message) {
        if (socketState.state === "connecting") {
          ws.send(
            JSON.stringify({
              id: message.id,
              connect: {
                client: crypto.randomUUID(),
                ping: 120,
                pong: true,
              },
            }),
          );
          socketState.state = "connected";
          continue;
        }
      } else if ("subscribe" in message) {
        if (/^[^/]+\/grafana\/dashboard\/uid\/[^/]+$/.test(message.subscribe.channel)) {
          ws.send(
            JSON.stringify({
              id: message.id,
              subscribe: {},
            }),
          );
          continue;
        }
      } else if ("unsubscribe" in message) {
        ws.send(
          JSON.stringify({
            id: message.id,
            unsubscribe: {},
          }),
        );
        continue;
      } else if (Object.keys(message).length === 0) /* {} */ {
        if (socketState.state === "ping-sent") {
          socketState.state = "connected";
        }
        continue;
      }
    }
    needContainer = true;
  }

  ws.serializeAttachment(socketState);
  if (needContainer) {
    const state = await container.getState();
    if (state.status !== "running") {
      await container.startAndWaitForPorts();
    }
  }
}

// GET /api/frontend/assets
const FrontendAssetsSchema = z.object({
  version: z.string(),
  content: z.unknown(),
});

async function handleFrontendAssetsRequest(
  fetcher: (req: Request) => Promise<Response>,
  ctx: DurableObjectState,
  request: Request,
): Promise<Response> {
  const assetsParsed = FrontendAssetsSchema.safeParse(ctx.storage.kv.get("frontend-assets"));
  if (assetsParsed.success && assetsParsed.data.version === env.CF_VERSION_METADATA.id) {
    return new Response(JSON.stringify(assetsParsed.data.content), {
      headers: {
        "Content-Type": "application/json",
      },
    });
  }
  const resp = await fetcher(request);
  if (resp.ok) {
    ctx.storage.kv.put("frontend-assets", {
      version: env.CF_VERSION_METADATA.id,
      content: await resp.clone().json(),
    });
  }
  return resp;
}
