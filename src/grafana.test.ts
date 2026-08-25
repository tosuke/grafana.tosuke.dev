import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import {
  handleFakeLiveMessage,
  handleFrontendAssetsRequest,
  handleLiveRequest,
  handlePingFakeLive,
} from "./grafana";

function fakeContainer(state: "healthy" | "running" | "stopped" = "stopped") {
  return {
    defaultPort: 3000,
    getState: vi.fn(async () => ({ status: state })),
    listSchedules: vi.fn(async () => []),
    schedule: vi.fn(async () => ({ id: "schedule" })),
    startAndWaitForPorts: vi.fn(async () => {}),
    waitForPort: vi.fn(async () => {}),
    pingFakeLive: vi.fn(async () => {}),
  };
}

function testDO() {
  return env.TEST.get(env.TEST.newUniqueId());
}

function acceptSocket(ctx: DurableObjectState, state: "connecting" | "connected" | "ping-sent") {
  const { 0: client, 1: server } = new WebSocketPair();
  server.serializeAttachment({ state });
  ctx.acceptWebSocket(server, ["fake-live"]);
  client.accept();
  return { client, server };
}

function nextMessage(ws: WebSocket): Promise<string> {
  return new Promise((resolve) => {
    ws.addEventListener("message", (event) => resolve(String(event.data)), { once: true });
  });
}

function nextMessages(ws: WebSocket, count: number): Promise<string[]> {
  return new Promise((resolve) => {
    const messages: string[] = [];
    ws.addEventListener("message", (event) => {
      messages.push(String(event.data));
      if (messages.length === count) resolve(messages);
    });
  });
}

describe("handleLiveRequest", () => {
  it("forwards requests to a healthy container", async () => {
    const container = fakeContainer("healthy");
    let forwardedRequest: Request | undefined;
    const fetch = vi.fn(async (request: Request) => {
      forwardedRequest = request;
      return new Response("live");
    });
    const request = new Request("https://grafana.example/api/live/ws");

    await runInDurableObject(testDO(), async (_instance, ctx) => {
      const getTcpPort = vi.fn(() => ({ fetch }));
      Object.defineProperty(ctx, "container", { value: { getTcpPort } });

      const response = await handleLiveRequest(container as never, ctx, request);

      expect(getTcpPort).toHaveBeenCalledWith(3000);
      await expect(response.text()).resolves.toBe("live");
    });

    expect(container.waitForPort).toHaveBeenCalledWith({
      portToCheck: 3000,
      signal: request.signal,
    });
    expect(fetch).toHaveBeenCalledOnce();
    expect(forwardedRequest).toBeDefined();
    expect(new URL(forwardedRequest?.url ?? "").protocol).toBe("http:");
  });

  it("rejects non-WebSocket requests while the container is stopped", async () => {
    await runInDurableObject(testDO(), async (_instance, ctx) => {
      const response = await handleLiveRequest(
        fakeContainer() as never,
        ctx,
        new Request("https://grafana.example/api/live/ws"),
      );

      expect(response.status).toBe(404);
    });
  });

  it("accepts a fallback WebSocket and schedules its ping", async () => {
    const container = fakeContainer();

    await runInDurableObject(testDO(), async (_instance, ctx) => {
      const response = await handleLiveRequest(
        container as never,
        ctx,
        new Request("https://grafana.example/api/live/ws", {
          headers: { Connection: "Upgrade", Upgrade: "websocket" },
        }),
      );

      expect(response.status).toBe(101);
      expect(response.webSocket).not.toBeNull();
      expect(ctx.getWebSockets("fake-live")).toHaveLength(1);
      expect(ctx.getWebSockets("fake-live")[0]?.deserializeAttachment()).toEqual({
        state: "connecting",
      });
    });
    expect(container.schedule).toHaveBeenCalledWith(120, "pingFakeLive");
  });
});

describe("handlePingFakeLive", () => {
  it("pings connected sockets, closes unanswered sockets, and reschedules", async () => {
    const container = fakeContainer();

    await runInDurableObject(testDO(), async (_instance, ctx) => {
      const connecting = acceptSocket(ctx, "connecting");
      const connected = acceptSocket(ctx, "connected");
      const pingSent = acceptSocket(ctx, "ping-sent");
      const ping = nextMessage(connected.client);
      const close = new Promise<CloseEvent>((resolve) =>
        pingSent.client.addEventListener("close", resolve, { once: true }),
      );

      await handlePingFakeLive(container as never, ctx);

      await expect(ping).resolves.toBe("{}");
      expect(connecting.server.deserializeAttachment()).toEqual({ state: "connecting" });
      expect(connected.server.deserializeAttachment()).toEqual({ state: "ping-sent" });
      await expect(close).resolves.toMatchObject({ code: 1000, reason: "" });
    });
    expect(container.schedule).toHaveBeenCalledWith(120, "pingFakeLive");
  });
});

describe("handleFakeLiveMessage", () => {
  it("implements connect, dashboard subscription, unsubscribe, and pong messages", async () => {
    const container = fakeContainer();
    const messages = [
      { id: 1, connect: { name: "client" } },
      { id: 2, subscribe: { channel: "scope/grafana/dashboard/uid/dashboard" } },
      { id: 3, unsubscribe: {} },
      {},
    ];

    await runInDurableObject(testDO(), async (_instance, ctx) => {
      const { client, server } = acceptSocket(ctx, "connecting");
      const responses = nextMessages(client, 3);

      await handleFakeLiveMessage(
        container as never,
        server,
        messages.map((message) => JSON.stringify(message)).join("\n"),
      );

      const [connectResponse, subscribeResponse, unsubscribeResponse] = await responses;
      expect(connectResponse).toBeDefined();
      expect(JSON.parse(connectResponse ?? "null")).toMatchObject({
        id: 1,
        connect: { ping: 120, pong: true },
      });
      expect(subscribeResponse).toBe(JSON.stringify({ id: 2, subscribe: {} }));
      expect(unsubscribeResponse).toBe(JSON.stringify({ id: 3, unsubscribe: {} }));
      expect(server.deserializeAttachment()).toEqual({ state: "connected" });
    });
    expect(container.startAndWaitForPorts).not.toHaveBeenCalled();
  });

  it("starts the container for an unhandled message", async () => {
    const container = fakeContainer();

    await runInDurableObject(testDO(), async (_instance, ctx) => {
      const { server } = acceptSocket(ctx, "connected");
      await handleFakeLiveMessage(container as never, server, "not-json");
    });

    expect(container.startAndWaitForPorts).toHaveBeenCalledOnce();
  });
});

describe("handleFrontendAssetsRequest", () => {
  it("serves assets cached for the current Worker version", async () => {
    const content = { plugins: ["cached"] };
    const fetcher = vi.fn(async () => new Response("uncached"));

    await runInDurableObject(testDO(), async (_instance, ctx) => {
      ctx.storage.kv.put("frontend-assets", {
        version: env.CF_VERSION_METADATA.id,
        content,
      });

      const response = await handleFrontendAssetsRequest(
        fetcher,
        ctx,
        new Request("https://grafana.example/api/frontend/assets"),
      );

      expect(fetcher).not.toHaveBeenCalled();
      expect(response.headers.get("Content-Type")).toBe("application/json");
      await expect(response.json()).resolves.toEqual(content);
    });
  });

  it("fetches and stores assets when the cache is stale", async () => {
    const content = { plugins: ["fresh"] };
    const fetcher = vi.fn(async () => Response.json(content));

    await runInDurableObject(testDO(), async (_instance, ctx) => {
      ctx.storage.kv.put("frontend-assets", {
        version: `${env.CF_VERSION_METADATA.id}-stale`,
        content: { plugins: ["stale"] },
      });

      const response = await handleFrontendAssetsRequest(
        fetcher,
        ctx,
        new Request("https://grafana.example/api/frontend/assets"),
      );

      expect(response.status).toBe(200);
      expect(fetcher).toHaveBeenCalledOnce();
      expect(ctx.storage.kv.get("frontend-assets")).toEqual({
        version: env.CF_VERSION_METADATA.id,
        content,
      });
    });
  });
});
