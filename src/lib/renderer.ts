import { env, tracing, waitUntil } from "cloudflare:workers";
import { Buffer } from "node:buffer";
import { Hono } from "hono";
import { zValidator } from "@hono/zod-validator";
import * as z from "zod/mini";
import { type Page } from "@cloudflare/puppeteer";

const MIN_WIDTH = 1000;
const MIN_HEIGHT = 500;
const MAX_WIDTH = 3000;
const MAX_HEIGHT = 3000;
const RENDER_TIMEOUT_MS = 30_000;
const SCROLL_WAIT_MS = 50;

interface BrowserPageGlobals {
  document: {
    body: { scrollHeight: number; toString(): string };
  };
  window: {
    innerHeight: number;
    scrollY: number;
    scrollBy(x: number, y: number): void;
    scrollTo(x: number, y: number): void;
    __grafanaRenderBindingSupported?: boolean;
  };
}

export function createRendererApp(fetcher: (req: Request) => Promise<Response> = fetch): Hono {
  const app = new Hono();

  app.use("*", async (c, next) => {
    await next();
    console.log("render", c.req.method, c.req.url, c.res.status);
  });

  app.get("/render/version", (c) => {
    return c.json({ version: "5.0.0" });
  });

  app.get(
    "/render",
    zValidator(
      "query",
      z.object({
        url: z.string().check(z.url()),
        width: z.optional(z.coerce.number().check(z.int())),
        height: z.optional(z.coerce.number().check(z.int())),
        timeout: z.optional(z.coerce.number().check(z.int(), z.positive())),
        deviceScaleFactor: z.optional(z.coerce.number().check(z.positive())),
        domain: z.optional(z.string()),
        renderKey: z.optional(z.string()),
        encoding: z.optional(z.enum(["png", "pdf"])),
      }),
    ),
    async (c) =>
      tracing.enterSpan("render", async (span) => {
        const { url, domain, renderKey, width, height, encoding = "png" } = c.req.valid("query");
        span.setAttributes({
          "render.url": url,
        });

        const viewPortWidth =
          width == null ? MIN_WIDTH : Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, width));
        const requireFullPage = height === -1;
        const viewPortHeight =
          height == null || requireFullPage
            ? Math.floor(viewPortWidth * 0.75)
            : Math.max(MIN_HEIGHT, Math.min(MAX_HEIGHT, height));

        if (encoding === "pdf") {
          return c.text("PDF rendering is not implemented yet", 501);
        }

        const { default: puppeteer } = await import("@cloudflare/puppeteer");

        const browser = await puppeteer.launch(env.BROWSER);
        try {
          const page = await browser.newPage();

          const initPageTasks = [];
          initPageTasks.push(
            page.setViewport({
              width: viewPortWidth,
              height: viewPortHeight,
            }),
          );
          if (domain) {
            const cache = await caches.open("renderer-cache");
            await page.setRequestInterception(true);
            page.on("request", (request) => {
              void tracing
                .enterSpan("proxy renderer request", async (span) => {
                  const requestUrl = new URL(request.url());
                  if (requestUrl.hostname !== domain) {
                    await request.continue();
                    return;
                  }

                  span.setAttribute("http.method", request.method());
                  span.setAttribute("http.url", request.url());

                  const headers = new Headers(request.headers());
                  if (renderKey) {
                    headers.set("Cookie", `renderKey=${renderKey ?? ""}`);
                  }
                  const req = new Request(request.url(), {
                    method: request.method(),
                    headers: headers,
                    body: request.postData() ?? null,
                  });

                  let resp = await cache.match(req);
                  if (!resp) {
                    resp = await fetcher(req);
                    if (resp.ok && req.method === "GET") {
                      waitUntil(cache.put(req, resp.clone()));
                    }
                  }
                  span.setAttribute("http.status_code", resp.status);

                  await request.respond({
                    status: resp.status,
                    contentType: resp.headers.get("content-type") ?? "application/octet-stream",
                    body: Buffer.from(await resp.arrayBuffer()),
                  });
                })
                .catch(async (error: unknown) => {
                  console.error("Failed to proxy renderer request", error);
                  if (!request.isInterceptResolutionHandled()) {
                    await request.abort();
                  }
                });
            });
          }

          const { promise: renderMessage, resolve: resolveRenderMessage } =
            Promise.withResolvers<unknown>();
          initPageTasks.push(
            page.exposeFunction("__grafanaImageRendererMessageChannel", (msg: unknown) => {
              resolveRenderMessage(msg);
            }),
          );

          await Promise.all(initPageTasks);

          await page.goto(url, {
            timeout: RENDER_TIMEOUT_MS,
          });
          await scrollWholePage(page);
          const supportsBinding = await page.evaluate(() => {
            const globals = globalThis as unknown as BrowserPageGlobals;
            return globals.window.__grafanaRenderBindingSupported === true;
          });
          if (supportsBinding) {
            await Promise.race([
              renderMessage,
              scheduler.wait(RENDER_TIMEOUT_MS).then(() => {
                throw new Error(
                  `Timed out waiting for page readiness after ${RENDER_TIMEOUT_MS}ms`,
                );
              }),
            ]);
          } else {
            await page.waitForNetworkIdle({
              timeout: RENDER_TIMEOUT_MS,
            });
          }

          if (requireFullPage) {
            const scrollHeight = await page.evaluate(() => {
              const { document } = globalThis as unknown as BrowserPageGlobals;
              return document.body.scrollHeight;
            });
            if (scrollHeight > viewPortHeight) {
              await page.setViewport({
                width: viewPortWidth,
                height: scrollHeight,
              });
            }
          }

          const buffer = await page.screenshot({
            type: "png",
          });

          return c.body(new Uint8Array(buffer), 200, {
            "Content-Type": "image/png",
          });
        } finally {
          await browser.close();
        }
      }),
  );

  app.get("render/csv", (c) => {
    return c.text("CSV rendering is not implemented yet", 501);
  });

  return app;
}

async function scrollWholePage(page: Page): Promise<void> {
  await scheduler.wait(SCROLL_WAIT_MS);
  const numScrolls = await page.evaluate(() => {
    const { document, window } = globalThis as unknown as BrowserPageGlobals;
    return Math.floor(document.body.scrollHeight / window.innerHeight);
  });
  for (let i = 0; i < numScrolls; i++) {
    await page.evaluate(() => {
      const { window } = globalThis as unknown as BrowserPageGlobals;
      window.scrollBy(0, window.innerHeight);
    });
    await scheduler.wait(SCROLL_WAIT_MS);
  }
  await page.evaluate(() => {
    const { window } = globalThis as unknown as BrowserPageGlobals;
    window.scrollTo(0, 0);
  });
}
