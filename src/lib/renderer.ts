import { env, tracing, waitUntil } from "cloudflare:workers";
import { Buffer } from "node:buffer";
import { Hono } from "hono";
import { zValidator } from "@hono/zod-validator";
import * as z from "zod/mini";
import { launch, type Page } from "@cloudflare/playwright";

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

        await using browser = await launch(env.BROWSER);
        await using page = await browser.newPage();

        const initPageTasks = [];
        initPageTasks.push(
          page.setViewportSize({
            width: viewPortWidth,
            height: viewPortHeight,
          }),
        );
        if (domain) {
          const cache = await caches.open("renderer-cache");
          initPageTasks.push(
            page.route(
              (url) => url.hostname === domain,
              (route) =>
                tracing.enterSpan("proxy renderer request", async (span) => {
                  const routeRequest = route.request();
                  span.setAttribute("http.method", routeRequest.method());
                  span.setAttribute("http.url", routeRequest.url());

                  const headers = new Headers(routeRequest.headers());
                  if (renderKey) {
                    headers.set("Cookie", `renderKey=${renderKey ?? ""}`);
                  }
                  const req = new Request(routeRequest.url(), {
                    method: routeRequest.method(),
                    headers: headers,
                    body: routeRequest.postDataBuffer(),
                  });

                  let resp = await cache.match(req);
                  if (!resp) {
                    resp = await fetcher(req);
                    if (resp.ok && req.method === "GET") {
                      waitUntil(cache.put(req, resp.clone()));
                    }
                  }
                  span.setAttribute("http.status_code", resp.status);

                  await route.fulfill({
                    status: resp.status,
                    contentType: resp.headers.get("content-type") ?? "application/octet-stream",
                    body: Buffer.from(await resp.arrayBuffer()),
                  });
                }),
            ),
          );
        }

        const { promise: renderMessage, resolve: resolveRenderMessage } = Promise.withResolvers();
        initPageTasks.push(
          page.exposeBinding("__grafanaImageRendererMessageChannel", (_source, msg) => {
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
              throw new Error(`Timed out waiting for page readiness after ${RENDER_TIMEOUT_MS}ms`);
            }),
          ]);
        } else {
          await page.waitForLoadState("networkidle", {
            timeout: RENDER_TIMEOUT_MS,
          });
        }

        if (requireFullPage) {
          const scrollHeight = await page.evaluate(() => {
            const { document } = globalThis as unknown as BrowserPageGlobals;
            return document.body.scrollHeight;
          });
          if (scrollHeight > viewPortHeight) {
            await page.setViewportSize({
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
      }),
  );

  app.get("render/csv", (c) => {
    return c.text("CSV rendering is not implemented yet", 501);
  });

  return app;
}

async function scrollWholePage(page: Page): Promise<void> {
  await page.waitForTimeout(SCROLL_WAIT_MS);
  const numScrolls = await page.evaluate(() => {
    const { document, window } = globalThis as unknown as BrowserPageGlobals;
    return Math.floor(document.body.scrollHeight / window.innerHeight);
  });
  for (let i = 0; i < numScrolls; i++) {
    await page.evaluate(() => {
      const { window } = globalThis as unknown as BrowserPageGlobals;
      window.scrollBy(0, window.innerHeight);
    });
    await page.waitForTimeout(SCROLL_WAIT_MS);
  }
  await page.evaluate(() => {
    const { window } = globalThis as unknown as BrowserPageGlobals;
    window.scrollTo(0, 0);
  });
}
