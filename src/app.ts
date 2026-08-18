import { Hono } from "hono";
import { HTTPException } from "hono/http-exception";
import { grafana } from "./grafana";
import { createLitestreamApp } from "./lib/litestream";

export const app = new Hono();

const litestreamApp = createLitestreamApp(() => grafana().litestream());

app.route("/_litestream", litestreamApp);

app.all("*", async (c) => {
  return grafana().fetch(c.req.raw);
});

app.onError(async (err) => {
  if (err instanceof HTTPException) {
    return err.getResponse();
  }

  // To support source maps in stack traces, we re-throw the error so that Tail Workers can handle it and provide a proper stack trace.
  // FYI: https://developers.cloudflare.com/workers/observability/source-maps/
  throw err;
});
