import { Hono } from "hono";
import { grafana } from "./grafana";
import { createLitestreamApp } from "./lib/litestream";

export const app = new Hono();

const litestreamApp = createLitestreamApp(() => grafana().litestream());

app.route("/_litestream", litestreamApp);

app.all("*", async (c) => {
  return grafana().fetch(c.req.raw);
});
