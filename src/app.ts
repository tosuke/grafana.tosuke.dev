import { Hono } from "hono";
import { grafana } from "./grafana";

export const app = new Hono();

app.all("*", async (c) => {
  return grafana().fetch(c.req.raw);
});
