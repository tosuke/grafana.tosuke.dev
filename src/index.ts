import { app } from "./app";
export { Grafana } from "./grafana";
export { ContainerProxy } from "@cloudflare/containers";
export default {
  fetch: app.fetch,
} satisfies ExportedHandler;
