export {};

declare module "hono" {
  interface ExecutionContext {
    cache?: globalThis.ExecutionContext["cache"];
    readonly access?: globalThis.ExecutionContext["access"];
    tracing: globalThis.ExecutionContext["tracing"];
    abort: globalThis.ExecutionContext["abort"];
  }
}
