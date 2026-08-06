import { describe, expect, it, vi } from "vitest";
import { withEnv } from "cloudflare:workers";
import { ai, DEFAULT_AI_MODEL } from "./ai";

describe("Workers AI OpenAI compatibility API", () => {
  it("lists the default model", async () => {
    const models = vi.fn(async () => [{ name: DEFAULT_AI_MODEL }]);
    const response = await withEnv({ AI: { models } }, () =>
      ai.fetch(new Request("https://ai.worker/v1/models")),
    );
    if (!(response instanceof Response)) throw new Error("Expected a Response");

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      data: [{ id: DEFAULT_AI_MODEL }],
    });
  });

  it("converts a chat completion to the OpenAI response format", async () => {
    const run = vi.fn(async (_model: string, _input: Record<string, unknown>) => ({
      response: "Hello from Workers AI",
    }));
    const response = await withEnv({ AI: { run } }, () =>
      ai.fetch(
        new Request("https://ai.worker/v1/chat/completions", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            messages: [{ role: "user", content: "Say hello" }],
          }),
        }),
      ),
    );
    if (!(response instanceof Response)) throw new Error("Expected a Response");

    expect(response.status).toBe(200);
    expect(run).toHaveBeenCalledWith(DEFAULT_AI_MODEL, {
      messages: [{ role: "user", content: "Say hello" }],
      reasoning_effort: "low",
    });
    await expect(response.json()).resolves.toMatchObject({
      object: "chat.completion",
      model: DEFAULT_AI_MODEL,
      choices: [
        {
          message: { role: "assistant", content: "Hello from Workers AI" },
          finish_reason: "stop",
        },
      ],
    });
  });

  it("rejects malformed requests", async () => {
    const response = await ai.fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        body: JSON.stringify({ messages: [] }),
      }),
    );

    expect(response.status).toBe(400);
  });
});
