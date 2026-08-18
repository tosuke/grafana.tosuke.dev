import { describe, expect, it } from "vitest";
import { MockLanguageModelV4 } from "ai/test";
import { simulateReadableStream } from "ai";
import { createAi, DEFAULT_AI_MODEL } from "./ai";

const usage = {
  inputTokens: { total: 0, noCache: 0, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: 0, text: 0, reasoning: 0 },
};

function createModel(modelId = DEFAULT_AI_MODEL) {
  return new MockLanguageModelV4({
    provider: "test",
    modelId,
    doGenerate: {
      content: [{ type: "text", text: "Hello from the language model" }],
      finishReason: { unified: "stop", raw: undefined },
      usage,
      warnings: [],
    },
    doStream: {
      stream: simulateReadableStream({
        chunks: [
          { type: "text-start", id: "text-1" },
          { type: "text-delta", id: "text-1", delta: "Hello" },
          { type: "text-delta", id: "text-1", delta: " from the stream" },
          { type: "text-end", id: "text-1" },
          { type: "finish", finishReason: { unified: "stop", raw: undefined }, usage },
        ],
      }),
    },
  });
}

function createModelFactory() {
  return (modelId: string) => createModel(modelId);
}

describe("OpenAI-compatible chat completion API", () => {
  it("lists the injected model", async () => {
    const response = await createAi(createModelFactory(), async () => [
      DEFAULT_AI_MODEL,
      "test-model",
    ]).fetch(new Request("https://ai.worker/v1/models"));

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      data: [{ id: DEFAULT_AI_MODEL }, { id: "test-model" }],
    });
  });

  it("converts a chat completion to the OpenAI response format", async () => {
    const model = createModel();
    const response = await createAi(() => model).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: [{ role: "user", content: "Say hello" }],
        }),
      }),
    );

    expect(response.status).toBe(200);
    expect(model.doGenerateCalls[0]?.prompt).toMatchObject([
      { role: "user", content: [{ type: "text", text: "Say hello" }] },
    ]);
    await expect(response.json()).resolves.toMatchObject({
      object: "chat.completion",
      model: DEFAULT_AI_MODEL,
      choices: [
        {
          message: { role: "assistant", content: "Hello from the language model" },
          finish_reason: "stop",
        },
      ],
    });
  });

  it("includes reasoning content in a chat completion", async () => {
    const model = new MockLanguageModelV4({
      provider: "test",
      modelId: DEFAULT_AI_MODEL,
      doGenerate: {
        content: [
          { type: "reasoning", text: "The answer is straightforward. " },
          { type: "text", text: "Hello from the language model" },
        ],
        finishReason: { unified: "stop", raw: undefined },
        usage,
        warnings: [],
      },
    });
    const response = await createAi(() => model).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: [{ role: "user", content: "Say hello" }] }),
      }),
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      choices: [
        {
          message: {
            content: "Hello from the language model",
            reasoning_content: "The answer is straightforward. ",
          },
        },
      ],
    });
  });

  it("selects the requested model", async () => {
    const requestedModels: string[] = [];
    const response = await createAi((modelId) => {
      requestedModels.push(modelId);
      return createModel(modelId);
    }).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: "test-model",
          messages: [{ role: "user", content: "Say hello" }],
        }),
      }),
    );

    expect(requestedModels).toEqual(["test-model"]);
    await expect(response.json()).resolves.toMatchObject({
      model: "test-model",
      choices: [{ message: { content: "Hello from the language model" } }],
    });
  });

  it("converts system messages to instructions", async () => {
    const model = createModel();
    const response = await createAi(() => model).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: [
            { role: "system", content: "You are concise." },
            { role: "user", content: "Say hello" },
          ],
        }),
      }),
    );

    expect(response.status).toBe(200);
    expect(model.doGenerateCalls[0]).toMatchObject({
      prompt: [
        { role: "system", content: "You are concise." },
        { role: "user", content: [{ type: "text", text: "Say hello" }] },
      ],
    });
  });

  it("passes reasoning effort to the model factory", async () => {
    const options: Array<{ modelId: string; reasoningEffort?: string | null | undefined }> = [];
    const response = await createAi((modelId, modelOptions) => {
      options.push({ modelId, ...modelOptions });
      return createModel(modelId);
    }).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          reasoning_effort: "high",
          messages: [{ role: "user", content: "Think carefully" }],
        }),
      }),
    );

    expect(response.status).toBe(200);
    expect(options).toEqual([{ modelId: DEFAULT_AI_MODEL, reasoningEffort: "high" }]);
  });

  it("passes chat completion generation parameters to the language model", async () => {
    const model = createModel();
    const response = await createAi(() => model).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          max_tokens: 32,
          max_completion_tokens: 64,
          temperature: 0.4,
          top_p: 0.8,
          stop: ["END"],
          presence_penalty: 0.2,
          frequency_penalty: 0.3,
          seed: 42,
          messages: [{ role: "user", content: "Say hello" }],
        }),
      }),
    );

    expect(response.status).toBe(200);
    expect(model.doGenerateCalls[0]).toMatchObject({
      maxOutputTokens: 64,
      temperature: 0.4,
      topP: 0.8,
      stopSequences: ["END"],
      presencePenalty: 0.2,
      frequencyPenalty: 0.3,
      seed: 42,
    });
  });

  it("supports image inputs and tool calls", async () => {
    const model = new MockLanguageModelV4({
      provider: "test",
      modelId: DEFAULT_AI_MODEL,
      doGenerate: {
        content: [
          {
            type: "tool-call",
            toolCallId: "call_1",
            toolName: "get_weather",
            input: '{"city":"Tokyo"}',
          },
        ],
        finishReason: { unified: "tool-calls", raw: undefined },
        usage,
        warnings: [],
      },
    });
    const response = await createAi(() => model).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: [
            {
              role: "user",
              content: [
                { type: "text", text: "What is the weather?" },
                {
                  type: "image_url",
                  image_url: { url: "data:image/png;base64,iVBORw0KGgo=" },
                },
              ],
            },
          ],
          tools: [
            {
              type: "function",
              function: {
                name: "get_weather",
                description: "Get the weather for a city",
                parameters: {
                  type: "object",
                  properties: { city: { type: "string" } },
                  required: ["city"],
                },
              },
            },
          ],
          tool_choice: { type: "function", function: { name: "get_weather" } },
        }),
      }),
    );

    expect(response.status).toBe(200);
    expect(model.doGenerateCalls[0]?.prompt).toMatchObject([
      {
        role: "user",
        content: [
          { type: "text", text: "What is the weather?" },
          { type: "file", mediaType: "image/png" },
        ],
      },
    ]);
    expect(model.doGenerateCalls[0]?.tools).toHaveLength(1);
    await expect(response.json()).resolves.toMatchObject({
      choices: [
        {
          message: {
            content: null,
            tool_calls: [
              {
                id: "call_1",
                type: "function",
                function: { name: "get_weather", arguments: '{"city":"Tokyo"}' },
              },
            ],
          },
          finish_reason: "tool_calls",
        },
      ],
    });
  });

  it("converts streamed text to OpenAI SSE chunks", async () => {
    const response = await createAi(createModelFactory()).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: [{ role: "user", content: "Say hello" }],
          stream: true,
        }),
      }),
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/event-stream");
    const body = await response.text();
    expect(body).toContain('"content":"Hello"');
    expect(body).toContain('"content":" from the stream"');
    expect(body).toContain('"finish_reason":"stop"');
    expect(body).toContain("data: [DONE]");

    const events = body
      .split("\n\n")
      .filter((event) => event.startsWith("data: "))
      .map((event) => event.slice("data: ".length));
    expect(events.at(-1)).toBe("[DONE]");
    expect(JSON.parse(events.at(-2) ?? "{}")).toMatchObject({
      choices: [{ delta: {}, finish_reason: "stop" }],
    });
  });

  it("includes streamed reasoning content in OpenAI SSE chunks", async () => {
    const model = new MockLanguageModelV4({
      provider: "test",
      modelId: DEFAULT_AI_MODEL,
      doStream: {
        stream: simulateReadableStream({
          chunks: [
            { type: "reasoning-start", id: "reasoning-1" },
            { type: "reasoning-delta", id: "reasoning-1", delta: "Think first" },
            { type: "reasoning-end", id: "reasoning-1" },
            { type: "text-start", id: "text-1" },
            { type: "text-delta", id: "text-1", delta: "The answer" },
            { type: "text-end", id: "text-1" },
            { type: "finish", finishReason: { unified: "stop", raw: undefined }, usage },
          ],
        }),
      },
    });
    const response = await createAi(() => model).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          messages: [{ role: "user", content: "Think first" }],
          stream: true,
        }),
      }),
    );

    expect(response.status).toBe(200);
    expect(await response.text()).toContain('"reasoning_content":"Think first"');
  });

  it("rejects malformed requests", async () => {
    const response = await createAi(createModelFactory()).fetch(
      new Request("https://ai.worker/v1/chat/completions", {
        method: "POST",
        body: JSON.stringify({ messages: [] }),
      }),
    );

    expect(response.status).toBe(400);
  });
});
