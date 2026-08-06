import { env } from "cloudflare:workers";
import { Hono } from "hono";
import { z } from "zod";

export const DEFAULT_AI_MODEL = "@cf/meta/llama-3.1-8b-instruct-fp8";

const messageSchema = z.object({
  role: z.enum(["system", "user", "assistant", "tool"]),
  content: z.string(),
});

const aiCompletionSchema = z.object({
  response: z.string().optional(),
});

type NormalizedCompletion = {
  response: string;
};

function normalizeCompletion(value: unknown): NormalizedCompletion {
  const parsed = aiCompletionSchema.safeParse(value);
  if (!parsed.success) return { response: "" };
  return { response: parsed.data.response ?? "" };
}

const chatCompletionRequestSchema = z.object({
  model: z.string().optional(),
  messages: z.array(messageSchema).min(1),
  max_tokens: z.number().int().positive().optional(),
  temperature: z.number().min(0).optional(),
  stream: z.boolean().optional(),
});

export const ai = new Hono();

ai.get("/v1/models", async (c) => {
  const models = await env.AI.models({ task: "Text Generation" });
  return c.json({
    object: "list",
    data: models.map((m) => ({
      id: m.name,
      object: "model",
      created: 0,
      owned_by: "workers-ai",
    })),
  });
});

ai.post("/v1/chat/completions", async (c) => {
  const json = await c.req.json();
  console.log("Received chat completion request:", json);
  const parsed = chatCompletionRequestSchema.safeParse(json);
  if (!parsed.success) {
    return c.json(
      { error: { message: "Invalid chat completion request", type: "invalid_request_error" } },
      400,
    );
  }

  const envAI = env.AI as Ai<any> as Ai<{ [key: string]: BaseAiTextGeneration }>;
  const requestedModel = parsed.data.model;
  console.log("Requested model:", requestedModel);
  const model = parsed.data.model ?? DEFAULT_AI_MODEL;

  const { messages, max_tokens: maxTokens, temperature, stream } = parsed.data;
  const input = {
    messages,
    ...(maxTokens === undefined ? {} : { max_tokens: maxTokens }),
    ...(temperature === undefined ? {} : { temperature }),
    reasoning_effort: "low",
  };

  if (stream === true) {
    let completionStream: ReadableStream;
    try {
      completionStream = await envAI.run(model, { ...input, stream: true });
    } catch (error) {
      console.error("Workers AI streaming request failed", error);
      return c.json(
        { error: { message: "Workers AI streaming request failed", type: "server_error" } },
        502,
      );
    }
    return new Response(completionStream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      },
    });
  }

  const completion = await envAI.run(model, input);
  const normalized = normalizeCompletion(completion);

  return c.json({
    id: `chatcmpl-${crypto.randomUUID()}`,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [
      {
        index: 0,
        message: {
          role: "assistant",
          content: normalized.response,
        },
        finish_reason: "stop",
      },
    ],
  });
});
