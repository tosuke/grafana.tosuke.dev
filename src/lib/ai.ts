import {
  generateText,
  jsonSchema,
  type FilePart,
  type JSONSchema7,
  type LanguageModel,
  type ModelMessage,
  type ToolChoice,
  type ToolSet,
  streamText,
} from "ai";
import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { z } from "zod";

export const DEFAULT_AI_MODEL = "@cf/meta/llama-3.1-8b-instruct-fp8";

export type ReasoningEffort = "low" | "medium" | "high" | "none";
export type LanguageModelFactory = (
  modelId: string,
  options: { reasoningEffort?: ReasoningEffort | undefined },
) => LanguageModel;
export type ModelList = () => Promise<readonly string[]>;

const textContentPartSchema = z.object({
  type: z.literal("text"),
  text: z.string(),
});

const imageContentPartSchema = z.object({
  type: z.literal("image_url"),
  image_url: z.object({
    url: z.string(),
    detail: z.enum(["auto", "low", "high"]).optional(),
  }),
});

const toolCallSchema = z.object({
  id: z.string(),
  type: z.literal("function"),
  function: z.object({
    name: z.string(),
    arguments: z.string(),
  }),
});

const messageSchema = z.object({
  role: z.enum(["system", "user", "assistant", "tool"]),
  content: z
    .union([z.string(), z.array(z.union([textContentPartSchema, imageContentPartSchema]))])
    .nullable(),
  name: z.string().optional(),
  tool_call_id: z.string().optional(),
  tool_calls: z.array(toolCallSchema).optional(),
});

const toolSchema = z.object({
  type: z.literal("function"),
  function: z.object({
    name: z.string(),
    description: z.string().optional(),
    parameters: z.record(z.string(), z.unknown()).optional(),
    strict: z.boolean().optional(),
  }),
});

const toolChoiceSchema = z.union([
  z.enum(["none", "auto", "required"]),
  z.object({
    type: z.literal("function"),
    function: z.object({ name: z.string() }),
  }),
]);

const chatCompletionRequestSchema = z.object({
  model: z.string().optional(),
  messages: z.array(messageSchema).min(1),
  max_tokens: z.number().int().positive().optional(),
  max_completion_tokens: z.number().int().positive().optional(),
  temperature: z.number().min(0).optional(),
  top_p: z.number().min(0).max(1).optional(),
  stop: z
    .union([z.string(), z.array(z.string()).min(1)])
    .nullable()
    .optional(),
  presence_penalty: z.number().min(-2).max(2).optional(),
  frequency_penalty: z.number().min(-2).max(2).optional(),
  seed: z.number().int().optional(),
  reasoning_effort: z.enum(["low", "medium", "high", "none"]).optional(),
  tools: z.array(toolSchema).optional(),
  tool_choice: toolChoiceSchema.optional(),
  stream: z.boolean().optional(),
});

type ChatCompletionRequest = z.infer<typeof chatCompletionRequestSchema>;
type ChatCompletionTool = z.infer<typeof toolSchema>;
type ChatCompletionToolChoice = z.infer<typeof toolChoiceSchema>;

function parseToolArguments(argumentsText: string): unknown {
  try {
    return JSON.parse(argumentsText);
  } catch {
    return {};
  }
}

function imageMediaType(url: string): string {
  return url.match(/^data:([^;,]+)/)?.[1] ?? "image";
}

function toContent(
  content: ChatCompletionRequest["messages"][number]["content"],
): string | Array<{ type: "text"; text: string } | FilePart> {
  if (content === null || typeof content === "string") return content ?? "";

  return content.map((part) => {
    if (part.type === "text") return part;
    return {
      type: "file",
      data: new URL(part.image_url.url),
      mediaType: imageMediaType(part.image_url.url),
    } satisfies FilePart;
  });
}

function toText(content: ChatCompletionRequest["messages"][number]["content"]): string {
  const value = toContent(content);
  if (typeof value === "string") return value;
  return value.map((part) => (part.type === "text" ? part.text : part.data.toString())).join("\n");
}

function toModelMessages(messages: ChatCompletionRequest["messages"]): ModelMessage[] {
  type NonSystemMessage = Exclude<ChatCompletionRequest["messages"][number], { role: "system" }>;
  return messages
    .filter((message): message is NonSystemMessage => message.role !== "system")
    .map((message, index) => {
      switch (message.role) {
        case "user":
          return { role: "user", content: toContent(message.content) };
        case "assistant": {
          const content = toText(message.content);
          const toolCalls = (message.tool_calls ?? []).map((toolCall) => ({
            type: "tool-call" as const,
            toolCallId: toolCall.id,
            toolName: toolCall.function.name,
            input: parseToolArguments(toolCall.function.arguments),
          }));

          if (toolCalls.length === 0) return { role: "assistant", content };

          return {
            role: "assistant",
            content: [
              ...(content === "" ? [] : [{ type: "text" as const, text: content }]),
              ...toolCalls,
            ],
          };
        }
        case "tool":
          return {
            role: "tool",
            content: [
              {
                type: "tool-result",
                toolCallId: message.tool_call_id ?? `tool-call-${index}`,
                toolName: message.name ?? "tool",
                output: { type: "text", value: toText(message.content) },
              },
            ],
          };
        default:
          throw new Error(`Unsupported chat message role: ${message.role}`);
      }
    });
}

function toInstructions(messages: ChatCompletionRequest["messages"]): string | undefined {
  const instructions = messages
    .filter((message) => message.role === "system")
    .map((message) => toText(message.content))
    .filter((content) => content.length > 0)
    .join("\n\n");
  return instructions.length > 0 ? instructions : undefined;
}

function toOpenAIFinishReason(reason: string): "stop" | "length" | "content_filter" | "tool_calls" {
  switch (reason) {
    case "length":
      return "length";
    case "content-filter":
      return "content_filter";
    case "tool-calls":
      return "tool_calls";
    default:
      return "stop";
  }
}

function toTools(tools: ChatCompletionTool[]): ToolSet {
  return Object.fromEntries(
    tools.map(({ function: toolFunction }) => {
      const parameters = (toolFunction.parameters ?? {
        type: "object",
        properties: {},
      }) as JSONSchema7;
      return [
        toolFunction.name,
        {
          ...(toolFunction.description === undefined
            ? {}
            : { description: toolFunction.description }),
          inputSchema: jsonSchema(parameters),
          ...(toolFunction.strict === undefined ? {} : { strict: toolFunction.strict }),
        },
      ];
    }),
  ) as ToolSet;
}

function toToolChoice(choice: ChatCompletionToolChoice): ToolChoice<ToolSet> {
  if (typeof choice === "string") return choice;
  return { type: "tool", toolName: choice.function.name };
}

function toOpenAIToolCalls(
  toolCalls: ReadonlyArray<{ toolCallId: string; toolName: string; input: unknown }>,
) {
  return toolCalls.map((toolCall, index) => ({
    index,
    id: toolCall.toolCallId,
    type: "function" as const,
    function: {
      name: toolCall.toolName,
      arguments: JSON.stringify(toolCall.input) ?? "{}",
    },
  }));
}

export function createAi(
  createModel: LanguageModelFactory,
  listModels: ModelList = async () => [DEFAULT_AI_MODEL],
): Hono {
  const ai = new Hono();

  ai.get("/v1/models", async (c) => {
    const models = await listModels();
    return c.json({
      object: "list",
      data: models.map((modelId) => ({
        id: modelId,
        object: "model",
        created: 0,
        owned_by: "workers-ai",
      })),
    });
  });

  ai.post("/v1/chat/completions", async (c) => {
    const json: unknown = await c.req.json();
    console.log("Received chat completion request:", json);
    const parsed = chatCompletionRequestSchema.safeParse(json);
    if (!parsed.success) {
      return c.json(
        { error: { message: "Invalid chat completion request", type: "invalid_request_error" } },
        400,
      );
    }

    const {
      model: requestedModel,
      messages,
      max_tokens: maxTokens,
      max_completion_tokens: maxCompletionTokens,
      temperature,
      top_p: topP,
      stop,
      presence_penalty: presencePenalty,
      frequency_penalty: frequencyPenalty,
      seed,
      reasoning_effort: reasoningEffort,
      tools: requestedTools,
      tool_choice: requestedToolChoice,
      stream,
    } = parsed.data;
    const modelId = requestedModel ?? DEFAULT_AI_MODEL;
    const model = createModel(modelId, { reasoningEffort });
    const instructions = toInstructions(messages);
    const input = {
      model,
      ...(instructions === undefined ? {} : { instructions }),
      messages: toModelMessages(messages),
      ...(maxCompletionTokens != null || maxTokens != null
        ? { maxOutputTokens: maxCompletionTokens ?? maxTokens ?? -1 }
        : {}),
      ...(temperature == null ? {} : { temperature }),
      ...(topP == null ? {} : { topP }),
      ...(stop == null ? {} : { stopSequences: typeof stop === "string" ? [stop] : stop }),
      ...(presencePenalty == null ? {} : { presencePenalty }),
      ...(frequencyPenalty == null ? {} : { frequencyPenalty }),
      ...(seed == null ? {} : { seed }),
      tools: requestedTools == null ? {} : toTools(requestedTools),
      ...(requestedToolChoice !== null ? {} : { toolChoice: toToolChoice(requestedToolChoice) }),
    };

    if (stream === true) {
      const id = `chatcmpl-${crypto.randomUUID()}`;
      const created = Math.floor(Date.now() / 1000);
      const completion = streamText(input);
      c.header("Content-Type", "text/event-stream");
      c.header("Cache-Control", "no-cache");
      c.header("Connection", "keep-alive");
      return streamSSE(c, async (stream) => {
        let toolCallIndex = 0;
        await stream.writeSSE({
          data: JSON.stringify({
            id,
            object: "chat.completion.chunk",
            created,
            model: modelId,
            choices: [{ index: 0, delta: { role: "assistant", content: "" }, finish_reason: null }],
          }),
        });

        for await (const chunk of completion.stream) {
          if (chunk.type === "text-delta") {
            await stream.writeSSE({
              data: JSON.stringify({
                id,
                object: "chat.completion.chunk",
                created,
                model: modelId,
                choices: [{ index: 0, delta: { content: chunk.text }, finish_reason: null }],
              }),
            });
          } else if (chunk.type === "tool-call") {
            const toolCalls = toOpenAIToolCalls([chunk]);
            const toolCall = toolCalls[0];
            if (toolCall === undefined) continue;
            await stream.writeSSE({
              data: JSON.stringify({
                id,
                object: "chat.completion.chunk",
                created,
                model: modelId,
                choices: [
                  {
                    index: 0,
                    delta: { tool_calls: [{ ...toolCall, index: toolCallIndex++ }] },
                    finish_reason: null,
                  },
                ],
              }),
            });
          }
        }
        await stream.writeSSE({
          data: JSON.stringify({
            id,
            object: "chat.completion.chunk",
            created,
            model: modelId,
            choices: [
              {
                index: 0,
                finish_reason: toOpenAIFinishReason(await completion.finishReason),
              },
            ],
          }),
        });
        await stream.writeSSE({ data: "[DONE]" });
      });
    }

    try {
      const completion = await generateText(input);
      const toolCalls = toOpenAIToolCalls(completion.toolCalls);
      return c.json({
        id: `chatcmpl-${crypto.randomUUID()}`,
        object: "chat.completion",
        created: Math.floor(Date.now() / 1000),
        model: modelId,
        choices: [
          {
            index: 0,
            message: {
              role: "assistant",
              content: toolCalls.length > 0 ? completion.text || null : completion.text,
              ...(toolCalls.length > 0 ? { tool_calls: toolCalls } : {}),
            },
            finish_reason: toOpenAIFinishReason(completion.finishReason),
          },
        ],
      });
    } catch (error) {
      console.error("Language model request failed", error);
      return c.json(
        { error: { message: "Language model request failed", type: "server_error" } },
        502,
      );
    }
  });

  return ai;
}
