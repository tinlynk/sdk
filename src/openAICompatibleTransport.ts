// SPDX-License-Identifier: MIT
import { type ChatRequest, type ChatStreamEvent, type Message, type ToolCall } from "@tinlynk/protocol";
import { type ModelTransport } from "./modelClient.js";
import { type ModelDecision } from "./router.js";

export interface OpenAICompatibleTransportOptions {
  baseUrl: string;
  fetchImpl?: typeof fetch;
  headers?: HeadersInit;
}

interface OpenAIMessage {
  role: string;
  content: string | null;
  tool_call_id?: string;
  tool_calls?: Array<{
    id: string;
    type: "function";
    function: { name: string; arguments: string };
  }>;
}

interface StreamedToolCall {
  id: string;
  name: string;
  arguments: string;
}

export class OpenAICompatibleTransport implements ModelTransport {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly headers?: HeadersInit;

  constructor(options: OpenAICompatibleTransportOptions) {
    this.baseUrl = options.baseUrl.replace(/\/$/, "");
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.headers = options.headers;
  }

  async *stream(req: ChatRequest, decision: ModelDecision, signal?: AbortSignal): AsyncGenerator<ChatStreamEvent, void, unknown> {
    const stream = req.stream ?? true;
    const body = {
      model: decision.model,
      messages: toOpenAIMessages(req.messages),
      tools: req.tools?.map((tool) => ({
        type: "function",
        function: {
          name: tool.name,
          description: tool.description,
          parameters: tool.inputSchema,
        },
      })),
      stream,
      temperature: req.temperature,
    };
    const response = await this.fetchImpl(`${this.endpoint()}/chat/completions`, {
      method: "POST",
      headers: this.buildHeaders(),
      body: JSON.stringify(body),
      signal,
    });
    if (!response.ok) throw new Error(`chat completion failed: ${response.status}`);
    if (stream) {
      if (!response.body) throw new Error("chat completion returned no stream body");
      yield* parseChatCompletionStream(response.body);
      return;
    }

    const payload: unknown = await response.json();
    yield* readCompletionEvents(payload);
  }

  private endpoint(): string {
    return /\/v\d+$/.test(this.baseUrl) ? this.baseUrl : `${this.baseUrl}/v1`;
  }

  private buildHeaders(): Headers {
    const headers = new Headers(this.headers);
    headers.set("Content-Type", "application/json");
    return headers;
  }
}

function toOpenAIMessages(messages: Message[]): OpenAIMessage[] {
  const result: OpenAIMessage[] = [];
  for (const message of messages) {
    let text = "";
    let calls: NonNullable<OpenAIMessage["tool_calls"]> = [];
    const flush = () => {
      if (text || calls.length > 0) {
        result.push({ role: message.role, content: text || null, tool_calls: calls.length > 0 ? calls : undefined });
        text = "";
        calls = [];
      }
    };
    for (const part of message.content) {
      if (part.type === "text") {
        if (calls.length > 0 && part.text.length > 0) {
          throw new Error("OpenAI Chat Completions cannot preserve text after a tool call in one message");
        }
        text += part.text;
      } else if (part.type === "tool_call") {
        calls.push({
          id: part.callId,
          type: "function",
          function: { name: part.name, arguments: JSON.stringify(part.arguments) },
        });
      } else {
        flush();
        result.push({ role: "tool", content: serializeToolResult(part.result), tool_call_id: part.callId });
      }
    }
    flush();
    if (message.content.length === 0) result.push({ role: message.role, content: "" });
  }
  return result;
}

function serializeToolResult(result: unknown): string {
  if (typeof result === "string") return result;
  const serialized = JSON.stringify(result);
  if (serialized === undefined) throw new Error("tool result cannot be represented as JSON");
  return serialized;
}

async function* parseChatCompletionStream(stream: ReadableStream<Uint8Array>): AsyncGenerator<ChatStreamEvent, void, unknown> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  const toolCalls = new Map<number, StreamedToolCall>();
  let buffer = "";
  let finished = false;

  const handleData = (data: string): ChatStreamEvent[] => {
    if (data === "[DONE]") {
      finished = true;
      return [...completeToolCalls(toolCalls), { type: "done" }];
    }
    const payload: unknown = JSON.parse(data);
    if (!isRecord(payload) || !Array.isArray(payload.choices)) return [];
    const delta = isRecord(payload.choices[0]) && isRecord(payload.choices[0].delta) ? payload.choices[0].delta : undefined;
    if (!delta) return [];
    const events: ChatStreamEvent[] = [];
    if (typeof delta.content === "string" && delta.content.length > 0) {
      events.push({ type: "token", data: delta.content });
    }
    if (Array.isArray(delta.tool_calls)) {
      for (const item of delta.tool_calls) {
        if (!isRecord(item) || typeof item.index !== "number") {
          throw new Error("chat completion returned an invalid streamed tool call");
        }
        const call = toolCalls.get(item.index) ?? { id: "", name: "", arguments: "" };
        if (typeof item.id === "string") call.id += item.id;
        if (isRecord(item.function)) {
          if (typeof item.function.name === "string") call.name += item.function.name;
          if (typeof item.function.arguments === "string") call.arguments += item.function.arguments;
        }
        toolCalls.set(item.index, call);
      }
    }
    return events;
  };

  try {
    while (!finished) {
      const { done, value } = await reader.read();
      buffer += done ? decoder.decode() : decoder.decode(value, { stream: true });
      const lines = buffer.split(/\r?\n/);
      buffer = done ? "" : (lines.pop() ?? "");
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith("data:")) continue;
        for (const event of handleData(trimmed.slice("data:".length).trim())) yield event;
        if (finished) return;
      }
      if (done) break;
    }
  
    if (buffer.startsWith("data:")) {
      for (const event of handleData(buffer.slice("data:".length).trim())) yield event;
    }
    if (!finished) {
      throw new Error("chat completion stream ended before [DONE]");
    }
  } finally {
    await reader.cancel();
    reader.releaseLock();
  }
}

function* completeToolCalls(calls: Map<number, StreamedToolCall>): Generator<ChatStreamEvent> {
  for (const [, call] of [...calls.entries()].sort(([left], [right]) => left - right)) {
    if (!call.id || !call.name) throw new Error("chat completion returned an incomplete tool call");
    const args: unknown = JSON.parse(call.arguments || "{}");
    const toolCall: ToolCall = { callId: call.id, name: call.name, arguments: args };
    yield { type: "tool_call", data: toolCall };
  }
}

function* readCompletionEvents(payload: unknown): Generator<ChatStreamEvent> {
  if (!isRecord(payload) || !Array.isArray(payload.choices) || !isRecord(payload.choices[0])) {
    throw new Error("chat completion returned an invalid response");
  }
  const message = payload.choices[0].message;
  if (!isRecord(message)) throw new Error("chat completion returned no message");
  if (typeof message.content === "string" && message.content.length > 0) {
    yield { type: "token", data: message.content };
  }
  if (Array.isArray(message.tool_calls)) {
    for (const item of message.tool_calls) {
      if (!isRecord(item) || typeof item.id !== "string" || !isRecord(item.function) ||
          typeof item.function.name !== "string" || typeof item.function.arguments !== "string") {
        throw new Error("chat completion returned an invalid tool call");
      }
      const call: ToolCall = {
        callId: item.id,
        name: item.function.name,
        arguments: JSON.parse(item.function.arguments),
      };
      yield { type: "tool_call", data: call };
    }
  }
  const usage = isRecord(payload.usage) ? payload.usage : undefined;
  if (usage) yield { type: "done", data: { usage } };
  else yield { type: "done" };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
