// SPDX-License-Identifier: MIT
import { PROTOCOL_VERSION, type ChatRequest, type ChatStreamEvent, type ContentPart, type Message } from "@tinlynk/protocol";
import { type ModelTransport } from "./modelClient.js";
import { type ModelDecision } from "./router.js";

export interface ConnectTransportOptions {
  baseUrl: string;
  fetchImpl?: typeof fetch;
  headers?: HeadersInit;
}

export class ConnectTransport implements ModelTransport {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly headers?: HeadersInit;

  constructor(options: ConnectTransportOptions) {
    this.baseUrl = options.baseUrl.replace(/\/$/, "");
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.headers = options.headers;
  }

  async *stream(req: ChatRequest, decision: ModelDecision, signal?: AbortSignal): AsyncGenerator<ChatStreamEvent, void, unknown> {
    const body = { ...req, model: decision.model, stream: req.stream ?? true };
    const response = await this.fetchImpl(`${this.endpoint()}/chat`, {
      method: "POST",
      headers: this.buildHeaders(),
      body: JSON.stringify(body),
      signal,
    });
    if (!response.ok) throw new Error(`TinLynk Connect chat failed: ${response.status}`);
    if (response.headers.get("X-TinLynk-Protocol-Version") !== PROTOCOL_VERSION) {
      throw new Error("TinLynk Connect returned an unsupported protocol version");
    }

    if (body.stream) {
      if (!response.body) throw new Error("TinLynk Connect returned no chat stream");
      yield* parseEventStream(response.body);
      return;
    }

    const payload: unknown = await response.json();
    if (!isMessage(payload) || payload.role !== "assistant") throw new Error("TinLynk Connect returned an invalid assistant message");
    for (const part of payload.content) yield* partToEvents(part);
    yield { type: "done" };
  }

  private endpoint(): string {
    return /\/v\d+$/.test(this.baseUrl) ? this.baseUrl : `${this.baseUrl}/v1`;
  }

  private buildHeaders(): Headers {
    const headers = new Headers(this.headers);
    headers.set("Content-Type", "application/json");
    headers.set("X-TinLynk-Protocol-Version", PROTOCOL_VERSION);
    return headers;
  }
}

async function* parseEventStream(stream: ReadableStream<Uint8Array>): AsyncGenerator<ChatStreamEvent, void, unknown> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let dataLines: string[] = [];

  const consumeLine = (line: string): ChatStreamEvent | undefined => {
    if (line === "") {
      if (dataLines.length === 0) return undefined;
      const data = dataLines.join("\n");
      dataLines = [];
      if (data === "[DONE]") return { type: "done" };
      const value: unknown = JSON.parse(data);
      if (!isChatStreamEvent(value)) throw new Error("TinLynk Connect returned an invalid stream event");
      return value;
    }
    if (line.startsWith("data:")) dataLines.push(line.slice("data:".length).trimStart());
    return undefined;
  };

  try {
    while (true) {
      const { done, value } = await reader.read();
      buffer += done ? decoder.decode() : decoder.decode(value, { stream: true });
      const lines = buffer.split(/\r?\n/);
      buffer = done ? "" : (lines.pop() ?? "");
      for (const line of lines) {
        const event = consumeLine(line);
        if (event) {
          yield event;
          if (event.type === "done" || event.type === "error") return;
        }
      }
      if (done) break;
    }
  
    if (buffer) {
      const event = consumeLine(buffer);
      if (event) {
        yield event;
        if (event.type === "done" || event.type === "error") return;
      }
    }
    if (dataLines.length > 0) {
      const event = consumeLine("");
      if (event) {
        yield event;
        if (event.type === "done" || event.type === "error") return;
      }
    }
    throw new Error("TinLynk Connect stream ended before a done event");
  } finally {
    await reader.cancel();
    reader.releaseLock();
  }
}

function partToEvents(part: ContentPart): ChatStreamEvent[] {
  switch (part.type) {
    case "text":
      return part.text ? [{ type: "token", data: part.text }] : [];
    case "tool_call": {
      const { name, arguments: args, callId } = part;
      return [{ type: "tool_call", data: { name, arguments: args, callId } }];
    }
    case "tool_result":
      return [{ type: "tool_result", data: { callId: part.callId, result: part.result } }];
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isContentPart(value: unknown): value is ContentPart {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  if (value.type === "text") return onlyKeys(value, ["type", "text"]) && typeof value.text === "string";
  if (value.type === "tool_call") {
    return onlyKeys(value, ["type", "callId", "name", "arguments"]) &&
      typeof value.callId === "string" && typeof value.name === "string" && "arguments" in value;
  }
  if (value.type === "tool_result") {
    return onlyKeys(value, ["type", "callId", "result", "isError"]) &&
      typeof value.callId === "string" && "result" in value &&
      (value.isError === undefined || typeof value.isError === "boolean");
  }
  return false;
}

function onlyKeys(value: Record<string, unknown>, allowed: string[]): boolean {
  return Object.keys(value).every((key) => allowed.includes(key));
}

function isMessage(value: unknown): value is Message {
  return isRecord(value) && typeof value.id === "string" &&
    ["system", "user", "assistant", "tool"].includes(String(value.role)) &&
    typeof value.timestamp === "string" && Array.isArray(value.content) &&
    value.content.every(isContentPart);
}

function isChatStreamEvent(value: unknown): value is ChatStreamEvent {
  if (!isRecord(value) || typeof value.type !== "string") return false;
  switch (value.type) {
    case "token": return typeof value.data === "string";
    case "tool_call":
      return isRecord(value.data) && typeof value.data.name === "string" &&
        typeof value.data.callId === "string" && "arguments" in value.data;
    case "tool_result":
      return isRecord(value.data) && typeof value.data.callId === "string" && "result" in value.data;
    case "done": return value.data === undefined || isRecord(value.data);
    case "error":
      return isRecord(value.data) && typeof value.data.message === "string";
    default: return false;
  }
}
