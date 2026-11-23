// SPDX-License-Identifier: MIT
import { randomUUID } from "node:crypto";
import { type ChatRequest, type ChatStreamEvent, type ContentPart, type Message } from "@tinlynk/protocol";
import { Router, type ModelDecision } from "./router.js";

export interface ModelTransport {
  stream(req: ChatRequest, decision: ModelDecision, signal?: AbortSignal): AsyncGenerator<ChatStreamEvent, void, unknown>;
}

export interface ModelClientOptions {
  router: Router;
  transport: ModelTransport;
}

export class ModelClient {
  private readonly router: Router;
  private readonly transport: ModelTransport;

  constructor(opts: ModelClientOptions) {
    this.router = opts.router;
    this.transport = opts.transport;
  }

  async *stream(req: ChatRequest, signal?: AbortSignal): AsyncGenerator<ChatStreamEvent, void, unknown> {
    const decision = await this.router.route({ requestedModel: req.model, messages: req.messages.length });
    yield* this.transport.stream(req, decision, signal);
  }

  async complete(req: ChatRequest, signal?: AbortSignal): Promise<Message> {
    const content: ContentPart[] = [];
    for await (const event of this.stream(req, signal)) {
      switch (event.type) {
        case "token":
          appendText(content, event.data);
          break;
        case "tool_call":
          content.push({ type: "tool_call", ...event.data });
          break;
        case "tool_result":
          content.push({ type: "tool_result", ...event.data });
          break;
        case "error":
          throw new Error(event.data.message);
        case "done":
          break;
      }
    }
    return { id: randomUUID(), role: "assistant", content, timestamp: new Date().toISOString() };
  }
}

export function appendText(content: ContentPart[], text: string): void {
  if (!text) return;
  const last = content.at(-1);
  if (last?.type === "text") last.text += text;
  else content.push({ type: "text", text });
}
