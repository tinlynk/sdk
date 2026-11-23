// SPDX-License-Identifier: MIT
import { randomUUID } from "node:crypto";
import { type ChatRequest, type ContentPart, type Message, type ToolCall } from "@tinlynk/protocol";
import { appendText, ModelClient } from "./modelClient.js";
import { ToolClient } from "./toolClient.js";

const DEFAULT_MAX_TOOL_ROUNDS = 8;

export interface AgentOptions {
  model: ModelClient;
  tools?: ToolClient;
  policy?: { allowTools?: string[]; maxToolRounds?: number };
}

export interface AgentRunResult {
  messages: Message[];
  text: string;
}

export class Agent {
  private readonly model: ModelClient;
  private readonly tools?: ToolClient;
  private readonly allowTools: Set<string>;
  private readonly maxToolRounds: number;

  constructor(opts: AgentOptions) {
    this.model = opts.model;
    this.tools = opts.tools;
    this.allowTools = new Set(opts.policy?.allowTools ?? []);
    this.maxToolRounds = opts.policy?.maxToolRounds ?? DEFAULT_MAX_TOOL_ROUNDS;
  }

  async run(req: ChatRequest, signal?: AbortSignal): Promise<AgentRunResult> {
    const messages = req.messages.slice();
    const resultMessages: Message[] = [];
    let finalText = "";
    let toolRounds = 0;

    while (true) {
      signal?.throwIfAborted();
      const content: ContentPart[] = [];
      const calls: ToolCall[] = [];
      const turnRequest: ChatRequest = { ...req, messages };

      for await (const event of this.model.stream(turnRequest, signal)) {
        switch (event.type) {
          case "token":
            appendText(content, event.data);
            break;
          case "tool_call":
            calls.push(event.data);
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

      const assistantMessage: Message = {
        id: randomUUID(),
        role: "assistant",
        content,
        timestamp: new Date().toISOString(),
      };
      messages.push(assistantMessage);
      resultMessages.push(assistantMessage);
      finalText = content.filter((part) => part.type === "text").map((part) => part.text).join("");

      if (calls.length === 0) return { messages: resultMessages, text: finalText };
      if (!this.tools) throw new Error(`model requested tool ${calls[0].name} but no tool client is configured`);
      if (toolRounds >= this.maxToolRounds) throw new Error("agent exceeded the maximum tool-call rounds");

      for (const call of calls) {
        signal?.throwIfAborted();
        if (this.allowTools.size > 0 && !this.allowTools.has(call.name)) {
          throw new Error(`tool ${call.name} is not allowed by the agent policy`);
        }
        const result = await this.tools.call(call);
        if (result.callId !== call.callId) throw new Error(`tool ${call.name} returned a mismatched call id`);
        const toolMessage: Message = {
          id: randomUUID(),
          role: "tool",
          content: [{ type: "tool_result", callId: result.callId, result: result.result }],
          timestamp: new Date().toISOString(),
        };
        messages.push(toolMessage);
        resultMessages.push(toolMessage);
      }
      toolRounds++;
    }
  }
}
