// SPDX-License-Identifier: MIT
import { type ToolCall, type ToolDescriptor } from "@tinlynk/protocol";

export interface ToolInvokeResult {
  callId: string;
  result: unknown;
}

export interface ToolClientOptions {
  endpoint?: string;
  fetchImpl?: typeof fetch;
  invoke?: (call: ToolCall) => Promise<ToolInvokeResult>;
}

export class ToolClient {
  private readonly endpoint?: string;
  private readonly fetchImpl: typeof fetch;
  private readonly invokeFn?: (call: ToolCall) => Promise<ToolInvokeResult>;
  private readonly registry: Map<string, ToolDescriptor> = new Map();

  constructor(opts: ToolClientOptions = {}) {
    this.endpoint = opts.endpoint;
    this.fetchImpl = opts.fetchImpl || fetch;
    this.invokeFn = opts.invoke;
  }

  register(descriptor: ToolDescriptor): void {
    this.registry.set(descriptor.name, descriptor);
  }

  async list(): Promise<ToolDescriptor[]> {
    if (this.endpoint) {
      const res = await this.fetchImpl(`${this.endpoint.replace(/\/$/, "")}/v1/tools`, { method: "GET" });
      if (!res.ok) throw new Error(`failed to list tools: ${res.status}`);
      const data = (await res.json()) as ToolDescriptor[];
      data.forEach((tool) => this.registry.set(tool.name, tool));
    }
    return Array.from(this.registry.values());
  }

  async call(call: ToolCall): Promise<ToolInvokeResult> {
    if (this.invokeFn) return this.invokeFn(call);
    if (this.endpoint) return this.callRemote(call);
    if (!this.registry.has(call.name)) {
      throw new Error(`unknown tool: ${call.name}`);
    }
    return { callId: call.callId, result: { echo: call.arguments } };
  }

  private async callRemote(call: ToolCall): Promise<ToolInvokeResult> {
    const res = await this.fetchImpl(`${this.endpoint!.replace(/\/$/, "")}/v1/tools/call`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(call),
    });
    if (!res.ok) throw new Error(`tool call failed: ${res.status}`);
    const data = (await res.json()) as ToolInvokeResult;
    return data;
  }
}
