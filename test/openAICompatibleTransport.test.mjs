import { strict as assert } from "node:assert";
import { test } from "node:test";
import { OpenAICompatibleTransport, Router, ModelClient } from "../dist/index.js";

test("openai compatible transport reads non-streaming chat completions", async () => {
  const fetchMock = async (url, init) => {
    assert.equal(url, "http://connect.test/v1/chat/completions");
    assert.equal(init.method, "POST");
    const body = JSON.parse(init.body);
    assert.equal(body.model, "openai:test-model");
    assert.equal(body.stream, false);
    return new Response(
      JSON.stringify({
        choices: [{ message: { role: "assistant", content: "hello" } }],
        usage: { total_tokens: 2 },
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  };
  const router = new Router({ strategy: "direct-only" });
  const transport = new OpenAICompatibleTransport({ baseUrl: "http://connect.test", fetchImpl: fetchMock });
  const client = new ModelClient({ router, transport });
  const events = [];
  for await (const event of client.stream({
    model: "openai:test-model",
    stream: false,
    messages: [{ id: "1", role: "user", content: [{ type: "text", text: "hi" }], timestamp: new Date().toISOString() }],
  })) {
    events.push(event);
  }
  assert.deepEqual(events, [
    { type: "token", data: "hello" },
    { type: "done", data: { usage: { total_tokens: 2 } } },
  ]);
});

test("openai compatible transport parses streaming chat completions", async () => {
  const stream = new ReadableStream({
    start(controller) {
      const encoder = new TextEncoder();
      controller.enqueue(encoder.encode('data: {"choices":[{"delta":{"content":"hel"}}]}\n\n'));
      controller.enqueue(encoder.encode('data: {"choices":[{"delta":{"content":"lo"}}]}\n\n'));
      controller.enqueue(encoder.encode("data: [DONE]\n\n"));
      controller.close();
    },
  });
  const fetchMock = async () => new Response(stream, { status: 200, headers: { "Content-Type": "text/event-stream" } });
  const router = new Router({ strategy: "direct-only" });
  const transport = new OpenAICompatibleTransport({ baseUrl: "http://connect.test", fetchImpl: fetchMock });
  const client = new ModelClient({ router, transport });
  const events = [];
  for await (const event of client.stream({
    model: "openai:test-model",
    stream: true,
    messages: [{ id: "1", role: "user", content: [{ type: "text", text: "hi" }], timestamp: new Date().toISOString() }],
  })) {
    events.push(event);
  }
  assert.deepEqual(events, [
    { type: "token", data: "hel" },
    { type: "token", data: "lo" },
    { type: "done" },
  ]);
});


test("openai compatible transport maps protocol tool calls and results", async () => {
  const fetchMock = async (_url, init) => {
    const body = JSON.parse(init.body);
    assert.deepEqual(body.messages, [
      { role: "assistant", content: null, tool_calls: [
        { id: "c1", type: "function", function: { name: "echo", arguments: '{"value":7}' } },
        { id: "c1b", type: "function", function: { name: "echo", arguments: '{"value":8}' } },
      ] },
      { role: "tool", content: '{"echoed":7}', tool_call_id: "c1" },
      { role: "tool", content: '{"echoed":8}', tool_call_id: "c1b" },
    ]);
    return new Response(JSON.stringify({
      choices: [{ message: { role: "assistant", content: null, tool_calls: [{
        id: "c2", type: "function", function: { name: "echo", arguments: "{\"value\":8}" },
      }] } }],
    }), { status: 200, headers: { "Content-Type": "application/json" } });
  };
  const client = new ModelClient({
    router: new Router({ strategy: "direct-only" }),
    transport: new OpenAICompatibleTransport({ baseUrl: "http://connect.test/v1", fetchImpl: fetchMock }),
  });
  const events = [];
  for await (const event of client.stream({
    model: "openai:test-model",
    stream: false,
    messages: [
      { id: "a1", role: "assistant", content: [
        { type: "tool_call", callId: "c1", name: "echo", arguments: { value: 7 } },
        { type: "tool_call", callId: "c1b", name: "echo", arguments: { value: 8 } },
      ], timestamp: "2026-01-01T00:00:00Z" },
      { id: "t1", role: "tool", content: [{ type: "tool_result", callId: "c1", result: { echoed: 7 } }], timestamp: "2026-01-01T00:00:01Z" },
      { id: "t2", role: "tool", content: [{ type: "tool_result", callId: "c1b", result: { echoed: 8 } }], timestamp: "2026-01-01T00:00:02Z" },
    ],
  })) events.push(event);
  assert.deepEqual(events, [
    { type: "tool_call", data: { callId: "c2", name: "echo", arguments: { value: 8 } } },
    { type: "done" },
  ]);
});

test("openai compatible transport rejects a truncated SSE response", async () => {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'));
      controller.close();
    },
  });
  const client = new ModelClient({
    router: new Router({ strategy: "direct-only" }),
    transport: new OpenAICompatibleTransport({
      baseUrl: "http://connect.test",
      fetchImpl: async () => new Response(stream, { status: 200 }),
    }),
  });
  await assert.rejects(async () => {
    for await (const _event of client.stream({
      model: "openai:test-model",
      stream: true,
      messages: [{ id: "1", role: "user", content: [{ type: "text", text: "hi" }], timestamp: "2026-01-01T00:00:00Z" }],
    })) {}
  }, /ended before \[DONE\]/);
});


test("openai compatible transport rejects text after a tool call in one message", async () => {
  const client = new ModelClient({
    router: new Router({ strategy: "direct-only" }),
    transport: new OpenAICompatibleTransport({
      baseUrl: "http://connect.test",
      fetchImpl: async () => new Response(JSON.stringify({ choices: [{ message: { role: "assistant", content: "ok" } }] }), { status: 200 }),
    }),
  });
  await assert.rejects(async () => {
    for await (const _event of client.stream({
      model: "openai:test-model",
      stream: false,
      messages: [{ id: "a1", role: "assistant", content: [
        { type: "tool_call", callId: "c1", name: "echo", arguments: {} },
        { type: "text", text: "after" },
      ], timestamp: "2026-01-01T00:00:00Z" }],
    })) {}
  }, /cannot preserve text after a tool call/);
});


test("openai compatible transport accumulates streamed tool-call deltas", async () => {
  const stream = new ReadableStream({
    start(controller) {
      const encoder = new TextEncoder();
      const first = { choices: [{ delta: { tool_calls: [{ index: 0, id: "call_", type: "function", function: { name: "get_", arguments: '{"x":' } }] } }] };
      const second = { choices: [{ delta: { tool_calls: [{ index: 0, id: "1", function: { name: "data", arguments: "1}" } }] } }] };
      controller.enqueue(encoder.encode("data: " + JSON.stringify(first) + "\n\n"));
      controller.enqueue(encoder.encode("data: " + JSON.stringify(second) + "\n\n"));
      controller.enqueue(encoder.encode("data: [DONE]\n\n"));
      controller.close();
    },
  });
  const client = new ModelClient({
    router: new Router({ strategy: "direct-only" }),
    transport: new OpenAICompatibleTransport({
      baseUrl: "http://connect.test",
      fetchImpl: async (_url, init) => {
        const body = JSON.parse(init.body);
        assert.deepEqual(body.tools, [{ type: "function", function: {
          name: "echo", description: "Echo input", parameters: { type: "object" },
        } }]);
        return new Response(stream, { status: 200 });
      },
    }),
  });
  const events = [];
  for await (const event of client.stream({
    model: "openai:test-model",
    stream: true,
    tools: [{ name: "echo", description: "Echo input", inputSchema: { type: "object" } }],
    messages: [{ id: "1", role: "user", content: [{ type: "text", text: "hi" }], timestamp: "2026-01-01T00:00:00Z" }],
  })) events.push(event);
  assert.deepEqual(events, [
    { type: "tool_call", data: { callId: "call_1", name: "get_data", arguments: { x: 1 } } },
    { type: "done" },
  ]);
});
