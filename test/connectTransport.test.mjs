import { strict as assert } from "node:assert";
import { test } from "node:test";
import { ConnectTransport, ModelClient, Router } from "../dist/index.js";

const request = {
  model: "gpt-alias",
  stream: true,
  messages: [{ id: "u1", role: "user", content: [{ type: "text", text: "Hello" }], timestamp: "2026-01-01T00:00:00Z" }],
};

test("Connect transport sends protocol version and parses SSE events", async () => {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('data: {"type":"token","data":"hello"}\n\n'));
      controller.enqueue(new TextEncoder().encode('data: {"type":"done"}\n\n'));
      controller.close();
    },
  });
  const fetchMock = async (url, init) => {
    assert.equal(url, "http://connect.test/v1/chat");
    assert.equal(init.headers.get("X-TinLynk-Protocol-Version"), "0.1.0");
    assert.equal(JSON.parse(init.body).model, "gpt-alias");
    return new Response(stream, { status: 200, headers: { "Content-Type": "text/event-stream", "X-TinLynk-Protocol-Version": "0.1.0" } });
  };
  const client = new ModelClient({
    router: new Router(),
    transport: new ConnectTransport({ baseUrl: "http://connect.test", fetchImpl: fetchMock }),
  });
  const events = [];
  for await (const event of client.stream(request)) events.push(event);
  assert.deepEqual(events, [{ type: "token", data: "hello" }, { type: "done" }]);
});

test("Connect transport converts non-stream messages to events", async () => {
  const fetchMock = async (_url, init) => {
    assert.equal(JSON.parse(init.body).stream, false);
    return new Response(JSON.stringify({
      id: "a1",
      role: "assistant",
      content: [{ type: "text", text: "hello" }, { type: "tool_call", callId: "c1", name: "echo", arguments: {} }],
      timestamp: "2026-01-01T00:00:00Z",
    }), { status: 200, headers: { "Content-Type": "application/json", "X-TinLynk-Protocol-Version": "0.1.0" } });
  };
  const client = new ModelClient({
    router: new Router(),
    transport: new ConnectTransport({ baseUrl: "http://connect.test", fetchImpl: fetchMock }),
  });
  const events = [];
  for await (const event of client.stream({ ...request, stream: false })) events.push(event);
  assert.deepEqual(events, [
    { type: "token", data: "hello" },
    { type: "tool_call", data: { callId: "c1", name: "echo", arguments: {} } },
    { type: "done" },
  ]);
});


test("Connect transport rejects a stream without a done event", async () => {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('data: {"type":"token","data":"partial"}\n\n'));
      controller.close();
    },
  });
  const client = new ModelClient({
    router: new Router(),
    transport: new ConnectTransport({ baseUrl: "http://connect.test", fetchImpl: async () => new Response(stream, { headers: { "X-TinLynk-Protocol-Version": "0.1.0" } }) }),
  });
  await assert.rejects(async () => {
    for await (const _event of client.stream(request)) {}
  }, /ended before a done event/);
});


test("Connect transport treats an error event as terminal", async () => {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode('data: {"type":"error","data":{"message":"upstream failed"}}\n\n'));
      controller.close();
    },
  });
  const client = new ModelClient({
    router: new Router(),
    transport: new ConnectTransport({ baseUrl: "http://connect.test", fetchImpl: async () => new Response(stream, { headers: { "X-TinLynk-Protocol-Version": "0.1.0" } }) }),
  });
  const events = [];
  for await (const event of client.stream(request)) events.push(event);
  assert.deepEqual(events, [{ type: "error", data: { message: "upstream failed" } }]);
});


test("Connect transport rejects a mismatched response protocol version", async () => {
  const client = new ModelClient({
    router: new Router(),
    transport: new ConnectTransport({
      baseUrl: "http://connect.test",
      fetchImpl: async () => new Response("{}", { headers: { "X-TinLynk-Protocol-Version": "99.0.0" } }),
    }),
  });
  await assert.rejects(async () => {
    for await (const _event of client.stream({ ...request, stream: false })) {}
  }, /unsupported protocol version/);
});

test("Connect transport forwards cancellation to fetch", async () => {
  const controller = new AbortController();
  const client = new ModelClient({
    router: new Router(),
    transport: new ConnectTransport({ baseUrl: "http://connect.test", fetchImpl: async (_url, init) => {
      assert.equal(init.signal, controller.signal);
      await new Promise((_resolve, reject) => init.signal.addEventListener("abort", () => reject(init.signal.reason), { once: true }));
    } }),
  });
  const result = client.complete(request, controller.signal);
  await new Promise((resolve) => setImmediate(resolve));
  controller.abort(new Error("cancelled"));
  await assert.rejects(result, /cancelled/);
});
