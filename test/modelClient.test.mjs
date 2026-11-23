import { strict as assert } from "node:assert";
import { test } from "node:test";
import { ModelClient } from "../dist/modelClient.js";
import { Router } from "../dist/router.js";

class FakeTransport {
  constructor(events) {
    this.events = events;
  }
  async *stream() {
    for (const event of this.events) yield event;
  }
}

const request = {
  model: "x",
  messages: [{ id: "1", role: "user", content: [{ type: "text", text: "hi" }], timestamp: new Date().toISOString() }],
};

test("model client uses its configured transport", async () => {
  const router = new Router({ strategy: "direct-only" });
  const transport = new FakeTransport([{ type: "token", data: "hello" }, { type: "done" }]);
  const client = new ModelClient({ router, transport });
  const events = [];
  for await (const event of client.stream(request)) events.push(event);
  assert.deepEqual(events, [{ type: "token", data: "hello" }, { type: "done" }]);
});

test("complete collects ordered response parts", async () => {
  const client = new ModelClient({
    router: new Router(),
    transport: new FakeTransport([
      { type: "token", data: "hello" },
      { type: "tool_call", data: { name: "echo", arguments: { x: 1 }, callId: "c1" } },
      { type: "done" },
    ]),
  });
  const result = await client.complete(request);
  assert.deepEqual(result.content, [
    { type: "text", text: "hello" },
    { type: "tool_call", name: "echo", arguments: { x: 1 }, callId: "c1" },
  ]);
});
