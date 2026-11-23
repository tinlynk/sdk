import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Agent } from "../dist/agent.js";
import { ModelClient } from "../dist/modelClient.js";
import { Router } from "../dist/router.js";
import { ToolClient } from "../dist/toolClient.js";

class MultiTurnTransport {
  constructor() {
    this.turn = 0;
  }

  async *stream(req) {
    if (this.turn++ === 0) {
      yield { type: "token", data: "Checking." };
      yield { type: "tool_call", data: { name: "echo", arguments: { value: 7 }, callId: "c1" } };
      yield { type: "done" };
      return;
    }

    const result = req.messages.at(-1);
    assert.equal(result.role, "tool");
    assert.deepEqual(result.content, [{ type: "tool_result", callId: "c1", result: { echoed: { value: 7 } } }]);
    yield { type: "token", data: "Done." };
    yield { type: "done" };
  }
}

function createRequest() {
  return {
    model: "x",
    messages: [{ id: "u1", role: "user", content: [{ type: "text", text: "hi" }], timestamp: new Date().toISOString() }],
  };
}

test("agent executes tools and sends correlated results in a follow-up request", async () => {
  const transport = new MultiTurnTransport();
  const model = new ModelClient({ router: new Router(), transport });
  const tools = new ToolClient({
    invoke: async (call) => ({ callId: call.callId, result: { echoed: call.arguments } }),
  });
  const result = await new Agent({ model, tools, policy: { allowTools: ["echo"] } }).run(createRequest());
  assert.equal(result.text, "Done.");
  assert.equal(result.messages.length, 3);
  assert.deepEqual(result.messages[0].content, [
    { type: "text", text: "Checking." },
    { type: "tool_call", name: "echo", arguments: { value: 7 }, callId: "c1" },
  ]);
});

test("agent rejects tools outside its allow-list", async () => {
  const model = new ModelClient({
    router: new Router(),
    transport: new MultiTurnTransport(),
  });
  const agent = new Agent({
    model,
    tools: new ToolClient({ invoke: async (call) => ({ callId: call.callId, result: null }) }),
    policy: { allowTools: ["other"] },
  });
  await assert.rejects(agent.run(createRequest()), /not allowed/);
});
