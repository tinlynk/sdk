import { strict as assert } from "node:assert";
import { test } from "node:test";
import { ToolClient } from "../dist/toolClient.js";

test("tool client registry call", async () => {
  const client = new ToolClient();
  client.register({ name: "echo", inputSchema: { type: "object" } });
  const result = await client.call({ name: "echo", callId: "1", arguments: { foo: "bar" } });
  assert.equal(result.callId, "1");
  assert.deepEqual(result.result, { echo: { foo: "bar" } });
});

test("tool client remote list and call", async (t) => {
  const fetchMock = async (url, init) => {
    if (init?.method === "GET") {
      return new Response(JSON.stringify([{ name: "remote.echo", inputSchema: { type: "object" } }]), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }
    const parsed = JSON.parse(init?.body || "{}");
    return new Response(JSON.stringify({ callId: parsed.callId, result: { ok: true } }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };
  const client = new ToolClient({ endpoint: "http://fake-tools", fetchImpl: fetchMock });
  const tools = await client.list();
  assert.equal(tools[0].name, "remote.echo");
  const result = await client.call({ name: "remote.echo", callId: "c1", arguments: {} });
  assert.equal(result.callId, "c1");
  assert.deepEqual(result.result, { ok: true });
});
