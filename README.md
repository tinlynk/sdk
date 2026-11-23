# TinLynk SDK

Typed client utilities for model streaming, tool calls, and agent runs.

## Connect Transport

Start Connect, create a client token with `tlconnect auth create token-name`, and set `TINLYNK_CONNECT_TOKEN` to the displayed token before running the example. Use `ConnectTransport` for the canonical TinLynk protocol. `OpenAICompatibleTransport` is an interoperability option for Connect’s OpenAI-compatible Chat Completions facade. Keep app model traffic on Connect:

```ts
import { ModelClient, ConnectTransport, Router } from "@tinlynk/sdk";

const token = process.env.TINLYNK_CONNECT_TOKEN;
if (!token) throw new Error("TINLYNK_CONNECT_TOKEN is required");
const router = new Router({ strategy: "direct-only" });
const transport = new ConnectTransport({
  baseUrl: "http://127.0.0.1:8730",
  headers: { Authorization: `Bearer ${token}` }
});
const model = new ModelClient({ router, transport });

for await (const event of model.stream({
  model: "gpt-5-mini",
  stream: true,
  messages: [{ id: "u1", role: "user", content: [{ type: "text", text: "Hello" }], timestamp: new Date().toISOString() }]
})) {
  console.log(event);
}
```

See `../examples/sdk-connect-chat.mjs` for a runnable local example.

Pass an `AbortSignal` as the second argument to `model.stream`, `model.complete`, or `agent.run` to cancel an in-flight model request. The signal cannot interrupt an already running local tool handler.
