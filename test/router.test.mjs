import { strict as assert } from "node:assert";
import { test } from "node:test";
import { Router } from "../dist/router.js";

test("router falls back to direct when connect missing", async () => {
  const router = new Router({ strategy: "direct-only" });
  const decision = await router.route({ requestedModel: "openai:gpt-4.1-mini", messages: 1 });
  assert.equal(decision.model, "openai:gpt-4.1-mini");
  assert.equal(decision.metadata?.source, "direct");
});

test("router returns direct decisions while model selection is disabled", async () => {
  const router = new Router({ connectUrl: "http://fake-connect" });
  const decision = await router.route({ requestedModel: "gpt-5-mini", messages: 2 });
  assert.equal(decision.model, "gpt-5-mini");
  assert.equal(decision.metadata?.source, "direct");
});
