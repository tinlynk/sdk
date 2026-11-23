// SPDX-License-Identifier: MIT

export interface ModelSelectionContext {
  requestedModel: string;
  messages: number;
  estimatedInputTokens?: number;
}

export interface ModelDecision {
  model: string;
  metadata?: Record<string, unknown>;
}

export interface RouterOptions {
  connectUrl?: string;
  strategy?: "direct-only";
  fetchImpl?: typeof fetch;
}

export class Router {
  constructor(opts: RouterOptions = {}) {
    void opts;
  }

  async route(ctx: ModelSelectionContext): Promise<ModelDecision> {
    return { model: ctx.requestedModel, metadata: { source: "direct" } };
  }
}
