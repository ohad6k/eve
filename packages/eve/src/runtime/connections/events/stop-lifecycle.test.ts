import { AsyncLocalStorage } from "node:async_hooks";
import { beforeEach, expect, it, vi } from "vitest";

import { HookNotFoundError } from "#compiled/@workflow/errors/index.js";
import type { CompiledBundle } from "#runtime/sessions/runtime-context-keys.js";
import type { ConnectionEventPayload } from "./delivery.js";
import type { EventBinding } from "./state.js";

/**
 * One fake Workflow world shared by the watch's real run (workflow.ts) and the
 * real receiver routing (owner.ts): runs publish to streams, hooks end with
 * their run, and Connect's unsubscribe answer is held by the test.
 */
const world = vi.hoisted(() => {
  type Run = {
    published: unknown[];
    firstPublished: Promise<unknown>;
    publish: (v: unknown) => void;
  };
  type Hook = {
    token: string;
    runId: string;
    metadata: unknown;
    queue: unknown[];
    waiter?: () => void;
  };
  const runs = new Map<string, Run>();
  const hooks = new Map<string, Hook>();
  let runCount = 0;
  let releaseBackoff: (() => void) | undefined;
  return {
    runs,
    hooks,
    currentRun: undefined as unknown as { getStore(): string | undefined },
    deliveries: [] as string[],
    cleanup: "pending" as "pending" | "done",
    unsubscribes: 0,
    newRun(): string {
      const runId = `run-${++runCount}`;
      let publish!: (v: unknown) => void;
      const firstPublished = new Promise<unknown>((resolve) => (publish = resolve));
      runs.set(runId, {
        published: [],
        firstPublished,
        publish: (v) => {
          runs.get(runId)!.published.push(v);
          publish(v);
        },
      });
      return runId;
    },
    endRun(runId: string) {
      for (const [token, hook] of hooks) if (hook.runId === runId) hooks.delete(token);
    },
    /** The next unsubscribe backoff waits for this, so the test sees the stopping window. */
    backoffElapsed() {
      releaseBackoff?.();
    },
    sleep(until: Date | number) {
      if (typeof until !== "number") return new Promise<void>(() => {});
      return new Promise<void>((resolve) => (releaseBackoff = resolve));
    },
  };
});

vi.mock("#compiled/@workflow/core/index.js", () => ({
  createHook(options: { token: string; metadata?: unknown }) {
    const runId = world.currentRun.getStore()!;
    const previous = world.hooks.get(options.token);
    const hook = {
      token: options.token,
      runId,
      metadata: options.metadata,
      queue: [] as unknown[],
      waiter: undefined as (() => void) | undefined,
      async getConflict() {
        return previous === undefined ? null : { runId: previous.runId };
      },
      async dispose() {
        if (world.hooks.get(options.token) === hook) world.hooks.delete(options.token);
      },
      // oxlint-disable-next-line unicorn/no-thenable -- Workflow hooks are awaited for their first payload.
      then(resolve: (v: unknown) => unknown, reject: (e: unknown) => unknown) {
        return (async () => {
          while (hook.queue.length === 0) await new Promise<void>((r) => (hook.waiter = r));
          return hook.queue.shift();
        })().then(resolve, reject);
      },
    };
    if (previous === undefined) world.hooks.set(options.token, hook);
    return hook;
  },
  sleep: world.sleep,
}));

vi.mock("#internal/workflow/runtime.js", () => ({
  async getHookByToken(token: string) {
    const hook = world.hooks.get(token);
    if (hook === undefined) throw new HookNotFoundError(token);
    return { runId: hook.runId, metadata: Promise.resolve(hook.metadata) };
  },
  async resumeHook(token: string, value: unknown) {
    const hook = world.hooks.get(token);
    if (hook === undefined) throw new HookNotFoundError(token);
    hook.queue.push(value);
    const waiter = hook.waiter;
    hook.waiter = undefined;
    waiter?.();
    return {};
  },
  getRun(runId: string) {
    const run = world.runs.get(runId)!;
    return {
      getReadable: (options?: { startIndex?: number }) =>
        Object.assign(
          new ReadableStream({
            async pull(controller) {
              const index = options?.startIndex;
              controller.enqueue(
                index === undefined ? await run.firstPublished : run.published[index],
              );
              controller.close();
            },
          }),
          { getTailIndex: async () => run.published.length - 1 },
        ),
    };
  },
}));

vi.mock("#execution/workflow-runtime.js", () => ({
  connectionEventWorkflowReference: { workflowId: "watch" },
  connectionEventDeliveryWorkflowReference: { workflowId: "delivery" },
  async startWorkflowOnCurrentDeployment(reference: { workflowId: string }, [input]: [any]) {
    if (reference.workflowId === "delivery") {
      world.deliveries.push(input.payload.delivery.deliveryId);
      return { runId: "delivery-run" };
    }
    const { connectionEventWorkflow } = await import("./workflow.js");
    const runId = world.newRun();
    const storage = world.currentRun as AsyncLocalStorage<string>;
    void storage
      .run(runId, () => connectionEventWorkflow(input))
      .finally(() => world.endRun(runId));
    return { runId };
  },
}));

vi.mock("#runtime/connections/events/dispatch-step.js", () => ({
  dispatchConnectionEventStep: vi.fn(),
}));

vi.mock("#runtime/connections/events/lifecycle-step.js", () => {
  const publish = (outcome: unknown) =>
    world.runs.get(world.currentRun.getStore()!)!.publish(outcome);
  return {
    async openConnectionEventStep() {
      const outcome = {
        subscription: {
          id: "sub_1",
          name: "email_mailbox.message_received",
          arguments: {},
          status: "active",
          expiresAt: null,
        },
      };
      publish(outcome);
      return outcome;
    },
    async publishConnectionEventOutcomeStep(outcome: unknown) {
      publish(outcome);
    },
    async markConnectionEventStoppingStep() {
      publish({ stopping: true });
    },
    async unsubscribeConnectionEventStep() {
      world.unsubscribes++;
      return world.cleanup;
    },
    async startConnectionEventDeliveryStep(input: ConnectionEventPayload & { payload: any }) {
      world.deliveries.push(input.payload.delivery.deliveryId);
    },
  };
});

world.currentRun = new AsyncLocalStorage<string>();

const {
  findConnectionEventOwner,
  retireConnectionEventOwner,
  startConnectionEventDelivery,
  startConnectionEventOwner,
} = await import("./owner.js");

const binding = {
  id: "a".repeat(64),
  sessionId: "session-1",
  auth: null,
  connectionName: "bird",
  request: { name: "email_mailbox.message_received", arguments: {}, ttlMs: null },
} as unknown as EventBinding;
const bundle = {
  compiledArtifactsSource: { kind: "disk", appRoot: "/virtual/events" },
} as CompiledBundle;
const delivery = (deliveryId: string) =>
  ({
    kind: "connection-event",
    connectionName: "bird",
    bindingId: binding.id,
    delivery: { deliveryId, subscriptionId: "sub_1" },
  }) as ConnectionEventPayload;

/** The model stops the watch; Connect is still cleaning up upstream. */
async function stopWhileConnectCleansUp() {
  expect(await retireConnectionEventOwner(binding.id)).toBe(true);
  await vi.waitFor(() => expect(world.unsubscribes).toBe(1));
}

beforeEach(() => {
  world.runs.clear();
  world.hooks.clear();
  world.deliveries.length = 0;
  world.cleanup = "pending";
  world.unsubscribes = 0;
});

it("runs no callback for a delivery that arrives after the watch was stopped", async () => {
  expect(await startConnectionEventOwner(binding, bundle)).toMatchObject({ status: "active" });
  expect(await startConnectionEventDelivery(delivery("before-stop"), bundle)).toBe(true);

  await stopWhileConnectCleansUp();

  // The user said stop. A new email must not start another agent turn.
  expect(await startConnectionEventDelivery(delivery("after-stop"), bundle)).toBe(false);
  expect(world.deliveries).toEqual(["before-stop"]);
});

it("does not report a stopping watch as active when the same event is watched again", async () => {
  await startConnectionEventOwner(binding, bundle);
  await stopWhileConnectCleansUp();

  // "Actually, keep watching." The repeat lands on the watch that is being torn down.
  const again = await startConnectionEventOwner(binding, bundle);
  expect(again).toEqual({ stopping: true });

  // Connect finishes cleanup; the old run ends with its hook.
  world.cleanup = "done";
  world.backoffElapsed();
  await vi.waitFor(async () => expect(await findConnectionEventOwner(binding.id)).toBeUndefined());
});
