// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EventId, ProviderInstanceId, ThreadId, TurnId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import {
  makeOmpAdapter,
  mapOmpSessionUpdate,
  ompModelsFromConfig,
  ompPermissionOptionId,
} from "../Layers/OmpAdapter.ts";
import { execScriptSource, writeFakeCli } from "../../testUtils/fakeCli.ts";

const instanceId = ProviderInstanceId.make("omp-test-instance");
const mockAgentPath = NodePath.join(
  NodePath.dirname(NodeURL.fileURLToPath(import.meta.url)),
  "../../../scripts/acp-mock-agent.ts",
);
const testLayer = NodeServices.layer;

it("maps omp ACP assistant chunks to canonical content events", () => {
  const event = mapOmpSessionUpdate({
    threadId: ThreadId.make("thread-omp"),
    turnId: TurnId.make("turn-omp"),
    eventId: EventId.make("event-omp"),
    createdAt: "2026-09-19T00:00:00.000Z",
    update: {
      sessionUpdate: "agent_message_chunk",
      content: { type: "text", text: "hello from omp" },
    },
  });

  expect(event).toMatchObject({
    type: "content.delta",
    provider: "omp",
    payload: { streamKind: "assistant_text", delta: "hello from omp" },
  });
});

it("maps omp ACP tool calls to canonical item events", () => {
  const event = mapOmpSessionUpdate({
    threadId: ThreadId.make("thread-omp"),
    eventId: EventId.make("event-omp"),
    createdAt: "2026-09-19T00:00:00.000Z",
    update: {
      sessionUpdate: "tool_call",
      toolCallId: "call-1",
      title: "Run command",
      kind: "execute",
      status: "in_progress",
      rawInput: { command: "pwd" },
    },
  });

  expect(event).toMatchObject({
    type: "item.updated",
    provider: "omp",
    itemId: "call-1",
  });
});

it("keeps unrelated ACP updates out of the canonical stream", () => {
  const event = mapOmpSessionUpdate({
    threadId: ThreadId.make("thread-omp"),
    eventId: EventId.make("event-omp"),
    createdAt: "2026-09-19T00:00:00.000Z",
    update: { sessionUpdate: "current_mode_update" } as never,
  });

  expect(event).toBeUndefined();
});

it("translates ACP approval decisions and the omp model catalog", () => {
  expect(
    ompPermissionOptionId(
      [
        { optionId: "allow_once", name: "Allow once", kind: "allow_once" },
        { optionId: "reject_once", name: "Reject once", kind: "reject_once" },
      ],
      "accept",
    ),
  ).toBe("allow_once");
  expect(
    ompPermissionOptionId(
      [{ optionId: "reject_once", name: "Reject once", kind: "reject_once" }],
      "cancel",
    ),
  ).toBeUndefined();
  expect(
    ompModelsFromConfig([
      {
        id: "model",
        name: "Model",
        category: "model",
        type: "select",
        currentValue: "omp/a",
        options: [
          { value: "omp/a", name: "OMP A" },
          { value: "omp/b", name: "OMP B" },
        ],
      },
    ]),
  ).toEqual([
    { slug: "omp/a", name: "OMP A" },
    { slug: "omp/b", name: "OMP B" },
  ]);
});

it.layer(testLayer)("waits for omp completion before resolving sendTurn", (it) =>
  it.effect("publishes one completed turn first", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "omp-adapter-test-" });
      const binaryPath = writeFakeCli({
        directory: cwd,
        name: "omp-test-agent",
        source: execScriptSource({ scriptPath: mockAgentPath }),
      });
      const adapter = yield* makeOmpAdapter({
        instanceId,
        binaryPath,
        childProcessSpawner: yield* ChildProcessSpawner.ChildProcessSpawner,
      });
      const threadId = ThreadId.make("omp-lifecycle-thread");
      const events: Array<import("@t3tools/contracts").ProviderRuntimeEvent> = [];
      const order: string[] = [];
      yield* adapter.streamEvents.pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            events.push(event);
            order.push(event.type);
          }),
        ),
        Effect.forkScoped,
      );
      yield* adapter.startSession({
        threadId,
        providerInstanceId: instanceId,
        cwd,
        runtimeMode: "auto",
      });
      const result = yield* adapter.sendTurn({ threadId, input: "hello from test" });
      order.push("sendTurn.resolved");
      const completed = events.filter(
        (event) => event.type === "turn.completed" && event.turnId === result.turnId,
      );
      expect(completed).toHaveLength(1);
      expect(completed[0]).toMatchObject({ payload: { state: "completed" } });
      expect(order.indexOf("turn.completed")).toBeLessThan(order.indexOf("sendTurn.resolved"));
      expect((yield* adapter.readThread(threadId)).turns).toHaveLength(1);
    }),
  ),
);
