import {
  OmpSettings,
  ProviderDriverKind,
  TextGenerationError,
  type ServerProvider,
  type ServerProviderModel,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import type * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { makeOmpAdapter, ompModelsFromConfig } from "../Layers/OmpAdapter.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import type { ProviderDriver, ProviderInstance } from "../ProviderDriver.ts";
import type { ServerProviderShape } from "../Services/ServerProvider.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";

const DRIVER_KIND = ProviderDriverKind.make("omp");
const decodeSettings = Schema.decodeSync(OmpSettings);
const EMPTY_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });

const serverModelsFromConfig = (
  options: ReadonlyArray<import("effect-acp/schema").SessionConfigOption>,
): ReadonlyArray<ServerProviderModel> =>
  ompModelsFromConfig(options).map((model) => ({
    ...model,
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  }));

const serverCommandsFromAcp = (
  commands: ReadonlyArray<import("effect-acp/schema").AvailableCommand>,
): ReadonlyArray<ServerProviderSlashCommand> => {
  const seen = new Set<string>();
  return commands.flatMap((command) => {
    const name = command.name.trim();
    if (!name || seen.has(name)) return [];
    seen.add(name);
    const description = command.description.trim();
    const hint = command.input?.hint.trim();
    return [{ name, ...(description ? { description } : {}), ...(hint ? { input: { hint } } : {}) }];
  });
};

export type OmpDriverEnv = ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto;

const textGeneration: TextGeneration.TextGeneration["Service"] = {
  generateCommitMessage: () => Effect.fail(new TextGenerationError({ operation: "generateCommitMessage", detail: "omp text generation is not implemented yet." })),
  generatePrContent: () => Effect.fail(new TextGenerationError({ operation: "generatePrContent", detail: "omp text generation is not implemented yet." })),
  generateBranchName: () => Effect.fail(new TextGenerationError({ operation: "generateBranchName", detail: "omp text generation is not implemented yet." })),
  generateThreadTitle: () => Effect.fail(new TextGenerationError({ operation: "generateThreadTitle", detail: "omp text generation is not implemented yet." })),
};

export const OmpDriver: ProviderDriver<OmpSettings, OmpDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: { displayName: "oh-my-pi", supportsMultipleInstances: true },
  configSchema: OmpSettings,
  defaultConfig: () => decodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const continuation = { driverKind: DRIVER_KIND, continuationKey: `${DRIVER_KIND}:instance:${instanceId}` };
      const stamp = withInstanceIdentity({ instanceId, driverKind: DRIVER_KIND, displayName, accentColor, continuationGroupKey: continuation.continuationKey });
      const settings = { ...config, enabled } satisfies OmpSettings;
      const checkedAt = DateTime.formatIso(yield* DateTime.now);
      const snapshot: ServerProvider = stamp({
        displayName: "oh-my-pi",
        enabled: settings.enabled,
        installed: true,
        version: null,
        status: settings.enabled ? "ready" : "disabled",
        auth: { status: settings.enabled ? "authenticated" : "unknown" },
        checkedAt,
        models: [
          { slug: "nine/cate.primary", name: "Omni Primary", isCustom: false, capabilities: EMPTY_CAPABILITIES },
          { slug: "nine/cate.premium", name: "Omni Premium", isCustom: false, capabilities: EMPTY_CAPABILITIES },
          { slug: "openrouter/@preset/budget-ds-flash", name: "Budget DS Flash", isCustom: false, capabilities: EMPTY_CAPABILITIES },
          { slug: "openrouter/@preset/glm-5.3-flash", name: "GLM Flash", isCustom: false, capabilities: EMPTY_CAPABILITIES },
        ],
        slashCommands: [],
        skills: [],
        setup: { canAuthenticate: false, canInstall: false },
        supportsConversationRollback: false,
        supportsTextGeneration: false,
        message: settings.enabled ? "oh-my-pi ACP is ready to start." : "oh-my-pi is disabled in provider settings.",
      });
        const metadata = yield* SubscriptionRef.make(snapshot);
        const adapter = yield* makeOmpAdapter({
          instanceId,
          binaryPath: settings.binaryPath,
          environment: mergeProviderInstanceEnvironment(environment),
          childProcessSpawner: spawner,
          onConfigOptionsUpdated: (options) => SubscriptionRef.update(metadata, (current) => ({
            ...current,
            models: serverModelsFromConfig(options),
          })),
          onAvailableCommands: (commands) => SubscriptionRef.update(metadata, (current) => ({
            ...current,
            slashCommands: serverCommandsFromAcp(commands),
          })),
        });
        const providerShape: ServerProviderShape = {
          resolveMaintenance: () => Effect.succeed(makeManualOnlyProviderMaintenanceCapabilities({ provider: DRIVER_KIND, packageName: "@oh-my-pi/pi-coding-agent" })),
          getSnapshot: SubscriptionRef.get(metadata),
          refresh: SubscriptionRef.get(metadata),
          streamChanges: SubscriptionRef.changes(metadata),
        applyUsageLimits: () => Effect.void,
      };
      return {
        instanceId, driverKind: DRIVER_KIND, continuationIdentity: continuation, displayName, accentColor, enabled,
        snapshot: providerShape,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }).pipe(Effect.mapError((cause) => Schema.is(ProviderDriverError)(cause) ? cause : new ProviderDriverError({ driver: DRIVER_KIND, instanceId, detail: "Could not create omp provider.", cause }))),
};
