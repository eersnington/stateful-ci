import {
  RestoreRequest,
  RestoreResponse,
  routes,
  RunId,
} from "@stateful-ci/core";
import type { RestoreResponse as RestoreResponseType } from "@stateful-ci/core";
import { Console, Effect, Schema } from "effect";

import { restoreWorkspaceSnapshot } from "../snapshot-engine";
import { cliFailure, failCliFailure } from "./failure";
import {
  apiConfigFromEnv,
  githubOidcIdentityFromEnvOptional,
  restoreRequestFromEnv,
} from "./github-actions";
import type { RuntimeEnv } from "./github-actions";
import { downloadPlannedObject } from "./object-transfer";
import { decodeProtocolResponse, postProtocol } from "./protocol-client";
import { clearRestoreSession, writeRestoreSession } from "./restore-session";
import { loadConfig } from "./workspace-config";

const printRestoreResponse = (response: RestoreResponseType) =>
  response.decision === "denied"
    ? Console.log(
        `Restore denied: ${response.reason} (trust class: ${response.trustClass}). Save allowed: ${response.save.allowed ? "yes" : "no"}.`
      )
    : Console.log(
        `Restore allowed: snapshot ${response.snapshot.id} (${response.trustClass}).`
      );

const restoreProgramEffect = Effect.fn("restoreProgram")(
  function* restoreProgramEffect(env: RuntimeEnv) {
    const workspaceRoot = process.cwd();
    const loaded = yield* loadConfig(workspaceRoot);
    const api = yield* apiConfigFromEnv(env);
    const context = yield* restoreRequestFromEnv(env, loaded);
    const identity = yield* githubOidcIdentityFromEnvOptional(env);
    const request = identity === null ? context : { ...context, identity };

    yield* clearRestoreSession(workspaceRoot);

    const responseText = yield* postProtocol(
      api,
      routes.restore.path,
      Schema.encodeUnknownSync(Schema.fromJsonString(RestoreRequest))(request)
    );
    const response = yield* decodeProtocolResponse(
      RestoreResponse,
      responseText
    );

    if (
      response.decision === "denied" &&
      response.save.allowed &&
      response.workspaceId === undefined
    ) {
      return yield* Effect.fail(
        cliFailure(
          "The backend allowed save, but did not return a workspace target. Save cannot proceed without a workspaceId; retry stateful-ci restore or check that client and backend versions match."
        )
      );
    }

    if (response.decision === "allowed") {
      if (response.downloadPlan.length === 0) {
        return yield* Effect.fail(
          cliFailure(
            `The backend authorized restore for snapshot ${response.snapshot.id}, but did not provide object downloads. Restore did not mutate the workspace. Configure the object data plane or retry with a backend that supports restore downloads.`
          )
        );
      }

      for (const plan of response.downloadPlan) {
        yield* downloadPlannedObject(api, plan, workspaceRoot);
      }

      yield* restoreWorkspaceSnapshot({
        manifest: response.manifest,
        workspaceRoot,
      }).pipe(
        Effect.mapError((error) =>
          cliFailure(`${error.message} Restore did not mutate the workspace.`)
        )
      );

      yield* writeRestoreSession(workspaceRoot, {
        baseSnapshotId: response.snapshot.id,
        runId: Schema.decodeSync(RunId)(request.github.runId),
        workspaceId: response.workspaceId,
      });
    } else if (response.save.allowed && response.workspaceId !== undefined) {
      yield* writeRestoreSession(workspaceRoot, {
        baseSnapshotId: null,
        runId: Schema.decodeSync(RunId)(request.github.runId),
        workspaceId: response.workspaceId,
      });
    }

    yield* printRestoreResponse(response);
  }
);

export const restoreProgram = (env: RuntimeEnv) =>
  restoreProgramEffect(env).pipe(Effect.catchTag("CliFailure", failCliFailure));
