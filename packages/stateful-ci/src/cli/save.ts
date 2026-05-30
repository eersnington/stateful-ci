import {
  CommitSaveRequest,
  CommitSaveResponse,
  IdempotencyKey,
  PrepareSaveRequest,
  PrepareSaveResponse,
  protocolVersion,
  routes,
} from "@stateful-ci/core";
import type {
  CommitSaveResponse as CommitSaveResponseType,
  PrepareSaveResponse as PrepareSaveResponseType,
  RestoreRequest,
} from "@stateful-ci/core";
import { Console, Effect, Exit, Schema } from "effect";

import { createWorkspaceSnapshot } from "../snapshot-engine";
import { cliFailure, failCliFailure } from "./failure";
import {
  apiConfigFromEnv,
  githubOidcIdentityFromEnvOptional,
  restoreRequestFromEnv,
} from "./github-actions";
import type { RuntimeEnv } from "./github-actions";
import { uploadPlannedObject } from "./object-transfer";
import { decodeProtocolResponse, postProtocol } from "./protocol-client";
import { readRestoreSession } from "./restore-session";
import type { RestoreSession } from "./restore-session";
import { loadConfig } from "./workspace-config";
import type { LoadedConfig } from "./workspace-config";

const prepareSaveRequestFromRestore = Effect.fn(
  "prepareSaveRequestFromRestore"
)(function* prepareSaveRequestFromRestoreEffect(
  context: RestoreRequest,
  loaded: LoadedConfig,
  session: RestoreSession
) {
  if (session.runId !== context.github.runId) {
    return yield* Effect.fail(
      cliFailure(
        `The saved backend restore authorization belongs to run ${session.runId}, but this save is running as ${context.github.runId}. Run stateful-ci restore again in this job before saving.`
      )
    );
  }

  const snapshot = yield* createWorkspaceSnapshot({
    config: loaded.config,
    provenance: {
      git: context.git,
      github: context.github,
      runId: context.github.runId,
    },
    workspace: context.workspace,
    workspaceRoot: process.cwd(),
  }).pipe(
    Effect.mapError((error) =>
      cliFailure(
        `${error.message} Stateful CI did not send a save request because the local snapshot is incomplete.`
      )
    )
  );
  const request = {
    client: context.client,
    git: context.git,
    github: context.github,
    idempotencyKey: Schema.decodeSync(IdempotencyKey)(
      `run-${context.github.runId}-save-${snapshot.manifestDescriptor.snapshotId}`
    ),
    identity: context.identity,
    manifest: snapshot.manifestDescriptor,
    objects: snapshot.objects,
    protocolVersion,
    workspace: context.workspace,
  };
  const decoded = Schema.decodeUnknownExit(PrepareSaveRequest)(request);

  return Exit.isFailure(decoded)
    ? yield* Effect.fail(
        cliFailure(
          "The scanned workspace did not produce a valid prepare-save request."
        )
      )
    : decoded.value;
});

const printSaveResponse = (
  response: CommitSaveResponseType | PrepareSaveResponseType
) => {
  switch (response.decision) {
    case "denied": {
      return Console.log(`Save denied: ${response.reason}.`);
    }
    case "conflict": {
      return Console.log(
        `Save conflicted: expected head changed to generation ${response.actualHeadGeneration}.`
      );
    }
    case "idempotent": {
      return Console.log(
        `Save already committed: snapshot ${response.snapshotId} for workspace ${response.workspaceId}. Head generation: ${response.headGeneration}.`
      );
    }
    case "allowed": {
      return Console.log(
        `Save prepared: ${response.missingObjects.length} object(s) need upload.`
      );
    }
    case "committed": {
      return Console.log(
        `Save committed: snapshot ${response.snapshotId} for workspace ${response.workspaceId}. Head generation: ${response.headGeneration}.`
      );
    }
    default: {
      return Console.log("Save response did not match a known decision.");
    }
  }
};

/**
 * Runs the two-phase save protocol.
 *
 * The GitHub OIDC identity is intentionally resolved separately for prepare and
 * commit. `/v1/save/prepare` uses the first identity to authorize the save plan
 * and return any required object uploads. `/v1/save/commit` uses an identity
 * resolved after uploads to authorize the final metadata mutation and re-check
 * the GitHub run context.
 *
 * Object uploads do not use this OIDC identity; they use the backend-provided
 * transfer plan. Keeping commit independently authenticated avoids coupling the
 * final mutation to a token fetched before a potentially long upload phase.
 */
export const saveProgram = (env: RuntimeEnv) =>
  Effect.gen(function* saveProgramEffect() {
    const workspaceRoot = process.cwd();
    const loaded = yield* loadConfig(workspaceRoot);
    const api = yield* apiConfigFromEnv(env);
    const context = yield* restoreRequestFromEnv(env, loaded);
    const session = yield* readRestoreSession(workspaceRoot);
    const localPrepareRequest = yield* prepareSaveRequestFromRestore(
      context,
      loaded,
      session
    );
    const prepareIdentity = yield* githubOidcIdentityFromEnvOptional(env);
    const prepareRequest =
      prepareIdentity === null
        ? localPrepareRequest
        : { ...localPrepareRequest, identity: prepareIdentity };
    const prepareResponseText = yield* postProtocol(
      api,
      routes.prepareSave.path,
      Schema.encodeUnknownSync(Schema.fromJsonString(PrepareSaveRequest))(
        prepareRequest
      )
    );
    const prepareResponse = yield* decodeProtocolResponse(
      PrepareSaveResponse,
      prepareResponseText
    );

    if (prepareResponse.decision === "denied") {
      yield* printSaveResponse(prepareResponse);
      return;
    }

    for (const plan of prepareResponse.missingObjects) {
      yield* uploadPlannedObject(api, plan, workspaceRoot);
    }

    const commitIdentity = yield* githubOidcIdentityFromEnvOptional(env);
    const commitRequest = {
      baseSnapshotId: prepareResponse.baseSnapshotId,
      expectedHeadGeneration: prepareResponse.expectedHeadGeneration,
      idempotencyKey: prepareRequest.idempotencyKey,
      manifest: prepareRequest.manifest,
      objects: prepareRequest.objects,
      protocolVersion,
      runId: context.github.runId,
      target: prepareResponse.commitTarget,
      workspaceId: prepareResponse.workspaceId,
      ...(commitIdentity === null ? {} : { identity: commitIdentity }),
    };
    const decodedCommitRequest =
      Schema.decodeUnknownExit(CommitSaveRequest)(commitRequest);

    if (Exit.isFailure(decodedCommitRequest)) {
      return yield* Effect.fail(
        cliFailure(
          "The scanned workspace and backend prepare response did not produce a valid commit-save request."
        )
      );
    }

    const commitResponseText = yield* postProtocol(
      api,
      routes.commitSave.path,
      Schema.encodeUnknownSync(Schema.fromJsonString(CommitSaveRequest))(
        decodedCommitRequest.value
      )
    );
    const response = yield* decodeProtocolResponse(
      CommitSaveResponse,
      commitResponseText
    );

    yield* printSaveResponse(response);
  }).pipe(Effect.catchTag("CliFailure", failCliFailure));
