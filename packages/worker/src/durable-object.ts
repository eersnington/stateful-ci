import type { CommitSaveResponse } from "@stateful-ci/core";
import { Effect } from "effect";

import { createD1MetadataBackend, MetadataBackend } from "./metadata";
import { MetadataBackendError } from "./metadata-backend-error";
import {
  createMetadataSnapshotCoordinator,
  SnapshotCoordinator,
} from "./snapshot-coordinator";
import type {
  AuthorizeRestoreInput,
  CommitSaveInput,
  PrepareSaveCoordinatorResult,
  PrepareSaveInput,
  RestoreCoordinatorResult,
} from "./snapshot-coordinator";

interface DurableObjectEnv {
  readonly STATEFUL_CI_METADATA: D1Database;
}

type CoordinatorRequest =
  | {
      readonly action: "authorizeRestore";
      readonly input: AuthorizeRestoreInput;
    }
  | { readonly action: "commitSave"; readonly input: CommitSaveInput }
  | { readonly action: "prepareSave"; readonly input: PrepareSaveInput }
  | {
      readonly action: "recordRestoreAllowed";
      readonly input: Parameters<
        SnapshotCoordinator["Service"]["recordRestoreAllowed"]
      >[0];
    }
  | {
      readonly action: "recordRestoreObjectDenial";
      readonly input: Parameters<
        SnapshotCoordinator["Service"]["recordRestoreObjectDenial"]
      >[0];
    };

const coordinatorError = (operation: string, cause: unknown) =>
  new MetadataBackendError({
    cause,
    message: `Workspace snapshot coordinator failed during ${operation}. Mutable snapshot state was not safely updated; retry after checking Durable Object and D1 bindings.`,
    operation,
  });

const isCoordinatorRequest = (value: unknown): value is CoordinatorRequest =>
  typeof value === "object" &&
  value !== null &&
  "action" in value &&
  "input" in value &&
  (value.action === "authorizeRestore" ||
    value.action === "commitSave" ||
    value.action === "prepareSave" ||
    value.action === "recordRestoreAllowed" ||
    value.action === "recordRestoreObjectDenial");

const readCoordinatorRequest = (request: Request) =>
  Effect.tryPromise({
    catch: (cause) => coordinatorError("readCoordinatorRequest", cause),
    try: async () => {
      const body = await request.json();

      if (!isCoordinatorRequest(body)) {
        throw new Error("Invalid coordinator request payload.");
      }

      return body;
    },
  });

const jsonResponse = (value: unknown) => Response.json(value);

export class WorkspaceSnapshotCoordinatorDurableObject {
  readonly #env: DurableObjectEnv;

  constructor(_state: DurableObjectState, env: DurableObjectEnv) {
    this.#env = env;
  }

  fetch(request: Request): Promise<Response> {
    if (request.method !== "POST") {
      return Promise.resolve(
        Response.json(
          {
            message:
              "Workspace snapshot coordinator accepts POST requests only.",
          },
          { headers: { Allow: "POST" }, status: 405 }
        )
      );
    }

    const metadata = createD1MetadataBackend(this.#env.STATEFUL_CI_METADATA);
    const coordinator = createMetadataSnapshotCoordinator();

    return Effect.runPromise(
      Effect.gen(function* coordinatorFetchEffect() {
        const message = yield* readCoordinatorRequest(request);

        switch (message.action) {
          case "authorizeRestore": {
            return jsonResponse(
              yield* coordinator.authorizeRestore(message.input)
            );
          }
          case "commitSave": {
            return jsonResponse(yield* coordinator.commitSave(message.input));
          }
          case "prepareSave": {
            return jsonResponse(yield* coordinator.prepareSave(message.input));
          }
          case "recordRestoreAllowed": {
            yield* coordinator.recordRestoreAllowed(message.input);
            return jsonResponse({ ok: true });
          }
          case "recordRestoreObjectDenial": {
            yield* coordinator.recordRestoreObjectDenial(message.input);
            return jsonResponse({ ok: true });
          }
          default: {
            return jsonResponse({ message: "Unknown coordinator action." });
          }
        }
      }).pipe(
        Effect.provideService(MetadataBackend, metadata),
        Effect.match({
          onFailure: (error) => Response.json(error, { status: 500 }),
          onSuccess: (response) => response,
        })
      )
    );
  }
}

const callCoordinator = <A>(
  namespace: DurableObjectNamespace,
  target: { readonly namespace: string; readonly refName: string },
  message: CoordinatorRequest,
  decode: (body: unknown) => A
) =>
  Effect.tryPromise({
    catch: (cause) => coordinatorError(message.action, cause),
    try: async () => {
      const id = namespace.idFromName(`${target.namespace}\n${target.refName}`);
      const response = await namespace
        .get(id)
        .fetch("https://stateful-ci.internal/", {
          body: JSON.stringify(message),
          method: "POST",
        });

      const body = await response.json();

      if (!response.ok) {
        throw body;
      }

      return decode(body);
    },
  });

const decodeCoordinatorResponse = <A>(body: unknown) => body as A;

export const createDurableObjectSnapshotCoordinator = (
  namespace: DurableObjectNamespace
): SnapshotCoordinator["Service"] =>
  SnapshotCoordinator.of({
    authorizeRestore: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "authorizeRestore", input },
        (body) => decodeCoordinatorResponse<RestoreCoordinatorResult>(body)
      ),
    commitSave: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "commitSave", input },
        (body) => decodeCoordinatorResponse<CommitSaveResponse>(body)
      ),
    prepareSave: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "prepareSave", input },
        (body) => decodeCoordinatorResponse<PrepareSaveCoordinatorResult>(body)
      ),
    recordRestoreAllowed: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "recordRestoreAllowed", input },
        () => void 0
      ),
    recordRestoreObjectDenial: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "recordRestoreObjectDenial", input },
        () => void 0
      ),
  });
