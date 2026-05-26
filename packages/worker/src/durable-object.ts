import {
  CommitSaveResponse as CommitSaveResponseSchema,
  DenialReason,
  HeadGeneration,
  IdempotencyKey,
  ManifestKey,
  ManifestDescriptor,
  RunId,
  Sha256Digest,
  SnapshotId,
  SnapshotObjectInventory,
  TrustClass,
  WorkspaceId,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";

import { createD1MetadataBackend, MetadataBackend } from "./metadata";
import { MetadataBackendError } from "./metadata-backend-error";
import {
  createMetadataSnapshotCoordinator,
  SnapshotCoordinator,
} from "./snapshot-coordinator";

interface DurableObjectEnv {
  readonly STATEFUL_CI_METADATA: D1Database;
}

const RefTargetSchema = Schema.Struct({
  namespace: Schema.String.check(Schema.isMinLength(1)),
  refName: Schema.String.check(Schema.isMinLength(1)),
});

const ProducerContextSchema = Schema.Struct({
  actor: Schema.String,
  event: Schema.String,
  job: Schema.String,
  ref: Schema.String,
  repository: Schema.String,
  runId: RunId,
  sha: Schema.String,
  workflow: Schema.String,
});

const AuthorizeRestoreInputSchema = Schema.Struct({
  candidates: Schema.Array(RefTargetSchema),
  runId: RunId,
  target: RefTargetSchema,
  trustClass: TrustClass,
  workspaceId: WorkspaceId,
});

const PrepareSaveInputSchema = Schema.Struct({
  expiresAt: Schema.String.check(Schema.isMinLength(1)),
  producer: ProducerContextSchema,
  runId: RunId,
  target: RefTargetSchema,
  trustClass: TrustClass,
  workspaceId: WorkspaceId,
});

const CommitSaveInputSchema = Schema.Struct({
  baseSnapshotId: Schema.NullOr(SnapshotId),
  expectedHeadGeneration: HeadGeneration,
  idempotencyKey: IdempotencyKey,
  manifest: ManifestDescriptor,
  objects: SnapshotObjectInventory,
  producer: ProducerContextSchema,
  target: RefTargetSchema,
  workspaceId: WorkspaceId,
});

const RestoreAuditInputSchema = Schema.Struct({
  runId: RunId,
  snapshotId: SnapshotId,
  target: RefTargetSchema,
  trustClass: TrustClass,
  workspaceId: WorkspaceId,
});

const RestoreObjectDenialInputSchema = Schema.Struct({
  reason: DenialReason,
  runId: RunId,
  snapshotId: SnapshotId,
  target: RefTargetSchema,
  trustClass: TrustClass,
  workspaceId: WorkspaceId,
});

const CoordinatorRequestSchema = Schema.Union([
  Schema.Struct({
    action: Schema.Literal("authorizeRestore"),
    input: AuthorizeRestoreInputSchema,
  }),
  Schema.Struct({
    action: Schema.Literal("commitSave"),
    input: CommitSaveInputSchema,
  }),
  Schema.Struct({
    action: Schema.Literal("prepareSave"),
    input: PrepareSaveInputSchema,
  }),
  Schema.Struct({
    action: Schema.Literal("recordRestoreAllowed"),
    input: RestoreAuditInputSchema,
  }),
  Schema.Struct({
    action: Schema.Literal("recordRestoreObjectDenial"),
    input: RestoreObjectDenialInputSchema,
  }),
]);

type CoordinatorRequest = Schema.Schema.Type<typeof CoordinatorRequestSchema>;

interface CoordinatorRequestError {
  readonly _tag: "CoordinatorRequestError";
  readonly cause: unknown;
  readonly message: string;
  readonly operation: string;
}

const coordinatorRequestError = (
  operation: string,
  cause: unknown
): CoordinatorRequestError => ({
  _tag: "CoordinatorRequestError",
  cause,
  message:
    "Invalid coordinator request payload. The Durable Object did not run snapshot coordination; check the internal RPC caller and schema version before retrying.",
  operation,
});

const coordinatorError = (operation: string, cause: unknown) =>
  new MetadataBackendError({
    cause,
    message: `Workspace snapshot coordinator failed during ${operation}. Mutable snapshot state was not safely updated; retry after checking Durable Object and D1 bindings.`,
    operation,
  });

const readCoordinatorRequest = (request: Request) =>
  Effect.tryPromise({
    catch: (cause) => coordinatorRequestError("readCoordinatorRequest", cause),
    try: () => request.json(),
  }).pipe(
    Effect.flatMap((body) =>
      Effect.try({
        catch: (cause) =>
          coordinatorRequestError("decodeCoordinatorRequest", cause),
        try: () => Schema.decodeUnknownSync(CoordinatorRequestSchema)(body),
      })
    )
  );

const decodeCoordinatorBody =
  <S extends Schema.Decoder<unknown>>(schema: S) =>
  (body: unknown) =>
    Schema.decodeUnknownSync(schema)(body);

const VoidResponseSchema = Schema.Struct({ ok: Schema.Literal(true) });

const RestoreCoordinatorResultSchema = Schema.Union([
  Schema.Struct({
    decision: Schema.Literal("allowed"),
    manifest: ManifestDescriptor,
    objects: SnapshotObjectInventory,
    saveTarget: Schema.NullOr(RefTargetSchema),
    snapshot: Schema.Struct({
      createdAt: Schema.String,
      manifestDigest: Sha256Digest,
      manifestKey: ManifestKey,
      manifestSize: Schema.Number,
      namespace: Schema.String,
      parentSnapshotId: Schema.NullOr(SnapshotId),
      producerActor: Schema.String,
      producerEvent: Schema.String,
      producerJob: Schema.String,
      producerRef: Schema.String,
      producerRepository: Schema.String,
      producerRunId: RunId,
      producerSha: Schema.String,
      producerWorkflow: Schema.String,
      safetyJson: Schema.String,
      snapshotId: SnapshotId,
      statsJson: Schema.String,
      trustClass: TrustClass,
      workspaceId: WorkspaceId,
    }),
  }),
  Schema.Struct({
    decision: Schema.Literal("denied"),
    reason: RestoreObjectDenialInputSchema.fields.reason,
    saveTarget: Schema.NullOr(RefTargetSchema),
    snapshotId: Schema.NullOr(SnapshotId),
  }),
]);

const PrepareSaveCoordinatorResultSchema = Schema.Union([
  Schema.Struct({
    baseSnapshotId: Schema.NullOr(SnapshotId),
    decision: Schema.Literal("allowed"),
    expectedHeadGeneration: HeadGeneration,
  }),
  Schema.Struct({
    decision: Schema.Literal("denied"),
    reason: RestoreObjectDenialInputSchema.fields.reason,
  }),
]);

const jsonResponse = (value: unknown) => Response.json(value);

export class WorkspaceSnapshotCoordinatorDurableObject {
  readonly #env: DurableObjectEnv;
  #queue = Promise.resolve();

  constructor(_state: DurableObjectState, env: DurableObjectEnv) {
    this.#env = env;
  }

  async #serialized<A>(operation: () => Promise<A>): Promise<A> {
    const previous = this.#queue;

    const result = (async () => {
      try {
        await previous;
      } catch {
        // Keep later requests moving even if an earlier serialized action failed.
      }

      return await operation();
    })();
    this.#queue = (async () => {
      try {
        await result;
      } catch {
        // Keep the queue settled so future operations can continue.
      }
    })();

    return await result;
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

    return this.#serialized(() =>
      Effect.runPromise(
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
              return jsonResponse(
                yield* coordinator.prepareSave(message.input)
              );
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
              const unhandled: never = message;
              return Response.json(
                { message: "Unknown coordinator action.", unhandled },
                { status: 400 }
              );
            }
          }
        }).pipe(
          Effect.provideService(MetadataBackend, metadata),
          Effect.match({
            onFailure: (error) =>
              error._tag === "CoordinatorRequestError"
                ? Response.json(error, { status: 400 })
                : Response.json(error, { status: 500 }),
            onSuccess: (response) => response,
          })
        )
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

export const createDurableObjectSnapshotCoordinator = (
  namespace: DurableObjectNamespace
): SnapshotCoordinator["Service"] =>
  SnapshotCoordinator.of({
    authorizeRestore: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "authorizeRestore", input },
        decodeCoordinatorBody(RestoreCoordinatorResultSchema)
      ),
    commitSave: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "commitSave", input },
        decodeCoordinatorBody(CommitSaveResponseSchema)
      ),
    prepareSave: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "prepareSave", input },
        decodeCoordinatorBody(PrepareSaveCoordinatorResultSchema)
      ),
    recordRestoreAllowed: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "recordRestoreAllowed", input },
        (body) => {
          decodeCoordinatorBody(VoidResponseSchema)(body);
        }
      ),
    recordRestoreObjectDenial: (input) =>
      callCoordinator(
        namespace,
        input.target,
        { action: "recordRestoreObjectDenial", input },
        (body) => {
          decodeCoordinatorBody(VoidResponseSchema)(body);
        }
      ),
  });
