import type { ObjectTransferPlanEntry } from "@stateful-ci/core";
import {
  Forbidden,
  InvalidProtocolPayload,
  MethodNotAllowed,
  SnapshotObjectInventoryEntry,
} from "@stateful-ci/core";
import { Effect, Exit, Schema } from "effect";

import { BlobStore } from "./blob-store";
import { BlobStoreError } from "./blob-store-error";
import { objectKindForKey } from "./object-data-plane";
import { authorizeDevToken } from "./worker-auth";
import { devAuthEnabled, transferSecretForEnv } from "./worker-env";
import type { WorkerEnv } from "./worker-env";

const transferTokenHeader = "x-stateful-ci-transfer-token";
const transferExpiresAtHeader = "x-stateful-ci-transfer-expires-at";
const transferTokenTtlMillis = 15 * 60 * 1000;

const hexToBytes = (source: string) => {
  if (source.length % 2 !== 0 || !/^[0-9a-f]+$/iu.test(source)) {
    return null;
  }

  const bytes = new Uint8Array(source.length / 2);

  for (let index = 0; index < source.length; index += 2) {
    bytes[index / 2] = Number.parseInt(source.slice(index, index + 2), 16);
  }

  return bytes;
};

const constantTimeEqual = (actual: string, expected: string) => {
  const actualBytes = hexToBytes(actual);
  const expectedBytes = hexToBytes(expected);

  if (actualBytes === null || expectedBytes === null) {
    return false;
  }

  if (actualBytes.byteLength !== expectedBytes.byteLength) {
    return false;
  }

  const actualView = new DataView(
    actualBytes.buffer,
    actualBytes.byteOffset,
    actualBytes.byteLength
  );
  const expectedView = new DataView(
    expectedBytes.buffer,
    expectedBytes.byteOffset,
    expectedBytes.byteLength
  );
  let difference = 0;

  for (let index = 0; index < expectedBytes.byteLength; index += 1) {
    difference += Math.abs(
      actualView.getUint8(index) - expectedView.getUint8(index)
    );
  }

  return difference === 0;
};

const transferTokenPayload = (input: {
  readonly digest: string;
  readonly expiresAt: string;
  readonly key: string;
  readonly method: string;
  readonly size: number;
}) =>
  [
    "v1",
    input.method,
    input.key,
    input.digest,
    String(input.size),
    input.expiresAt,
  ].join("\n");

const importTransferSigningKey = Effect.fn("importTransferSigningKey")(
  function* importTransferSigningKeyEffect(secret: string) {
    return yield* Effect.tryPromise({
      catch: () =>
        new Forbidden({
          message:
            "The Worker could not prepare object transfer authorization. Check STATEFUL_CI_TRANSFER_SECRET and retry.",
        }),
      try: () =>
        crypto.subtle.importKey(
          "raw",
          new TextEncoder().encode(secret),
          { hash: "SHA-256", name: "HMAC" },
          false,
          ["sign"]
        ),
    });
  }
);

const signTransferPayload = Effect.fn("signTransferPayload")(
  function* signTransferPayloadEffect(key: CryptoKey, payload: string) {
    const signature = yield* Effect.tryPromise({
      catch: () =>
        new Forbidden({
          message:
            "The Worker could not sign object transfer authorization. Check backend crypto support and retry.",
        }),
      try: () =>
        crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload)),
    });

    return [...new Uint8Array(signature)]
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  }
);

const planWithObjectHeaders = (plan: ObjectTransferPlanEntry) => ({
  ...plan,
  headers: {
    ...plan.headers,
    "x-stateful-ci-object-digest": plan.object.digest,
    "x-stateful-ci-object-kind": plan.object.kind,
    "x-stateful-ci-object-size": String(plan.object.size),
  },
});

const transferHeadersForPlan = Effect.fn("transferHeadersForPlan")(
  function* transferHeadersForPlanEffect(
    key: CryptoKey,
    expiresAt: string,
    plan: ObjectTransferPlanEntry
  ) {
    const token = yield* signTransferPayload(
      key,
      transferTokenPayload({
        digest: plan.object.digest,
        expiresAt,
        key: plan.object.key,
        method: plan.method,
        size: plan.object.size,
      })
    );
    const planWithHeaders = planWithObjectHeaders(plan);

    return {
      ...planWithHeaders,
      headers: {
        ...planWithHeaders.headers,
        [transferExpiresAtHeader]: expiresAt,
        [transferTokenHeader]: token,
      },
    } satisfies ObjectTransferPlanEntry;
  }
);

export const withTransferHeaders = (
  env: WorkerEnv | undefined,
  plans: readonly ObjectTransferPlanEntry[]
) =>
  Effect.gen(function* withTransferHeadersEffect() {
    if (devAuthEnabled(env)) {
      return plans.map(planWithObjectHeaders);
    }

    const secret = transferSecretForEnv(env);

    if (secret === null) {
      return yield* new Forbidden({
        message:
          "The Worker does not have STATEFUL_CI_TRANSFER_SECRET configured, so backend-authorized object transfer plans cannot be issued. Configure the transfer secret before using restore/save object downloads or uploads.",
      });
    }

    const key = yield* importTransferSigningKey(secret);
    const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);
    const expiresAt = String(now + transferTokenTtlMillis);
    const authorizedPlans: ObjectTransferPlanEntry[] = [];

    for (const plan of plans) {
      authorizedPlans.push(yield* transferHeadersForPlan(key, expiresAt, plan));
    }

    return authorizedPlans;
  });

const expectedObjectFromHeaders = (
  key: SnapshotObjectInventoryEntry["key"],
  headers: Headers
) => {
  const expectedDigest = headers.get("x-stateful-ci-object-digest");
  const expectedKind = headers.get("x-stateful-ci-object-kind");
  const expectedSize = headers.get("x-stateful-ci-object-size");

  if (
    expectedDigest === null ||
    expectedKind === null ||
    expectedSize === null
  ) {
    return null;
  }

  const decoded = Schema.decodeUnknownExit(SnapshotObjectInventoryEntry)({
    digest: expectedDigest,
    key,
    kind: expectedKind,
    size: Number(expectedSize),
  });

  return Exit.isFailure(decoded) ? null : decoded.value;
};

const authorizeObjectTransfer = Effect.fn("authorizeObjectTransfer")(
  function* authorizeObjectTransferEffect(
    request: Request,
    env: WorkerEnv | undefined,
    key: SnapshotObjectInventoryEntry["key"]
  ) {
    if (devAuthEnabled(env)) {
      return yield* authorizeDevToken(request, env);
    }

    const expectedToken = transferSecretForEnv(env);
    const actualToken = request.headers.get(transferTokenHeader);
    const expiresAt = request.headers.get(transferExpiresAtHeader);
    const object = expectedObjectFromHeaders(key, request.headers);

    if (expectedToken === null) {
      return yield* new Forbidden({
        message:
          "The Worker does not have STATEFUL_CI_TRANSFER_SECRET configured, so object downloads and uploads are disabled. Configure the transfer secret and retry.",
      });
    }

    if (actualToken === null || expiresAt === null || object === null) {
      return yield* new Forbidden({
        message:
          "The object transfer request did not include complete backend-issued transfer authorization. Restore/save object bytes were not served or stored.",
      });
    }

    const expiresAtMillis = Number(expiresAt);

    if (!Number.isSafeInteger(expiresAtMillis)) {
      return yield* new Forbidden({
        message:
          "The object transfer request included an invalid transfer expiration. Restore/save object bytes were not served or stored.",
      });
    }

    const now = yield* Effect.clockWith((clock) => clock.currentTimeMillis);

    if (expiresAtMillis <= now) {
      return yield* new Forbidden({
        message:
          "The object transfer authorization expired. Retry restore/save so the backend can issue a fresh object plan.",
      });
    }

    const signingKey = yield* importTransferSigningKey(expectedToken);
    const expected = yield* signTransferPayload(
      signingKey,
      transferTokenPayload({
        digest: object.digest,
        expiresAt,
        key,
        method: request.method,
        size: object.size,
      })
    );

    if (!constantTimeEqual(actualToken, expected)) {
      return yield* new Forbidden({
        message:
          "The object transfer authorization did not match this method, object, digest, size, and expiry. Restore/save object bytes were not served or stored.",
      });
    }

    return yield* Effect.void;
  }
);

const invalidObjectUploadPlanResponse = (path: string) =>
  Response.json(
    new InvalidProtocolPayload({
      message: `Snapshot object upload ${path} is missing or has invalid prepare-plan headers. Send x-stateful-ci-object-digest, x-stateful-ci-object-kind, and x-stateful-ci-object-size from the upload plan.`,
    }),
    { status: 400 }
  );

const objectMethodNotAllowedResponse = (path: string, method: string) =>
  Response.json(
    new MethodNotAllowed({
      allowed: ["GET", "PUT"],
      message: `The ${path} route only accepts GET or PUT requests.`,
      method,
      path,
    }),
    { headers: { Allow: "GET, PUT" }, status: 405 }
  );

type ObjectRouteMethod = "GET" | "PUT";

const isObjectRouteMethod = (method: string): method is ObjectRouteMethod =>
  method === "GET" || method === "PUT";

export const handleObjectRoute = Effect.fn("handleObjectRoute")(
  function* handleObjectRouteEffect(
    request: Request,
    env: WorkerEnv | undefined,
    key: SnapshotObjectInventoryEntry["key"],
    path: string
  ) {
    const { method } = request;

    if (!isObjectRouteMethod(method)) {
      return objectMethodNotAllowedResponse(path, method);
    }

    const blobStore = yield* BlobStore;
    yield* authorizeObjectTransfer(request, env, key);

    if (method === "GET") {
      const object = yield* blobStore.get(key);

      return new Response(object.body, {
        headers: {
          "content-length": String(object.size),
          "content-type": "application/octet-stream",
          "x-stateful-ci-object-kind": objectKindForKey(key),
        },
      });
    }

    const object = expectedObjectFromHeaders(key, request.headers);

    if (object === null) {
      return invalidObjectUploadPlanResponse(path);
    }

    const contentLength = request.headers.get("content-length");
    const declaredSize = Number(contentLength);

    if (
      contentLength === null ||
      !Number.isSafeInteger(declaredSize) ||
      declaredSize !== object.size
    ) {
      return yield* new BlobStoreError({
        key,
        message: `Snapshot object ${key} upload declared ${contentLength ?? "no"} Content-Length, but the backend expected ${object.size}. The object was not stored.`,
        reason: "size_mismatch",
      });
    }

    if (request.body === null) {
      return yield* new BlobStoreError({
        key,
        message: `Snapshot object ${key} upload did not include a request body. The object was not stored.`,
        reason: "size_mismatch",
      });
    }

    yield* blobStore.putIfAbsent({
      body: request.body,
      expectedDigest: object.digest,
      expectedSize: object.size,
      key,
    });

    return new Response(null, { status: 204 });
  }
);
