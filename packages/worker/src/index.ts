import type { ApiErrorType } from "@stateful-ci/core";
import {
  Forbidden,
  HealthResponse,
  InvalidJsonBody,
  InvalidProtocolPayload,
  MethodNotAllowed,
  RequestBodyTooLarge,
  RestoreDeniedResponse,
  RestoreRequest,
  RouteNotFound,
  routes,
  SaveDeniedResponse,
  SaveRequest,
  Unauthorized,
} from "@stateful-ci/core";
import { Effect, Exit, Schema } from "effect";

import { classifyRunTrust } from "./run-classification";

/**
 * Maximum accepted size for JSON protocol request bodies.
 *
 * Restore/save requests are metadata-only control-plane messages. Snapshot bytes
 * must use the object-store data plane instead of these Worker routes, so a
 * small cap protects public endpoints from buffering oversized unauthenticated
 * bodies while leaving room for normal GitHub context and manifest metadata.
 */
export const maxProtocolBodyBytes = 64 * 1024;

interface WorkerEnv {
  readonly STATEFUL_CI_API_TOKEN?: string;
}

const protocolBodyTooLarge = new RequestBodyTooLarge({
  limitBytes: maxProtocolBodyBytes,
  message:
    "The request body exceeded the Stateful CI protocol limit. Restore/save requests are metadata-only; upload snapshot bytes through the object-store data plane.",
});

const isApiError = (error: unknown): error is ApiErrorType =>
  typeof error === "object" &&
  error !== null &&
  "_tag" in error &&
  (error._tag === "InvalidJsonBody" ||
    error._tag === "InvalidProtocolPayload" ||
    error._tag === "MethodNotAllowed" ||
    error._tag === "RequestBodyTooLarge" ||
    error._tag === "RouteNotFound" ||
    error._tag === "Unauthorized" ||
    error._tag === "Forbidden");

const authorizeRequest = (request: Request, env: WorkerEnv | undefined) => {
  const expectedToken = env?.STATEFUL_CI_API_TOKEN;
  const authorization = request.headers.get("authorization");

  if (authorization === null || !authorization.startsWith("Bearer ")) {
    return Effect.fail(
      new Unauthorized({
        message:
          "Restore and save requests require an Authorization bearer token. Set STATEFUL_CI_API_TOKEN in CI and send it as Authorization: Bearer <token>.",
      })
    );
  }

  if (expectedToken === undefined || expectedToken.length === 0) {
    return Effect.fail(
      new Forbidden({
        message:
          "The Worker does not have STATEFUL_CI_API_TOKEN configured, so restore/save requests are disabled. Configure the backend token before using Stateful CI.",
      })
    );
  }

  if (authorization.slice("Bearer ".length) !== expectedToken) {
    return Effect.fail(
      new Forbidden({
        message:
          "The Authorization bearer token did not match this Stateful CI backend. Check STATEFUL_CI_API_TOKEN and retry.",
      })
    );
  }

  return Effect.void;
};

const concatChunks = (chunks: readonly Uint8Array[]) => {
  const bytes = new Uint8Array(
    chunks.reduce((total, chunk) => total + chunk.byteLength, 0)
  );
  let offset = 0;

  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return bytes;
};

const invalidJsonBody = () =>
  new InvalidJsonBody({
    message:
      "The request body was not valid JSON. Send a JSON object that matches Stateful CI protocol v1.",
  });

const readProtocolBody = (request: Request) =>
  Effect.tryPromise({
    catch: (error) => (isApiError(error) ? error : invalidJsonBody()),
    try: async () => {
      const contentLength = request.headers.get("content-length");

      if (
        contentLength !== null &&
        Number(contentLength) > maxProtocolBodyBytes
      ) {
        throw protocolBodyTooLarge;
      }

      const chunks: Uint8Array[] = [];
      let byteLength = 0;

      for await (const chunk of request.body ?? []) {
        byteLength += chunk.byteLength;

        if (byteLength > maxProtocolBodyBytes) {
          throw protocolBodyTooLarge;
        }

        chunks.push(chunk);
      }

      return new TextDecoder().decode(concatChunks(chunks));
    },
  });

const parseProtocolJson = (source: string) =>
  Effect.gen(function* parseProtocolJsonEffect() {
    const decoded = Schema.decodeUnknownExit(Schema.UnknownFromJsonString)(
      source
    );

    if (Exit.isFailure(decoded)) {
      return yield* invalidJsonBody();
    }

    return decoded.value;
  });

const invalidProtocolPayload = () =>
  new InvalidProtocolPayload({
    message:
      "The request body was valid JSON but did not match Stateful CI protocol v1. Check the client version and request payload.",
  });

const decodeProtocolPayload =
  <A>(schema: Schema.Decoder<A>) =>
  (body: unknown) => {
    const decoded = Schema.decodeUnknownExit(schema)(body);

    return Exit.isFailure(decoded)
      ? Effect.fail(invalidProtocolPayload())
      : Effect.succeed(decoded.value);
  };

const handleRestore = (request: Request, env: WorkerEnv | undefined) =>
  Effect.gen(function* handleRestoreEffect() {
    yield* authorizeRequest(request, env);
    const restoreRequest = yield* readProtocolBody(request).pipe(
      Effect.flatMap(parseProtocolJson),
      Effect.flatMap(decodeProtocolPayload(RestoreRequest))
    );

    return Response.json(
      Schema.encodeUnknownSync(RestoreDeniedResponse)({
        decision: "denied",
        reason: "backend_policy_not_configured",
        save: { allowed: false },
        trustClass: classifyRunTrust(restoreRequest),
      })
    );
  });

const handleSave = (request: Request, env: WorkerEnv | undefined) =>
  Effect.gen(function* handleSaveEffect() {
    yield* authorizeRequest(request, env);
    yield* readProtocolBody(request).pipe(
      Effect.flatMap(parseProtocolJson),
      Effect.flatMap(decodeProtocolPayload(SaveRequest))
    );

    return Response.json(
      Schema.encodeUnknownSync(SaveDeniedResponse)({
        decision: "denied",
        reason: "backend_policy_not_configured",
      })
    );
  });

const methodNotAllowed = (path: string, method: string, allowed: string) =>
  Response.json(
    new MethodNotAllowed({
      allowed: [allowed],
      message: `The ${path} route only accepts ${allowed} requests.`,
      method,
      path,
    }),
    { headers: { Allow: allowed }, status: 405 }
  );

const handleRequest = (request: Request, env?: WorkerEnv) => {
  const path = new URL(request.url).pathname;

  if (path === routes.health.path && request.method !== routes.health.method) {
    return Effect.succeed(
      methodNotAllowed(path, request.method, routes.health.method)
    );
  }

  if (path === routes.health.path) {
    return Effect.succeed(
      Response.json(
        Schema.encodeUnknownSync(HealthResponse)({
          protocolVersion: 1,
          service: "stateful-ci-worker",
          status: "ok",
        })
      )
    );
  }

  if (
    path === routes.restore.path &&
    request.method !== routes.restore.method
  ) {
    return Effect.succeed(
      methodNotAllowed(path, request.method, routes.restore.method)
    );
  }

  if (path === routes.restore.path) {
    return handleRestore(request, env);
  }

  if (path === routes.save.path && request.method !== routes.save.method) {
    return Effect.succeed(
      methodNotAllowed(path, request.method, routes.save.method)
    );
  }

  if (path === routes.save.path) {
    return handleSave(request, env);
  }

  return Effect.succeed(
    Response.json(
      new RouteNotFound({
        message: "No Stateful CI route matches this request.",
        method: request.method,
        path,
      }),
      { status: 404 }
    )
  );
};

const apiErrorStatus = (error: ApiErrorType) => {
  switch (error._tag) {
    case "InvalidJsonBody":
    case "InvalidProtocolPayload": {
      return 400;
    }
    case "MethodNotAllowed": {
      return 405;
    }
    case "RequestBodyTooLarge": {
      return 413;
    }
    case "RouteNotFound": {
      return 404;
    }
    case "Unauthorized": {
      return 401;
    }
    case "Forbidden": {
      return 403;
    }
    default: {
      return 500;
    }
  }
};

const apiErrorResponse = (error: ApiErrorType) =>
  error._tag === "MethodNotAllowed"
    ? Response.json(error, {
        headers: { Allow: error.allowed.join(", ") },
        status: apiErrorStatus(error),
      })
    : Response.json(error, { status: apiErrorStatus(error) });

export const handleFetch = (
  request: Request,
  env?: WorkerEnv
): Promise<Response> =>
  Effect.runPromise(
    handleRequest(request, env).pipe(
      Effect.match({
        onFailure: apiErrorResponse,
        onSuccess: (response) => response,
      })
    )
  );

export default {
  fetch(request: Request, env: WorkerEnv) {
    return handleFetch(request, env);
  },
} satisfies ExportedHandler<WorkerEnv>;
