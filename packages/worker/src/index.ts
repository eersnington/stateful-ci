import {
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
} from "@stateful-ci/core";
import { Exit, Schema } from "effect";

/**
 * Maximum accepted size for JSON protocol request bodies.
 *
 * Restore/save requests are metadata-only control-plane messages. Snapshot bytes
 * must use the object-store data plane instead of these Worker routes, so a
 * small cap protects public endpoints from buffering oversized unauthenticated
 * bodies while leaving room for normal GitHub context and manifest metadata.
 */
export const maxProtocolBodyBytes = 64 * 1024;

const handleRestore = async (request: Request) => {
  const contentLength = request.headers.get("content-length");

  if (contentLength !== null && Number(contentLength) > maxProtocolBodyBytes) {
    return Response.json(
      new RequestBodyTooLarge({
        limitBytes: maxProtocolBodyBytes,
        message:
          "The request body exceeded the Stateful CI protocol limit. Restore/save requests are metadata-only; upload snapshot bytes through the object-store data plane.",
      }),
      { status: 413 }
    );
  }

  const chunks: Uint8Array[] = [];
  let byteLength = 0;

  for await (const chunk of request.body ?? []) {
    byteLength += chunk.byteLength;

    if (byteLength > maxProtocolBodyBytes) {
      return Response.json(
        new RequestBodyTooLarge({
          limitBytes: maxProtocolBodyBytes,
          message:
            "The request body exceeded the Stateful CI protocol limit. Restore/save requests are metadata-only; upload snapshot bytes through the object-store data plane.",
        }),
        { status: 413 }
      );
    }

    chunks.push(chunk);
  }

  let body: unknown;

  try {
    body = JSON.parse(await new Blob(chunks).text());
  } catch {
    return Response.json(
      new InvalidJsonBody({
        message:
          "The request body was not valid JSON. Send a JSON object that matches Stateful CI protocol v1.",
      }),
      { status: 400 }
    );
  }

  if (Exit.isFailure(Schema.decodeUnknownExit(RestoreRequest)(body))) {
    return Response.json(
      new InvalidProtocolPayload({
        message:
          "The request body was valid JSON but did not match Stateful CI protocol v1. Check the client version and request payload.",
      }),
      { status: 400 }
    );
  }

  return Response.json(
    Schema.encodeUnknownSync(RestoreDeniedResponse)({
      decision: "denied",
      reason: "backend_policy_not_configured",
      save: { allowed: false },
      trustClass: "unknown",
    })
  );
};

const handleSave = async (request: Request) => {
  const contentLength = request.headers.get("content-length");

  if (contentLength !== null && Number(contentLength) > maxProtocolBodyBytes) {
    return Response.json(
      new RequestBodyTooLarge({
        limitBytes: maxProtocolBodyBytes,
        message:
          "The request body exceeded the Stateful CI protocol limit. Restore/save requests are metadata-only; upload snapshot bytes through the object-store data plane.",
      }),
      { status: 413 }
    );
  }

  const chunks: Uint8Array[] = [];
  let byteLength = 0;

  for await (const chunk of request.body ?? []) {
    byteLength += chunk.byteLength;

    if (byteLength > maxProtocolBodyBytes) {
      return Response.json(
        new RequestBodyTooLarge({
          limitBytes: maxProtocolBodyBytes,
          message:
            "The request body exceeded the Stateful CI protocol limit. Restore/save requests are metadata-only; upload snapshot bytes through the object-store data plane.",
        }),
        { status: 413 }
      );
    }

    chunks.push(chunk);
  }

  let body: unknown;

  try {
    body = JSON.parse(await new Blob(chunks).text());
  } catch {
    return Response.json(
      new InvalidJsonBody({
        message:
          "The request body was not valid JSON. Send a JSON object that matches Stateful CI protocol v1.",
      }),
      { status: 400 }
    );
  }

  if (Exit.isFailure(Schema.decodeUnknownExit(SaveRequest)(body))) {
    return Response.json(
      new InvalidProtocolPayload({
        message:
          "The request body was valid JSON but did not match Stateful CI protocol v1. Check the client version and request payload.",
      }),
      { status: 400 }
    );
  }

  return Response.json(
    Schema.encodeUnknownSync(SaveDeniedResponse)({
      decision: "denied",
      reason: "backend_policy_not_configured",
    })
  );
};

export const handleFetch = (request: Request) => {
  const path = new URL(request.url).pathname;

  if (path === routes.health.path && request.method !== routes.health.method) {
    return Response.json(
      new MethodNotAllowed({
        allowed: [routes.health.method],
        message: `The ${path} route only accepts ${routes.health.method} requests.`,
        method: request.method,
        path,
      }),
      { headers: { Allow: routes.health.method }, status: 405 }
    );
  }

  if (path === routes.health.path) {
    return Response.json(
      Schema.encodeUnknownSync(HealthResponse)({
        protocolVersion: 1,
        service: "stateful-ci-worker",
        status: "ok",
      })
    );
  }

  if (
    path === routes.restore.path &&
    request.method !== routes.restore.method
  ) {
    return Response.json(
      new MethodNotAllowed({
        allowed: [routes.restore.method],
        message: `The ${path} route only accepts ${routes.restore.method} requests.`,
        method: request.method,
        path,
      }),
      { headers: { Allow: routes.restore.method }, status: 405 }
    );
  }

  if (path === routes.restore.path) {
    return handleRestore(request);
  }

  if (path === routes.save.path && request.method !== routes.save.method) {
    return Response.json(
      new MethodNotAllowed({
        allowed: [routes.save.method],
        message: `The ${path} route only accepts ${routes.save.method} requests.`,
        method: request.method,
        path,
      }),
      { headers: { Allow: routes.save.method }, status: 405 }
    );
  }

  if (path === routes.save.path) {
    return handleSave(request);
  }

  return Response.json(
    new RouteNotFound({
      message: "No Stateful CI route matches this request.",
      method: request.method,
      path,
    }),
    { status: 404 }
  );
};

export default {
  fetch(request: Request) {
    return handleFetch(request);
  },
} satisfies ExportedHandler;
