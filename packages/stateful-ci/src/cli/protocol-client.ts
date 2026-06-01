import { Effect, Exit, Schema } from "effect";

import { cliFailure } from "./failure";

export interface ApiConfig {
  readonly token: string | null;
  readonly url: URL;
}

export const relativeWorkerRouteUrl = (
  api: ApiConfig,
  route: string,
  context: "restore" | "save"
) => {
  if (!route.startsWith("/") || route.startsWith("//")) {
    return Effect.fail(
      cliFailure(
        `The backend returned an unsafe ${context} worker-route ${route}. Worker-route object plans must use a relative path beginning with /. ${context === "restore" ? "Restore did not mutate the workspace." : "Save did not upload or commit objects."}`
      )
    );
  }

  return Effect.succeed(new URL(route, api.url).href);
};

export const postProtocol = Effect.fn("postProtocol")(
  function* postProtocolEffect(api: ApiConfig, route: string, body: string) {
    const headers = new Headers({ "content-type": "application/json" });

    if (api.token !== null) {
      headers.set("authorization", `Bearer ${api.token}`);
    }

    const response = yield* Effect.tryPromise({
      catch: () =>
        cliFailure(
          `Could not reach Stateful CI backend at ${api.url.href}. Check STATEFUL_CI_API_URL and network access.`
        ),
      try: (signal) =>
        fetch(new URL(route, api.url), {
          body,
          headers,
          method: "POST",
          signal,
        }),
    });

    if (!response.ok) {
      return yield* Effect.fail(
        cliFailure(`Stateful CI backend returned HTTP ${response.status}.`)
      );
    }

    return yield* Effect.tryPromise({
      catch: () =>
        cliFailure(
          `Could not read Stateful CI backend response body after HTTP ${response.status}. Retry or check backend logs.`
        ),
      try: () => response.text(),
    });
  }
);

export const decodeProtocolResponse = <A>(
  schema: Schema.Decoder<A>,
  source: string
) => {
  const decoded = Schema.decodeUnknownExit(Schema.fromJsonString(schema))(
    source
  );

  return Exit.isFailure(decoded)
    ? Effect.fail(
        cliFailure(
          "The Stateful CI backend returned a response that does not match protocol v1. Check client and backend versions."
        )
      )
    : Effect.succeed(decoded.value);
};
