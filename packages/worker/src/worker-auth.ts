import { Forbidden, Unauthorized } from "@stateful-ci/core";
import { Effect } from "effect";

import type { WorkerEnv } from "./worker-env";

export const authorizeDevToken = (
  request: Request,
  env: WorkerEnv | undefined
) => {
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
