import { Sha256Digest } from "@stateful-ci/core";
import { Effect, Schema } from "effect";

const hexFromBytes = (bytes: Uint8Array) =>
  [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");

export const sha256BytesEffect = (bytes: Uint8Array) =>
  Effect.promise(() => crypto.subtle.digest("SHA-256", bytes)).pipe(
    Effect.map((digest) =>
      Schema.decodeSync(Sha256Digest)(
        `sha256:${hexFromBytes(new Uint8Array(digest))}`
      )
    )
  );
