import {
  InvalidJsonBody,
  InvalidProtocolPayload,
  RequestBodyTooLarge,
} from "@stateful-ci/core";
import { Effect, Exit, Schema } from "effect";

const maxProtocolBodyBytes = 64 * 1024;

export const decodeProtocolRequest = Effect.fn("decodeProtocolRequest")(
  function* decodeProtocolRequestEffect<A>(
    request: Request,
    schema: Schema.Decoder<A>
  ) {
    const contentLength = request.headers.get("content-length");

    if (
      contentLength !== null &&
      Number(contentLength) > maxProtocolBodyBytes
    ) {
      return yield* new RequestBodyTooLarge({
        limitBytes: maxProtocolBodyBytes,
        message:
          "The request body exceeded the Stateful CI protocol limit. Restore/save requests are metadata-only; upload snapshot bytes through the object-store data plane.",
      });
    }

    const reader = request.body?.getReader();
    const chunks: Uint8Array[] = [];
    let byteLength = 0;

    if (reader === undefined) {
      return yield* new InvalidJsonBody({
        message:
          "The request body was not valid JSON. Send a JSON object that matches Stateful CI protocol v1.",
      });
    }

    for (;;) {
      const chunk = yield* Effect.tryPromise({
        catch: () =>
          new InvalidJsonBody({
            message:
              "The request body was not valid JSON. Send a JSON object that matches Stateful CI protocol v1.",
          }),
        try: () => reader.read(),
      });

      if (chunk.done) {
        break;
      }

      byteLength += chunk.value.byteLength;

      if (byteLength > maxProtocolBodyBytes) {
        return yield* new RequestBodyTooLarge({
          limitBytes: maxProtocolBodyBytes,
          message:
            "The request body exceeded the Stateful CI protocol limit. Restore/save requests are metadata-only; upload snapshot bytes through the object-store data plane.",
        });
      }

      chunks.push(chunk.value);
    }

    const bytes = new Uint8Array(byteLength);
    let offset = 0;

    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }

    const source = new TextDecoder().decode(bytes);
    const decoded = Schema.decodeUnknownExit(Schema.UnknownFromJsonString)(
      source
    );

    if (Exit.isFailure(decoded)) {
      return yield* new InvalidJsonBody({
        message:
          "The request body was not valid JSON. Send a JSON object that matches Stateful CI protocol v1.",
      });
    }

    const payload = Schema.decodeUnknownExit(schema)(decoded.value);

    if (Exit.isFailure(payload)) {
      return yield* new InvalidProtocolPayload({
        message:
          "The request body was valid JSON but did not match Stateful CI protocol v1. Check the client version and request payload.",
      });
    }

    return payload.value;
  }
);
