import { sha256DigestFromHex } from "@stateful-ci/core";
import { Effect } from "effect";
import { describe, expect, test } from "vitest";

import {
  decodePack,
  encodePack,
  readPackEntry,
  sha256Bytes,
} from "./snapshot-pack";
import type { PackFormatError } from "./snapshot-pack";

const textBytes = (text: string) => new Uint8Array(Buffer.from(text, "utf-8"));

const digestWithPrefix = (prefix: string, suffix: number) =>
  sha256DigestFromHex(
    `${prefix}${suffix.toString(16).padStart(64 - prefix.length, "0")}`
  );

const expectPackError = (
  error: PackFormatError,
  reason: PackFormatError["reason"]
) => {
  expect(error.reason).toBe(reason);
};

describe("SCIPACK container", () => {
  test("encodes deterministic gzip payloads and decodes pack metadata", async () => {
    const hello = textBytes("hello");
    const goodbye = textBytes("goodbye");
    const encoded = await Effect.runPromise(
      encodePack([
        { bytes: goodbye, digest: sha256Bytes(goodbye) },
        { bytes: hello, digest: sha256Bytes(hello) },
      ])
    );
    const decoded = await Effect.runPromise(
      decodePack(encoded.bytes, encoded.digest)
    );

    expect(
      encoded.index.entries.map((entry) => entry.entryDigest)
    ).toStrictEqual(
      encoded.index.entries.map((entry) => entry.entryDigest).toSorted()
    );
    expect(decoded.digest).toBe(encoded.digest);
    expect(decoded.key).toBe(encoded.key);
    expect(decoded.entries.map((entry) => entry.entryDigest)).toStrictEqual(
      encoded.index.entries.map((entry) => entry.entryDigest)
    );
  });

  test("decodes a requested pack entry by digest", async () => {
    const hello = textBytes("hello");
    const goodbye = textBytes("goodbye");
    const helloInput = { bytes: hello, digest: sha256Bytes(hello) };
    const encoded = await Effect.runPromise(
      encodePack([{ bytes: goodbye, digest: sha256Bytes(goodbye) }, helloInput])
    );
    const entry = await Effect.runPromise(
      readPackEntry(encoded.bytes, helloInput.digest, encoded.digest)
    );

    expect([...entry.bytes]).toStrictEqual([...hello]);
  });

  test("rejects pack inputs whose digest does not match the bytes", async () => {
    const input = textBytes("payload");

    expectPackError(
      await Effect.runPromise(
        Effect.flip(
          encodePack([{ bytes: input, digest: digestWithPrefix("ff", 1) }])
        )
      ),
      "encode_input_digest_mismatch"
    );
  });

  test("rejects malformed headers and malformed indexes", async () => {
    const input = textBytes("payload");
    const encoded = await Effect.runPromise(
      encodePack([{ bytes: input, digest: sha256Bytes(input) }])
    );
    const badMagic = new Uint8Array(encoded.bytes);
    badMagic[0] = 0;
    const badIndex = new Uint8Array(encoded.bytes);
    new DataView(
      badIndex.buffer,
      badIndex.byteOffset,
      badIndex.byteLength
    ).setUint32(12, 1);

    expectPackError(
      await Effect.runPromise(Effect.flip(decodePack(badMagic))),
      "invalid_header"
    );
    expectPackError(
      await Effect.runPromise(Effect.flip(decodePack(badIndex))),
      "invalid_index"
    );
  });

  test("rejects tampered pack bytes before workspace materialization", async () => {
    const input = textBytes("payload");
    const encoded = await Effect.runPromise(
      encodePack([{ bytes: input, digest: sha256Bytes(input) }])
    );
    const tampered = new Uint8Array(encoded.bytes);
    const lastIndex = tampered.byteLength - 1;
    tampered[lastIndex] = tampered[lastIndex] === 0 ? 1 : 0;

    expectPackError(
      await Effect.runPromise(
        Effect.flip(decodePack(tampered, encoded.digest))
      ),
      "digest_mismatch"
    );
  });

  test("reports missing entry lookup as a value error", async () => {
    const input = textBytes("payload");
    const encoded = await Effect.runPromise(
      encodePack([{ bytes: input, digest: sha256Bytes(input) }])
    );

    expectPackError(
      await Effect.runPromise(
        Effect.flip(
          readPackEntry(
            encoded.bytes,
            digestWithPrefix("ff", 1),
            encoded.digest
          )
        )
      ),
      "missing_entry"
    );
  });
});
