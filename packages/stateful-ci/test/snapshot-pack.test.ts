import { assert, describe, it } from "@effect/vitest";
import {
  PackIndex,
  packHeaderLength,
  sha256DigestFromHex,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";

import {
  decodePack,
  encodePack,
  readPackEntry,
  sha256Bytes,
} from "../src/snapshot-pack";

const textBytes = (text: string) => new Uint8Array(Buffer.from(text, "utf-8"));

const digestWithPrefix = (prefix: string, suffix: number) =>
  sha256DigestFromHex(
    `${prefix}${suffix.toString(16).padStart(64 - prefix.length, "0")}`
  );

const replacePackIndex = (
  bytes: Uint8Array,
  update: (index: PackIndex) => PackIndex
) => {
  const headerView = new DataView(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength
  );
  const indexLength = headerView.getUint32(12);
  const indexSource = Buffer.from(
    bytes.slice(packHeaderLength, packHeaderLength + indexLength)
  ).toString("utf-8");
  const updatedIndex = update(
    Schema.decodeUnknownSync(PackIndex)(JSON.parse(indexSource))
  );
  const updatedIndexBytes = Buffer.from(JSON.stringify(updatedIndex), "utf-8");
  const updated = new Uint8Array(
    packHeaderLength +
      updatedIndexBytes.byteLength +
      bytes.byteLength -
      packHeaderLength -
      indexLength
  );
  const updatedHeader = new Uint8Array(bytes.slice(0, packHeaderLength));
  const updatedHeaderView = new DataView(updatedHeader.buffer);

  updatedHeaderView.setUint32(12, updatedIndexBytes.byteLength);
  updated.set(updatedHeader, 0);
  updated.set(updatedIndexBytes, packHeaderLength);
  updated.set(
    bytes.slice(packHeaderLength + indexLength),
    packHeaderLength + updatedIndexBytes.byteLength
  );

  return updated;
};

describe("SCIPACK container", () => {
  it.effect(
    "encodes deterministic gzip payloads and decodes pack metadata",
    () =>
      Effect.gen(function* encodeDeterministicGzipPayloadsEffect() {
        const hello = textBytes("hello");
        const goodbye = textBytes("goodbye");
        const encoded = yield* encodePack([
          { bytes: goodbye, digest: sha256Bytes(goodbye) },
          { bytes: hello, digest: sha256Bytes(hello) },
        ]);
        const decoded = yield* decodePack(encoded.bytes, encoded.digest);

        assert.deepStrictEqual(
          encoded.index.entries.map((entry) => entry.entryDigest),
          encoded.index.entries.map((entry) => entry.entryDigest).toSorted()
        );
        assert.strictEqual(decoded.digest, encoded.digest);
        assert.strictEqual(decoded.key, encoded.key);
        assert.deepStrictEqual(
          decoded.entries.map((entry) => entry.entryDigest),
          encoded.index.entries.map((entry) => entry.entryDigest)
        );
        assert.deepStrictEqual(
          decoded.entries.map((entry) => ({
            bytes: Buffer.from(entry.bytes).toString("utf-8"),
            compression: entry.compression,
          })),
          [
            { bytes: "hello", compression: "gzip" },
            { bytes: "goodbye", compression: "gzip" },
          ]
        );
        assert.isTrue(
          encoded.index.entries.every(
            (entry) =>
              entry.compressedLength > 0 && entry.compression === "gzip"
          )
        );
      })
  );

  it.effect("decodes a requested pack entry by digest", () =>
    Effect.gen(function* decodeRequestedPackEntryEffect() {
      const hello = textBytes("hello");
      const goodbye = textBytes("goodbye");
      const helloInput = { bytes: hello, digest: sha256Bytes(hello) };
      const encoded = yield* encodePack([
        { bytes: goodbye, digest: sha256Bytes(goodbye) },
        helloInput,
      ]);
      const entry = yield* readPackEntry(
        encoded.bytes,
        helloInput.digest,
        encoded.digest
      );

      assert.deepStrictEqual([...entry.bytes], [...hello]);
    })
  );

  it.effect("rejects pack inputs whose digest does not match the bytes", () =>
    Effect.gen(function* rejectDigestMismatchPackInputsEffect() {
      const input = textBytes("payload");
      const error = yield* Effect.flip(
        encodePack([{ bytes: input, digest: digestWithPrefix("ff", 1) }])
      );

      assert.strictEqual(error.reason, "encode_input_digest_mismatch");
    })
  );

  it.effect("rejects malformed headers and malformed indexes", () =>
    Effect.gen(function* rejectMalformedHeadersAndIndexesEffect() {
      const input = textBytes("payload");
      const encoded = yield* encodePack([
        { bytes: input, digest: sha256Bytes(input) },
      ]);
      const badMagic = new Uint8Array(encoded.bytes);
      badMagic[0] = 0;
      const badIndex = new Uint8Array(encoded.bytes);
      new DataView(
        badIndex.buffer,
        badIndex.byteOffset,
        badIndex.byteLength
      ).setUint32(12, 1);

      const badMagicError = yield* Effect.flip(decodePack(badMagic));
      const badIndexError = yield* Effect.flip(decodePack(badIndex));

      assert.strictEqual(badMagicError.reason, "invalid_header");
      assert.strictEqual(badIndexError.reason, "invalid_index");
    })
  );

  it.effect("rejects pack headers with unsupported flags", () =>
    Effect.gen(function* rejectUnsupportedPackFlagsEffect() {
      const input = textBytes("payload");
      const encoded = yield* encodePack([
        { bytes: input, digest: sha256Bytes(input) },
      ]);
      const unsupportedFlags = new Uint8Array(encoded.bytes);
      new DataView(
        unsupportedFlags.buffer,
        unsupportedFlags.byteOffset,
        unsupportedFlags.byteLength
      ).setUint16(10, 1);

      const error = yield* Effect.flip(decodePack(unsupportedFlags));

      assert.strictEqual(error.reason, "unsupported_flags");
    })
  );

  it.effect("rejects invalid index ordering and ranges", () =>
    Effect.gen(function* rejectInvalidPackIndexesEffect() {
      const first = textBytes("first");
      const second = textBytes("second");
      const encoded = yield* encodePack([
        { bytes: first, digest: sha256Bytes(first) },
        { bytes: second, digest: sha256Bytes(second) },
      ]);
      const unsorted = replacePackIndex(encoded.bytes, (index) => ({
        ...index,
        entries: [...index.entries].toReversed(),
      }));
      const outOfRange = replacePackIndex(encoded.bytes, (index) => ({
        ...index,
        entries: index.entries.map((entry, entryIndex) =>
          entryIndex === 0
            ? {
                ...entry,
                compressedOffset: encoded.bytes.byteLength,
              }
            : entry
        ),
      }));

      const unsortedError = yield* Effect.flip(decodePack(unsorted));
      const outOfRangeError = yield* Effect.flip(decodePack(outOfRange));

      assert.strictEqual(unsortedError.reason, "invalid_index");
      assert.strictEqual(outOfRangeError.reason, "invalid_index");
    })
  );

  it.effect(
    "rejects tampered pack bytes before workspace materialization",
    () =>
      Effect.gen(function* rejectTamperedPackBytesEffect() {
        const input = textBytes("payload");
        const encoded = yield* encodePack([
          { bytes: input, digest: sha256Bytes(input) },
        ]);
        const tampered = new Uint8Array(encoded.bytes);
        const lastIndex = tampered.byteLength - 1;
        tampered[lastIndex] = tampered[lastIndex] === 0 ? 1 : 0;
        const error = yield* Effect.flip(decodePack(tampered, encoded.digest));

        assert.strictEqual(error.reason, "digest_mismatch");
      })
  );

  it.effect(
    "rejects decoded entries whose digest does not match the index",
    () =>
      Effect.gen(function* rejectEntryDigestMismatchEffect() {
        const input = textBytes("payload");
        const encoded = yield* encodePack([
          { bytes: input, digest: sha256Bytes(input) },
        ]);
        const tampered = replacePackIndex(encoded.bytes, (index) => ({
          ...index,
          entries: index.entries.map((entry) => ({
            ...entry,
            entryDigest: digestWithPrefix("ff", 1),
          })),
        }));
        const error = yield* Effect.flip(decodePack(tampered));

        assert.strictEqual(error.reason, "entry_digest_mismatch");
      })
  );

  it.effect("rejects tampered compressed payloads", () =>
    Effect.gen(function* rejectTamperedCompressedPayloadEffect() {
      const input = textBytes("payload");
      const encoded = yield* encodePack([
        { bytes: input, digest: sha256Bytes(input) },
      ]);
      const tampered = new Uint8Array(encoded.bytes);
      const indexLength = new DataView(
        tampered.buffer,
        tampered.byteOffset,
        tampered.byteLength
      ).getUint32(12);
      const payloadOffset = packHeaderLength + indexLength;

      tampered[payloadOffset] = tampered[payloadOffset] === 0 ? 1 : 0;

      const error = yield* Effect.flip(decodePack(tampered));

      assert.strictEqual(error.reason, "malformed_payload");
    })
  );

  it.effect("reports missing entry lookup as a value error", () =>
    Effect.gen(function* reportMissingEntryLookupEffect() {
      const input = textBytes("payload");
      const encoded = yield* encodePack([
        { bytes: input, digest: sha256Bytes(input) },
      ]);
      const error = yield* Effect.flip(
        readPackEntry(encoded.bytes, digestWithPrefix("ff", 1), encoded.digest)
      );

      assert.strictEqual(error.reason, "missing_entry");
    })
  );
});
