import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { gzip, gunzip } from "node:zlib";

import {
  packFormatVersion,
  packHeaderLength,
  packKeyFromDigest,
  packMagic,
  PackIndex,
  sha256DigestFromHex,
} from "@stateful-ci/core";
import type { PackIndexEntry, PackKey, Sha256Digest } from "@stateful-ci/core";
import { Effect, Schema } from "effect";

export interface PackInput {
  readonly bytes: Uint8Array;
  readonly digest: Sha256Digest;
}

export interface EncodedPack {
  readonly bytes: Uint8Array;
  readonly digest: Sha256Digest;
  readonly index: PackIndex;
  readonly key: PackKey;
}

export interface DecodedPackEntry extends PackIndexEntry {
  readonly bytes: Uint8Array;
}

export interface DecodedPack {
  readonly digest: Sha256Digest;
  readonly entries: readonly DecodedPackEntry[];
  readonly index: PackIndex;
  readonly key: PackKey;
}

export const PackFormatErrorReason = Schema.Literals([
  "compression_failed",
  "digest_mismatch",
  "encode_input_digest_mismatch",
  "entry_digest_mismatch",
  "entry_size_mismatch",
  "invalid_header",
  "invalid_index",
  "malformed_payload",
  "missing_entry",
  "unsupported_version",
]);
export type PackFormatErrorReason = Schema.Schema.Type<
  typeof PackFormatErrorReason
>;

export class PackFormatError extends Schema.TaggedErrorClass<PackFormatError>()(
  "PackFormatError",
  {
    message: Schema.String,
    reason: PackFormatErrorReason,
  }
) {}

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);
const magicBytes = Buffer.from(packMagic, "utf-8");

const packError = (reason: PackFormatErrorReason, message: string) =>
  new PackFormatError({ message, reason });

const gzipBytes = (bytes: Uint8Array) =>
  Effect.tryPromise({
    catch: () =>
      packError(
        "compression_failed",
        "Pack payload entry could not be compressed."
      ),
    try: async () => new Uint8Array(await gzipAsync(bytes)),
  });

const gunzipBytes = (bytes: Uint8Array) =>
  Effect.tryPromise({
    catch: () =>
      packError(
        "malformed_payload",
        "Pack payload entry could not be decompressed."
      ),
    try: async () => new Uint8Array(await gunzipAsync(bytes)),
  });

const concatBytes = (parts: readonly Uint8Array[]) => {
  let byteLength = 0;

  for (const part of parts) {
    byteLength += part.byteLength;
  }

  const bytes = new Uint8Array(byteLength);
  let offset = 0;

  for (const part of parts) {
    bytes.set(part, offset);
    offset += part.byteLength;
  }

  return bytes;
};

export const sha256Bytes = (bytes: Uint8Array) =>
  sha256DigestFromHex(createHash("sha256").update(bytes).digest("hex"));

const canonicalPackIndexJson = (index: PackIndex) =>
  JSON.stringify({
    compression: index.compression,
    entries: index.entries.map((entry) => ({
      compressedLength: entry.compressedLength,
      compressedOffset: entry.compressedOffset,
      compression: entry.compression,
      entryDigest: entry.entryDigest,
      uncompressedSize: entry.uncompressedSize,
    })),
    formatVersion: index.formatVersion,
  });

const decodePackIndex = (source: string) =>
  Schema.decodeUnknownEffect(Schema.UnknownFromJsonString)(source).pipe(
    Effect.mapError(() =>
      packError("invalid_index", "Pack index is not valid JSON.")
    ),
    Effect.flatMap((json) =>
      Schema.decodeUnknownEffect(PackIndex)(json).pipe(
        Effect.mapError(() =>
          packError(
            "invalid_index",
            "Pack index does not match the SCIPACK schema."
          )
        )
      )
    )
  );

const indexEntriesAreSorted = (entries: readonly PackIndexEntry[]) =>
  entries.every((entry, index) => {
    const previous = entries[index - 1];
    return previous === undefined || previous.entryDigest <= entry.entryDigest;
  });

const indexEntriesFitPayload = (
  entries: readonly PackIndexEntry[],
  payloadLength: number
) =>
  entries.every(
    (entry) => entry.compressedOffset + entry.compressedLength <= payloadLength
  );

export const encodePack = Effect.fn("encodePack")(function* encodePackEffect(
  inputs: readonly PackInput[]
) {
  const compressedEntries = yield* Effect.all(
    [...new Map(inputs.map((input) => [input.digest, input])).values()]
      .toSorted((left, right) => left.digest.localeCompare(right.digest))
      .map((input) =>
        Effect.gen(function* validateAndCompressPackInputEffect() {
          const entryDigest = sha256Bytes(input.bytes);

          if (entryDigest !== input.digest) {
            return yield* packError(
              "encode_input_digest_mismatch",
              "Pack input bytes did not match the declared SHA-256 digest."
            );
          }

          const compressed = yield* gzipBytes(input.bytes);
          return { compressed, entryDigest, input };
        })
      )
  );
  const entries: PackIndexEntry[] = [];
  const payloads: Uint8Array[] = [];
  let compressedOffset = 0;

  for (const { compressed, entryDigest, input } of compressedEntries) {
    entries.push({
      compressedLength: compressed.byteLength,
      compressedOffset,
      compression: "gzip",
      entryDigest,
      uncompressedSize: input.bytes.byteLength,
    });
    payloads.push(compressed);
    compressedOffset += compressed.byteLength;
  }

  const index = {
    compression: "gzip",
    entries,
    formatVersion: 1,
  } satisfies PackIndex;
  const indexBytes = Buffer.from(canonicalPackIndexJson(index), "utf-8");
  const header = new Uint8Array(packHeaderLength);
  const headerView = new DataView(header.buffer);

  header.set(magicBytes, 0);
  headerView.setUint16(8, packFormatVersion);
  headerView.setUint16(10, 0);
  headerView.setUint32(12, indexBytes.byteLength);

  const bytes = concatBytes([header, indexBytes, ...payloads]);
  const digest = sha256Bytes(bytes);

  return { bytes, digest, index, key: packKeyFromDigest(digest) };
});

export const decodePack = Effect.fn("decodePack")(function* decodePackEffect(
  bytes: Uint8Array,
  expectedDigest?: Sha256Digest
) {
  const digest = sha256Bytes(bytes);

  if (expectedDigest !== undefined && digest !== expectedDigest) {
    return yield* packError(
      "digest_mismatch",
      "Pack bytes did not match the expected SHA-256 digest."
    );
  }

  if (
    bytes.byteLength < packHeaderLength ||
    magicBytes.some((byte, index) => bytes[index] !== byte)
  ) {
    return yield* packError(
      "invalid_header",
      "Pack header magic is not SCIPACK."
    );
  }

  const headerView = new DataView(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength
  );

  if (headerView.getUint16(8) !== packFormatVersion) {
    return yield* packError(
      "unsupported_version",
      "Pack container version is not supported."
    );
  }

  const indexLength = headerView.getUint32(12);
  const payloadOffset = packHeaderLength + indexLength;

  if (payloadOffset > bytes.byteLength) {
    return yield* packError(
      "invalid_header",
      "Pack index length exceeds object size."
    );
  }

  const index = yield* decodePackIndex(
    Buffer.from(bytes.slice(packHeaderLength, payloadOffset)).toString("utf-8")
  );
  const payloadLength = bytes.byteLength - payloadOffset;

  if (!indexEntriesAreSorted(index.entries)) {
    return yield* packError(
      "invalid_index",
      "Pack index entries are not sorted by entry digest."
    );
  }

  if (!indexEntriesFitPayload(index.entries, payloadLength)) {
    return yield* packError(
      "invalid_index",
      "Pack index entries point outside the payload."
    );
  }

  const entries: DecodedPackEntry[] = [];

  for (const entry of index.entries) {
    const decompressed = yield* gunzipBytes(
      bytes.slice(
        payloadOffset + entry.compressedOffset,
        payloadOffset + entry.compressedOffset + entry.compressedLength
      )
    );

    if (decompressed.byteLength !== entry.uncompressedSize) {
      return yield* packError(
        "entry_size_mismatch",
        "Pack payload entry size did not match the index."
      );
    }

    if (sha256Bytes(decompressed) !== entry.entryDigest) {
      return yield* packError(
        "entry_digest_mismatch",
        "Pack payload entry digest did not match the index."
      );
    }

    entries.push({ ...entry, bytes: decompressed });
  }

  return { digest, entries, index, key: packKeyFromDigest(digest) };
});

export const readPackEntry = Effect.fn("readPackEntry")(
  function* readPackEntryEffect(
    bytes: Uint8Array,
    entryDigest: Sha256Digest,
    expectedDigest?: Sha256Digest
  ) {
    const decoded = yield* decodePack(bytes, expectedDigest);
    const entry = decoded.entries.find(
      (candidate) => candidate.entryDigest === entryDigest
    );

    if (entry === undefined) {
      return yield* packError(
        "missing_entry",
        "Pack did not contain the requested entry digest."
      );
    }

    return entry;
  }
);
