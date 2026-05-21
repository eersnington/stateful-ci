import { describe, expect, test } from "vitest";

import {
  largeChunkSizeBytes,
  maxPackInputBytes,
  packFormatVersion,
  packHeaderLength,
  packMagic,
  planLargeFileChunkRanges,
  planSmallFilePacks,
  sha256DigestFromHex,
  smallFileThresholdBytes,
  targetPackInputBytes,
} from "./index";

const digestWithPrefix = (prefix: string, suffix: number) =>
  sha256DigestFromHex(
    `${prefix}${suffix.toString(16).padStart(64 - prefix.length, "0")}`
  );

describe("snapshot pack and chunk planning", () => {
  test("exposes the SCIPACK layout constants without runtime byte APIs", () => {
    expect(packMagic).toBe("SCIPACK\0");
    expect(packFormatVersion).toBe(1);
    expect(packHeaderLength).toBe(16);
  });

  test("small-file packing dedupes, buckets, sorts, and stays bounded", () => {
    const inputs = Array.from({ length: 140 }, (_, index) => ({
      digest: digestWithPrefix("aa", index + 1),
      size: smallFileThresholdBytes,
    }));
    const duplicate = inputs.at(5);

    if (duplicate === undefined) {
      throw new Error("test setup expected a duplicate input");
    }

    const plans = planSmallFilePacks([duplicate, ...inputs, duplicate]);
    const plannedDigests = plans.flatMap((plan) =>
      plan.entries.map((entry) => entry.digest)
    );

    expect(new Set(plannedDigests).size).toBe(inputs.length);
    expect(plans.every((plan) => plan.bucket === "aa")).toBeTruthy();
    expect(plannedDigests).toStrictEqual(plannedDigests.toSorted());
    expect(
      plans.every(
        (plan) =>
          plan.uncompressedSize <= targetPackInputBytes &&
          plan.uncompressedSize <= maxPackInputBytes
      )
    ).toBeTruthy();
  });

  test("small-file packing skips inputs larger than the target pack size", () => {
    const oversized = {
      digest: digestWithPrefix("aa", 1),
      size: targetPackInputBytes + 1,
    };
    const valid = {
      digest: digestWithPrefix("aa", 2),
      size: smallFileThresholdBytes,
    };

    const plans = planSmallFilePacks([oversized, valid]);

    expect(plans).toHaveLength(1);
    expect(plans[0]?.entries.map((entry) => entry.digest)).toStrictEqual([
      valid.digest,
    ]);
  });

  test("large-file chunk range planning uses deterministic fixed 4 MiB ranges", () => {
    const first = planLargeFileChunkRanges(largeChunkSizeBytes * 2 + 7);
    const second = planLargeFileChunkRanges(largeChunkSizeBytes * 2 + 7);

    expect(first.map((entry) => entry.length)).toStrictEqual([
      largeChunkSizeBytes,
      largeChunkSizeBytes,
      7,
    ]);
    expect(first.map((entry) => entry.ordinal)).toStrictEqual([0, 1, 2]);
    expect(first.map((entry) => entry.offset)).toStrictEqual([
      0,
      largeChunkSizeBytes,
      largeChunkSizeBytes * 2,
    ]);
    expect(first).toStrictEqual(second);
  });
});
