import type { Sha256Digest } from "./ids";
import { sha256HexFromDigest } from "./ids";

export const smallFileThresholdBytes = 64 * 1024;
export const targetPackInputBytes = 8 * 1024 * 1024;
export const maxPackInputBytes = 16 * 1024 * 1024;
export const largeChunkSizeBytes = 4 * 1024 * 1024;
export const packMagic = "SCIPACK\0";
export const packFormatVersion = 1;
export const packHeaderLength = 16;

export interface SmallFilePackInput {
  readonly digest: Sha256Digest;
  readonly size: number;
}

export interface SmallFilePackPlan {
  readonly bucket: string;
  readonly entries: readonly SmallFilePackInput[];
  readonly uncompressedSize: number;
}

export interface LargeFileChunkRange {
  readonly length: number;
  readonly offset: number;
  readonly ordinal: number;
}

export const planSmallFilePacks = (
  inputs: readonly SmallFilePackInput[]
): readonly SmallFilePackPlan[] => {
  for (const input of inputs) {
    if (input.size > targetPackInputBytes) {
      throw new Error(
        `Small-file pack input exceeds target size: ${input.size} > ${targetPackInputBytes}. Snapshot planning was aborted because this file must be handled by the large-file chunk path.`
      );
    }
  }

  const deduped = new Map<Sha256Digest, SmallFilePackInput>();

  for (const input of inputs) {
    deduped.set(input.digest, input);
  }

  const plans: {
    bucket: string;
    entries: SmallFilePackInput[];
    uncompressedSize: number;
  }[] = [];

  for (const input of [...deduped.values()].toSorted((left, right) =>
    left.digest.localeCompare(right.digest)
  )) {
    const bucket = sha256HexFromDigest(input.digest).slice(0, 2);
    const previous = plans.at(-1);

    if (
      previous === undefined ||
      previous.bucket !== bucket ||
      previous.uncompressedSize + input.size > targetPackInputBytes
    ) {
      plans.push({ bucket, entries: [input], uncompressedSize: input.size });
      continue;
    }

    previous.entries.push(input);
    previous.uncompressedSize += input.size;
  }

  return plans;
};

export const planLargeFileChunkRanges = (
  size: number
): readonly LargeFileChunkRange[] =>
  Array.from(
    { length: Math.ceil(size / largeChunkSizeBytes) },
    (_, ordinal) => ({
      length: Math.min(
        largeChunkSizeBytes,
        size - ordinal * largeChunkSizeBytes
      ),
      offset: ordinal * largeChunkSizeBytes,
      ordinal,
    })
  );
