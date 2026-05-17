import { createHash } from "node:crypto";
import { constants, createReadStream, createWriteStream } from "node:fs";
import {
  link,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readFile,
  readdir,
  realpath,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { finished, pipeline } from "node:stream/promises";

import type { StatefulCiConfigType } from "@stateful-ci/core";
import {
  excludedPathsForConfig,
  isBuiltInDeniedWorkspacePath,
  isUserExcludedWorkspacePath,
  workspacePathsForConfig,
} from "@stateful-ci/core";
import { Effect, Schema } from "effect";

export class SnapshotArchiveError extends Schema.TaggedErrorClass<SnapshotArchiveError>()(
  "SnapshotArchiveError",
  {
    cause: Schema.optional(Schema.String),
    message: Schema.String,
  }
) {}

export interface SnapshotArchiveSafety {
  readonly skippedByBuiltInDenylist: number;
  readonly skippedByUserExclude: number;
  readonly skippedUnsupportedType: number;
}

export interface SnapshotArchiveIdentity {
  readonly configHash: string;
  readonly gitSha: string;
  readonly repo: string;
  readonly runId: string;
  readonly workflow: string;
  readonly job: string;
}

export interface SnapshotArchiveSummary {
  readonly archiveDigest: string;
  readonly archiveKey: string;
  readonly fileCount: number;
  readonly manifestDigest: string;
  readonly manifestKey: string;
  readonly missingPaths: readonly string[];
  readonly safety: SnapshotArchiveSafety;
  readonly snapshotId: string;
  readonly totalBytes: number;
}

export interface RestoredSnapshotArchive {
  readonly fileCount: number;
  readonly totalBytes: number;
}

interface CandidateEntry {
  readonly path: string;
}

interface FileEntry {
  readonly path: string;
  readonly sha256: string;
  readonly size: number;
}

interface StagedFileEntry extends FileEntry {
  readonly stagedPath: string;
}

interface WalkResult {
  readonly entries: readonly CandidateEntry[];
  readonly missingPaths: readonly string[];
  readonly safety: SnapshotArchiveSafety;
}

interface LocalSnapshotManifest {
  readonly archiveDigest: string;
  readonly archiveKey: string;
  readonly entries: readonly FileEntry[];
  readonly formatVersion: 1;
  readonly snapshotId: string;
  readonly stats: {
    readonly fileCount: number;
    readonly totalBytes: number;
  };
}

const archiveMagic = "stateful-ci-archive-v1";
const archiveKeyPattern = /^archives\/sha256-[a-f0-9]{64}\.sciar$/u;
const manifestKeyPattern = /^manifests\/sha256-[a-f0-9]{64}\.json$/u;
const manifestFormatVersion = 1;
const maxArchiveBytes = 512 * 1024 * 1024;
const sha256DigestPattern = /^sha256:[a-f0-9]{64}$/u;

export const defaultLocalStorePath = (root: string) =>
  join(root, ".stateful-ci", "store");

const emptySafety = (): SnapshotArchiveSafety => ({
  skippedByBuiltInDenylist: 0,
  skippedByUserExclude: 0,
  skippedUnsupportedType: 0,
});

const combineWalkResults = (results: readonly WalkResult[]): WalkResult => {
  let skippedByBuiltInDenylist = 0;
  let skippedByUserExclude = 0;
  let skippedUnsupportedType = 0;

  for (const result of results) {
    skippedByBuiltInDenylist += result.safety.skippedByBuiltInDenylist;
    skippedByUserExclude += result.safety.skippedByUserExclude;
    skippedUnsupportedType += result.safety.skippedUnsupportedType;
  }

  return {
    entries: results.flatMap((result) => result.entries),
    missingPaths: results.flatMap((result) => result.missingPaths),
    safety: {
      skippedByBuiltInDenylist,
      skippedByUserExclude,
      skippedUnsupportedType,
    },
  };
};

const canonicalJson = (value: unknown) => JSON.stringify(value);

const digestFromHash = (hash: ReturnType<typeof createHash>) =>
  `sha256:${hash.digest("hex")}`;

const digestBytes = (bytes: Buffer | string) =>
  digestFromHash(createHash("sha256").update(bytes));

const objectKeyForDigest = (prefix: "archives" | "manifests", digest: string) =>
  `${prefix}/sha256-${digest.slice("sha256:".length)}${prefix === "archives" ? ".sciar" : ".json"}`;

const hasWindowsDrivePrefix = (path: string) => /^[A-Za-z]:/u.test(path);

const isNodeError = (error: unknown): error is NodeJS.ErrnoException =>
  typeof error === "object" && error !== null && "code" in error;

const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : String(error);

const ignoreError = () => void 0;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isSha256Digest = (value: unknown): value is string =>
  typeof value === "string" && sha256DigestPattern.test(value);

const normalizeRelativePath = (path: string) =>
  path
    .replaceAll("\\", "/")
    .split("/")
    .filter((part) => part.length > 0 && part !== ".")
    .join("/");

const isSafeArchivePath = (path: string) => {
  const normalized = normalizeRelativePath(path);

  return (
    path.length > 0 &&
    !path.includes("\0") &&
    !path.includes("\\") &&
    normalized.length > 0 &&
    !isAbsolute(path) &&
    !hasWindowsDrivePrefix(path) &&
    !normalized.split("/").includes("..")
  );
};

const isProtectedRestorePath = (path: string) => {
  const normalized = normalizeRelativePath(path);

  return (
    normalized === ".stateful-ci" ||
    normalized.startsWith(".stateful-ci/") ||
    isBuiltInDeniedWorkspacePath(normalized)
  );
};

const assertSafeArchivePath = (path: string) => {
  if (!isSafeArchivePath(path)) {
    throw new Error(`Archive entry path is unsafe and was rejected: ${path}`);
  }

  if (isProtectedRestorePath(path)) {
    throw new Error(
      `Archive entry path is protected and was rejected: ${path}`
    );
  }
};

const isInsideRoot = (root: string, path: string) => {
  const relativePath = relative(root, path);

  return (
    relativePath === "" ||
    (!relativePath.startsWith("..") && !isAbsolute(relativePath))
  );
};

const validateStoreKey = (key: string, pattern: RegExp, label: string) => {
  if (
    !pattern.test(key) ||
    key.includes("\0") ||
    key.includes("\\") ||
    key.split("/").includes("..") ||
    isAbsolute(key) ||
    hasWindowsDrivePrefix(key)
  ) {
    throw new Error(`${label} is not a valid Stateful CI object key: ${key}`);
  }

  return key;
};

const storePathForKey = (
  storeRoot: string,
  key: string,
  pattern: RegExp,
  label: string
) => {
  const root = resolve(storeRoot);
  const resolvedPath = resolve(root, validateStoreKey(key, pattern, label));

  if (!isInsideRoot(root, resolvedPath)) {
    throw new Error(`${label} resolved outside the local store: ${key}`);
  }

  return resolvedPath;
};

const workspacePathForEntry = (root: string, path: string) => {
  assertSafeArchivePath(path);

  const outputPath = resolve(root, ...normalizeRelativePath(path).split("/"));

  if (!isInsideRoot(resolve(root), outputPath)) {
    throw new Error(`Archive entry would write outside the workspace: ${path}`);
  }

  return outputPath;
};

const safeWorkspaceSourcePath = async (
  realRoot: string,
  root: string,
  path: string
) => {
  const normalized = normalizeRelativePath(path);
  const absolutePath = resolve(root, ...normalized.split("/"));

  if (!isInsideRoot(resolve(root), absolutePath)) {
    throw new Error(
      `Configured workspace path is outside the workspace: ${path}`
    );
  }

  const segments = normalized.split("/").filter(Boolean);
  let current = realRoot;

  for (const segment of segments.slice(0, -1)) {
    current = join(current, segment);
    const info = await lstat(current).catch((error: unknown) => {
      if (isNodeError(error) && error.code === "ENOENT") {
        return null;
      }

      throw error;
    });

    if (info === null) {
      break;
    }

    if (!info.isDirectory()) {
      throw new Error(
        `Configured workspace path parent is not a safe directory: ${current}`
      );
    }

    if (!isInsideRoot(realRoot, await realpath(current))) {
      throw new Error(
        `Configured workspace path parent resolves outside the workspace: ${current}`
      );
    }
  }

  return absolutePath;
};

const ensureSafeOutputParent = async (root: string, outputPath: string) => {
  const rootPath = await realpath(root);
  const parentRelativePath = relative(rootPath, dirname(outputPath));
  const segments = parentRelativePath.split(sep).filter(Boolean);
  let current = rootPath;

  for (const segment of segments) {
    current = join(current, segment);
    const info = await lstat(current).catch((error: unknown) => {
      if (isNodeError(error) && error.code === "ENOENT") {
        return null;
      }

      throw error;
    });

    if (info === null) {
      await mkdir(current);
      continue;
    }

    if (!info.isDirectory()) {
      throw new Error(
        `Archive entry parent is not a safe directory: ${current}`
      );
    }

    if (!isInsideRoot(rootPath, await realpath(current))) {
      throw new Error(
        `Archive entry parent resolves outside the workspace: ${current}`
      );
    }
  }
};

const ensureReplaceableOutputPath = async (outputPath: string) => {
  const info = await lstat(outputPath).catch((error: unknown) => {
    if (isNodeError(error) && error.code === "ENOENT") {
      return null;
    }

    throw error;
  });

  if (info !== null && !info.isFile()) {
    throw new Error(
      `Archive entry destination is not a replaceable file: ${outputPath}`
    );
  }
};

const applyRestoredFiles = async (
  root: string,
  tempRoot: string,
  outputPaths: readonly {
    readonly entry: FileEntry;
    readonly outputPath: string;
  }[]
) => {
  const backupRoot = await mkdtemp(join(tempRoot, "backup-"));
  const applied = [] as {
    readonly backupPath: string | null;
    readonly outputPath: string;
  }[];

  try {
    for (const { entry, outputPath } of outputPaths) {
      await ensureSafeOutputParent(root, outputPath);
      await ensureReplaceableOutputPath(outputPath);

      const sourcePath = join(tempRoot, entry.path.split("/").join(sep));
      const backupPath = join(backupRoot, entry.path.split("/").join(sep));
      const existing = await lstat(outputPath).catch((error: unknown) => {
        if (isNodeError(error) && error.code === "ENOENT") {
          return null;
        }

        throw error;
      });

      if (existing === null) {
        await rename(sourcePath, outputPath);
        applied.push({ backupPath: null, outputPath });
        continue;
      }

      await mkdir(dirname(backupPath), { recursive: true });
      await rename(outputPath, backupPath);

      try {
        await rename(sourcePath, outputPath);
        applied.push({ backupPath, outputPath });
      } catch (error) {
        await rename(backupPath, outputPath).catch(ignoreError);
        throw error;
      }
    }
  } catch (error) {
    for (const appliedFile of applied.toReversed()) {
      await rm(appliedFile.outputPath, { force: true }).catch(ignoreError);

      if (appliedFile.backupPath !== null) {
        await mkdir(dirname(appliedFile.outputPath), { recursive: true }).catch(
          ignoreError
        );
        await rename(appliedFile.backupPath, appliedFile.outputPath).catch(
          ignoreError
        );
      }
    }

    throw error;
  }
};

const parseFileEntry = (source: unknown): FileEntry => {
  if (
    !isRecord(source) ||
    typeof source.path !== "string" ||
    typeof source.size !== "number" ||
    !Number.isSafeInteger(source.size) ||
    source.size < 0 ||
    !isSha256Digest(source.sha256)
  ) {
    throw new Error("Snapshot archive contains an invalid entry.");
  }

  assertSafeArchivePath(source.path);

  return {
    path: normalizeRelativePath(source.path),
    sha256: source.sha256,
    size: source.size,
  };
};

const sha256File = async (path: string) => {
  const hash = createHash("sha256");

  for await (const chunk of createReadStream(path)) {
    hash.update(chunk);
  }

  return digestFromHash(hash);
};

const streamFileToStaging = async (
  realRoot: string,
  root: string,
  entry: CandidateEntry,
  stagedPath: string
): Promise<StagedFileEntry> => {
  const sourcePath = await safeWorkspaceSourcePath(realRoot, root, entry.path);
  const info = await lstat(sourcePath);

  if (!info.isFile() || info.nlink > 1) {
    throw new Error(
      `Configured workspace file changed or is unsupported during save: ${entry.path}`
    );
  }

  if (!isInsideRoot(realRoot, await realpath(sourcePath))) {
    throw new Error(
      `Configured workspace file resolves outside the workspace: ${entry.path}`
    );
  }

  await mkdir(dirname(stagedPath), { recursive: true });

  const hash = createHash("sha256");
  const source = await open(sourcePath, constants.O_NOFOLLOW);
  const output = await open(stagedPath, "wx").catch(async (error: unknown) => {
    await source.close();
    throw error;
  });
  let size = 0;

  try {
    const openedInfo = await source.stat();

    if (
      !openedInfo.isFile() ||
      openedInfo.nlink > 1 ||
      openedInfo.dev !== info.dev ||
      openedInfo.ino !== info.ino
    ) {
      throw new Error(
        `Configured workspace file changed or is unsupported during save: ${entry.path}`
      );
    }

    for await (const chunk of source.createReadStream()) {
      hash.update(chunk);
      size += chunk.length;
      await output.write(chunk);
    }
  } finally {
    await source.close();
    await output.close();
  }

  return {
    path: entry.path,
    sha256: digestFromHash(hash),
    size,
    stagedPath,
  };
};

const walkPath = async (
  realRoot: string,
  root: string,
  relativePath: string,
  excludes: readonly string[]
): Promise<WalkResult> => {
  const normalizedPath = normalizeRelativePath(relativePath);

  if (
    normalizedPath === ".stateful-ci" ||
    normalizedPath.startsWith(".stateful-ci/")
  ) {
    return {
      entries: [],
      missingPaths: [],
      safety: { ...emptySafety(), skippedByBuiltInDenylist: 1 },
    };
  }

  if (isBuiltInDeniedWorkspacePath(normalizedPath)) {
    return {
      entries: [],
      missingPaths: [],
      safety: { ...emptySafety(), skippedByBuiltInDenylist: 1 },
    };
  }

  if (isUserExcludedWorkspacePath(normalizedPath, excludes)) {
    return {
      entries: [],
      missingPaths: [],
      safety: { ...emptySafety(), skippedByUserExclude: 1 },
    };
  }

  const absolutePath = await safeWorkspaceSourcePath(
    realRoot,
    root,
    normalizedPath
  );
  const info = await lstat(absolutePath).catch(() => null);

  if (info === null) {
    return {
      entries: [],
      missingPaths: [normalizedPath],
      safety: emptySafety(),
    };
  }

  if (info.isFile()) {
    return info.nlink > 1
      ? {
          entries: [],
          missingPaths: [],
          safety: { ...emptySafety(), skippedUnsupportedType: 1 },
        }
      : {
          entries: [{ path: normalizedPath }],
          missingPaths: [],
          safety: emptySafety(),
        };
  }

  if (!info.isDirectory()) {
    return {
      entries: [],
      missingPaths: [],
      safety: { ...emptySafety(), skippedUnsupportedType: 1 },
    };
  }

  if (!isInsideRoot(realRoot, await realpath(absolutePath))) {
    throw new Error(
      `Configured workspace path resolves outside the workspace: ${normalizedPath}`
    );
  }

  const entries = await readdir(absolutePath);

  return combineWalkResults(
    await Promise.all(
      entries
        .toSorted((left, right) => left.localeCompare(right))
        .map((entry) =>
          walkPath(realRoot, root, `${normalizedPath}/${entry}`, excludes)
        )
    )
  );
};

const uniqueCandidates = (entries: readonly CandidateEntry[]) => {
  const paths = new Set<string>();

  return entries.filter((entry) => {
    if (paths.has(entry.path)) {
      return false;
    }

    paths.add(entry.path);
    return true;
  });
};

const assertUniqueEntries = (entries: readonly FileEntry[]) => {
  const paths = new Set<string>();

  for (const entry of entries) {
    if (paths.has(entry.path)) {
      throw new Error(
        `Snapshot archive contains duplicate entry: ${entry.path}`
      );
    }

    paths.add(entry.path);
  }
};

const writeArchive = async (
  archivePath: string,
  entries: readonly StagedFileEntry[]
) => {
  await mkdir(dirname(archivePath), { recursive: true });
  const output = createWriteStream(archivePath, { flags: "wx" });
  const publicEntries = entries.map(({ path, sha256, size }) => ({
    path,
    sha256,
    size,
  }));
  const entryJson = canonicalJson(publicEntries);

  output.write(
    `${archiveMagic}\n${Buffer.byteLength(entryJson)}\n${entryJson}`
  );

  for (const entry of entries) {
    await pipeline(createReadStream(entry.stagedPath), output, { end: false });
  }

  output.end();
  await finished(output);
};

const treeDigest = (entries: readonly FileEntry[]) =>
  `sha256:${createHash("sha256").update(canonicalJson(entries)).digest("hex")}`;

const snapshotIdFor = (identity: SnapshotArchiveIdentity, treeHash: string) =>
  `snap_${createHash("sha256")
    .update(
      [
        identity.configHash,
        identity.gitSha,
        identity.job,
        identity.repo,
        identity.runId,
        identity.workflow,
        treeHash,
      ].join("\n")
    )
    .digest("hex")
    .slice(0, 24)}`;

const manifestFromUnknown = (source: unknown): LocalSnapshotManifest => {
  if (!isRecord(source)) {
    throw new Error("Snapshot manifest is not a JSON object.");
  }

  if (
    source.formatVersion !== manifestFormatVersion ||
    typeof source.snapshotId !== "string" ||
    typeof source.archiveKey !== "string" ||
    !archiveKeyPattern.test(source.archiveKey) ||
    !isSha256Digest(source.archiveDigest) ||
    !Array.isArray(source.entries) ||
    !isRecord(source.stats) ||
    typeof source.stats.fileCount !== "number" ||
    typeof source.stats.totalBytes !== "number" ||
    !Number.isSafeInteger(source.stats.fileCount) ||
    !Number.isSafeInteger(source.stats.totalBytes) ||
    source.stats.fileCount < 0 ||
    source.stats.totalBytes < 0
  ) {
    throw new Error("Snapshot manifest does not match archive format v1.");
  }

  const entries = source.entries.map(parseFileEntry);

  assertUniqueEntries(entries);

  return {
    archiveDigest: source.archiveDigest,
    archiveKey: source.archiveKey,
    entries,
    formatVersion: manifestFormatVersion,
    snapshotId: source.snapshotId,
    stats: {
      fileCount: source.stats.fileCount,
      totalBytes: source.stats.totalBytes,
    },
  };
};

const readManifest = async (
  storeRoot: string,
  manifestKey: string,
  expectedDigest: string
) => {
  if (!isSha256Digest(expectedDigest)) {
    throw new Error("manifestDigest is not a valid Stateful CI object digest.");
  }

  const manifestBytes = await readFile(
    storePathForKey(storeRoot, manifestKey, manifestKeyPattern, "manifestKey")
  );

  if (digestBytes(manifestBytes) !== expectedDigest) {
    throw new Error(
      "Snapshot manifest digest does not match the restore response."
    );
  }

  return manifestFromUnknown(JSON.parse(manifestBytes.toString("utf-8")));
};

const readArchiveEntries = (archive: Buffer) => {
  const firstNewline = archive.indexOf("\n");

  if (
    firstNewline === -1 ||
    archive.subarray(0, firstNewline).toString() !== archiveMagic
  ) {
    throw new Error("Snapshot archive has an unsupported format.");
  }

  const secondNewline = archive.indexOf("\n", firstNewline + 1);

  if (secondNewline === -1) {
    throw new Error("Snapshot archive header is incomplete.");
  }

  const entryJsonLength = Number(
    archive.subarray(firstNewline + 1, secondNewline).toString()
  );

  if (!Number.isSafeInteger(entryJsonLength) || entryJsonLength < 0) {
    throw new Error("Snapshot archive entry length is invalid.");
  }

  const entryJsonStart = secondNewline + 1;
  const entryJsonEnd = entryJsonStart + entryJsonLength;

  if (entryJsonEnd > archive.length) {
    throw new Error("Snapshot archive entry index is truncated.");
  }

  const parsedEntries = JSON.parse(
    archive.subarray(entryJsonStart, entryJsonEnd).toString()
  );

  if (!Array.isArray(parsedEntries)) {
    throw new TypeError("Snapshot archive entry index is invalid.");
  }

  const entries = parsedEntries.map(parseFileEntry);

  assertUniqueEntries(entries);

  return { entries, payloadStart: entryJsonEnd };
};

const publishFile = async (
  tempPath: string,
  finalPath: string,
  digest: string
) => {
  await mkdir(dirname(finalPath), { recursive: true });

  try {
    await link(tempPath, finalPath);
  } catch (error) {
    if (!isNodeError(error) || error.code !== "EEXIST") {
      throw error;
    }

    if ((await sha256File(finalPath)) !== digest) {
      throw new Error(
        `Immutable snapshot object already exists with different bytes: ${finalPath}`,
        { cause: error }
      );
    }
  }
};

const archiveDigest = (path: string) => sha256File(path);

export const createSnapshotArchive = async (options: {
  readonly config: StatefulCiConfigType;
  readonly identity: SnapshotArchiveIdentity;
  readonly maxArchiveBytes?: number;
  readonly root: string;
  readonly storeRoot: string;
}): Promise<SnapshotArchiveSummary> => {
  const root = resolve(options.root);
  const realRoot = await realpath(root);
  const storeRoot = resolve(options.storeRoot);

  await mkdir(storeRoot, { recursive: true });

  const tmpRoot = await mkdtemp(join(storeRoot, ".tmp-save-"));

  try {
    const walked = combineWalkResults(
      await Promise.all(
        workspacePathsForConfig(options.config).map((path) =>
          walkPath(realRoot, root, path, excludedPathsForConfig(options.config))
        )
      )
    );
    const candidates = uniqueCandidates(walked.entries).toSorted(
      (left, right) => left.path.localeCompare(right.path)
    );
    const stagedEntries = [] as StagedFileEntry[];

    for (const [index, entry] of candidates.entries()) {
      stagedEntries.push(
        await streamFileToStaging(
          realRoot,
          root,
          entry,
          join(tmpRoot, "payload", String(index))
        )
      );
    }

    const entries = stagedEntries.map(({ path, sha256, size }) => ({
      path,
      sha256,
      size,
    }));
    const hash = treeDigest(entries);
    const snapshotId = snapshotIdFor(options.identity, hash);
    const tempArchivePath = join(tmpRoot, "archive.sciar");

    await writeArchive(tempArchivePath, stagedEntries);

    const archiveInfo = await stat(tempArchivePath);
    const archiveBytes = archiveInfo.size;

    if (archiveBytes > (options.maxArchiveBytes ?? maxArchiveBytes)) {
      throw new Error(
        `Snapshot archive is too large for the local extractor (${archiveBytes} bytes).`
      );
    }

    const digest = await archiveDigest(tempArchivePath);
    const archiveKey = validateStoreKey(
      objectKeyForDigest("archives", digest),
      archiveKeyPattern,
      "archiveKey"
    );
    let totalBytes = 0;

    for (const entry of entries) {
      totalBytes += entry.size;
    }
    const manifest: LocalSnapshotManifest = {
      archiveDigest: digest,
      archiveKey,
      entries,
      formatVersion: manifestFormatVersion,
      snapshotId,
      stats: { fileCount: entries.length, totalBytes },
    };
    const manifestText = `${canonicalJson(manifest)}\n`;
    const manifestDigest = digestBytes(manifestText);
    const manifestKey = validateStoreKey(
      objectKeyForDigest("manifests", manifestDigest),
      manifestKeyPattern,
      "manifestKey"
    );
    const tempManifestPath = join(tmpRoot, "manifest.json");

    await writeFile(tempManifestPath, manifestText, { flag: "wx" });
    await publishFile(
      tempArchivePath,
      storePathForKey(storeRoot, archiveKey, archiveKeyPattern, "archiveKey"),
      digest
    );
    await publishFile(
      tempManifestPath,
      storePathForKey(
        storeRoot,
        manifestKey,
        manifestKeyPattern,
        "manifestKey"
      ),
      manifestDigest
    );

    return {
      archiveDigest: digest,
      archiveKey,
      fileCount: entries.length,
      manifestDigest,
      manifestKey,
      missingPaths: walked.missingPaths,
      safety: walked.safety,
      snapshotId,
      totalBytes,
    };
  } finally {
    await rm(tmpRoot, { force: true, recursive: true });
  }
};

export const restoreSnapshotArchive = async (options: {
  readonly maxArchiveBytes?: number;
  readonly manifestDigest: string;
  readonly manifestKey: string;
  readonly root: string;
  readonly snapshotId: string;
  readonly storeRoot: string;
}): Promise<RestoredSnapshotArchive> => {
  const root = resolve(options.root);
  const storeRoot = resolve(options.storeRoot);
  const manifest = await readManifest(
    storeRoot,
    options.manifestKey,
    options.manifestDigest
  );

  if (manifest.snapshotId !== options.snapshotId) {
    throw new Error(
      "Snapshot manifest id does not match the restore response."
    );
  }

  const archivePath = storePathForKey(
    storeRoot,
    manifest.archiveKey,
    archiveKeyPattern,
    "archiveKey"
  );
  const archiveInfo = await stat(archivePath);

  if (archiveInfo.size > (options.maxArchiveBytes ?? maxArchiveBytes)) {
    throw new Error(
      `Snapshot archive is too large for the local extractor (${archiveInfo.size} bytes).`
    );
  }

  const archive = await readFile(archivePath);

  if (digestBytes(archive) !== manifest.archiveDigest) {
    throw new Error("Snapshot archive digest does not match the manifest.");
  }

  const { entries, payloadStart } = readArchiveEntries(archive);

  if (canonicalJson(entries) !== canonicalJson(manifest.entries)) {
    throw new Error("Snapshot archive entries do not match the manifest.");
  }

  const outputPaths = entries.map((entry) => ({
    entry,
    outputPath: workspacePathForEntry(root, entry.path),
  }));

  for (const { outputPath } of outputPaths) {
    await ensureSafeOutputParent(root, outputPath);
  }

  const tempParent = join(root, ".stateful-ci", "tmp");
  await mkdir(tempParent, { recursive: true });
  const tempRoot = await mkdtemp(join(tempParent, "restore-"));

  try {
    let offset = payloadStart;

    for (const entry of entries) {
      const nextOffset = offset + entry.size;

      if (nextOffset > archive.length) {
        throw new Error("Snapshot archive payload is truncated.");
      }

      const bytes = archive.subarray(offset, nextOffset);
      const digest = digestFromHash(createHash("sha256").update(bytes));

      if (digest !== entry.sha256) {
        throw new Error(`Snapshot archive digest mismatch for ${entry.path}.`);
      }

      const tempPath = join(tempRoot, entry.path.split("/").join(sep));

      await mkdir(dirname(tempPath), { recursive: true });
      await writeFile(tempPath, bytes, { flag: "wx" });
      offset = nextOffset;
    }

    if (offset !== archive.length) {
      throw new Error("Snapshot archive contains trailing bytes.");
    }

    await applyRestoredFiles(root, tempRoot, outputPaths);

    return manifest.stats;
  } catch (error) {
    throw new Error(
      `Snapshot restore failed before all files were applied; the workspace may be partially restored. ${error instanceof Error ? error.message : String(error)}`,
      { cause: error }
    );
  } finally {
    await rm(tempRoot, { force: true, recursive: true });
  }
};

export const createSnapshotArchiveEffect = Effect.fn("createSnapshotArchive")(
  function* createSnapshotArchiveEffect(
    options: Parameters<typeof createSnapshotArchive>[0]
  ) {
    return yield* Effect.tryPromise({
      catch: (cause) =>
        new SnapshotArchiveError({
          cause: errorMessage(cause),
          message:
            "Could not create a local snapshot archive. Check configured paths, file permissions, and available disk space.",
        }),
      try: () => createSnapshotArchive(options),
    });
  }
);

export const restoreSnapshotArchiveEffect = Effect.fn("restoreSnapshotArchive")(
  function* restoreSnapshotArchiveEffect(
    options: Parameters<typeof restoreSnapshotArchive>[0]
  ) {
    return yield* Effect.tryPromise({
      catch: (cause) =>
        new SnapshotArchiveError({
          cause: errorMessage(cause),
          message: errorMessage(cause),
        }),
      try: () => restoreSnapshotArchive(options),
    });
  }
);
