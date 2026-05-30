import { RunId, SnapshotId, WorkspaceId } from "@stateful-ci/core";
import { Effect, Exit, FileSystem, Path, Schema } from "effect";

import { cliFailure } from "./failure";

const restoreSessionFile = ".stateful-ci/restore-session.json";

const RestoreSession = Schema.Struct({
  baseSnapshotId: Schema.NullOr(SnapshotId),
  runId: RunId,
  workspaceId: WorkspaceId,
});
export type RestoreSession = Schema.Schema.Type<typeof RestoreSession>;

export const writeRestoreSession = Effect.fn("writeRestoreSession")(
  function* writeRestoreSessionEffect(
    directory: string,
    session: RestoreSession
  ) {
    const fs = yield* FileSystem.FileSystem;
    const pathService = yield* Path.Path;
    const path = pathService.join(directory, restoreSessionFile);

    yield* fs
      .makeDirectory(pathService.dirname(path), { recursive: true })
      .pipe(
        Effect.mapError(() =>
          cliFailure(
            "Could not create .stateful-ci to remember backend restore authorization. Save will not run without a backend-issued workspace."
          )
        )
      );
    yield* fs
      .writeFileString(
        path,
        `${Schema.encodeUnknownSync(Schema.fromJsonString(RestoreSession))(session)}\n`
      )
      .pipe(
        Effect.mapError(() =>
          cliFailure(
            "Could not persist backend restore authorization. Save will not run without a backend-issued workspace."
          )
        )
      );
  }
);

export const clearRestoreSession = Effect.fn("clearRestoreSession")(
  function* clearRestoreSessionEffect(directory: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = (yield* Path.Path).join(directory, restoreSessionFile);

    yield* fs
      .remove(path, { force: true })
      .pipe(
        Effect.mapError(() =>
          cliFailure(
            `Could not clear stale ${restoreSessionFile}. Remove it manually before retrying restore so stale save authorization cannot be reused.`
          )
        )
      );
  }
);

export const readRestoreSession = Effect.fn("readRestoreSession")(
  function* readRestoreSessionEffect(directory: string) {
    const path = (yield* Path.Path).join(directory, restoreSessionFile);
    const source = yield* (yield* FileSystem.FileSystem)
      .readFileString(path)
      .pipe(
        Effect.mapError(() =>
          cliFailure(
            `Could not read ${restoreSessionFile}. Run stateful-ci restore before stateful-ci save so the backend can issue a workspace target.`
          )
        )
      );
    const decoded = Schema.decodeUnknownExit(
      Schema.fromJsonString(RestoreSession)
    )(source);

    return Exit.isFailure(decoded)
      ? yield* Effect.fail(
          cliFailure(
            `${restoreSessionFile} is invalid. Run stateful-ci restore again before saving.`
          )
        )
      : decoded.value;
  }
);
