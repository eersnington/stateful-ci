import { Schema } from "effect";

export class SnapshotHeaderFromManifestFailed extends Schema.TaggedErrorClass<SnapshotHeaderFromManifestFailed>()(
  "SnapshotHeaderFromManifestFailed",
  {
    message: Schema.String,
  }
) {}
