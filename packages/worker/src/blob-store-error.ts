import { Schema } from "effect";

export class BlobStoreError extends Schema.TaggedErrorClass<BlobStoreError>()(
  "BlobStoreError",
  {
    key: Schema.optional(Schema.String),
    message: Schema.String,
    reason: Schema.Literals([
      "conflict",
      "digest_mismatch",
      "io_failed",
      "missing",
      "size_mismatch",
      "unsupported",
    ]),
  }
) {}
