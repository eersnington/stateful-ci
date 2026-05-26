import { Schema } from "effect";

export class MetadataBackendError extends Schema.TaggedErrorClass<MetadataBackendError>()(
  "MetadataBackendError",
  {
    cause: Schema.optional(Schema.Defect),
    message: Schema.String,
    operation: Schema.String,
  }
) {}
