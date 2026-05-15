import { Schema } from "effect";

export class MethodNotAllowed extends Schema.TaggedErrorClass<MethodNotAllowed>()(
  "MethodNotAllowed",
  {
    allowed: Schema.Array(Schema.String),
    message: Schema.String,
    method: Schema.String,
    path: Schema.String,
  }
) {}
