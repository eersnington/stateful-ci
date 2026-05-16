import { Schema } from "effect";

export class Forbidden extends Schema.TaggedErrorClass<Forbidden>()(
  "Forbidden",
  {
    message: Schema.String,
  }
) {}
