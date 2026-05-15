import { Schema } from "effect";

export class InvalidJsonBody extends Schema.TaggedErrorClass<InvalidJsonBody>()(
  "InvalidJsonBody",
  {
    message: Schema.String,
  }
) {}
