import { Schema } from "effect";

export class RequestBodyTooLarge extends Schema.TaggedErrorClass<RequestBodyTooLarge>()(
  "RequestBodyTooLarge",
  {
    limitBytes: Schema.Number,
    message: Schema.String,
  }
) {}
