import { Schema } from "effect";

export class RequestBodyTooLarge extends Schema.TaggedErrorClass<RequestBodyTooLarge>()(
  "RequestBodyTooLarge",
  {
    limitBytes: Schema.Number.check(
      Schema.isInt(),
      Schema.isGreaterThanOrEqualTo(1)
    ),
    message: Schema.String,
  }
) {}
