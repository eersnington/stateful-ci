import { Schema } from "effect";

export class InvalidProtocolPayload extends Schema.TaggedErrorClass<InvalidProtocolPayload>()(
  "InvalidProtocolPayload",
  {
    message: Schema.String,
  }
) {}
