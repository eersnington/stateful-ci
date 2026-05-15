import { Schema } from "effect";

export class RouteNotFound extends Schema.TaggedErrorClass<RouteNotFound>()(
  "RouteNotFound",
  {
    message: Schema.String,
    method: Schema.String,
    path: Schema.String,
  }
) {}
