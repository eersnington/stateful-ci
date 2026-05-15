import { Schema } from "effect";

import { InvalidJsonBody } from "./invalid-json-body";
import { InvalidProtocolPayload } from "./invalid-protocol-payload";
import { MethodNotAllowed } from "./method-not-allowed";
import { RequestBodyTooLarge } from "./request-body-too-large";
import { RouteNotFound } from "./route-not-found";

export const ApiError = Schema.Union([
  InvalidJsonBody,
  InvalidProtocolPayload,
  RouteNotFound,
  MethodNotAllowed,
  RequestBodyTooLarge,
]);
export type ApiError = Schema.Schema.Type<typeof ApiError>;
