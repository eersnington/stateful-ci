#!/usr/bin/env node
import { pathToFileURL } from "node:url";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";

import { runCli } from "./cli/index";

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runCli(process.argv.slice(2)).pipe(
    Effect.provide(NodeServices.layer),
    NodeRuntime.runMain
  );
}
