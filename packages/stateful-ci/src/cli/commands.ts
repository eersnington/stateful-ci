import { clientVersion } from "@stateful-ci/core";
import { Console, Effect } from "effect";
import { Command } from "effect/unstable/cli";

import { dashboardProgram } from "./dashboard";
import { deployProgram } from "./deploy";
import { restoreProgram } from "./restore";
import { saveProgram } from "./save";
import { failExistingConfig, writeDefaultConfig } from "./workspace-config";

export const initCommand = Command.make("init", {}, () =>
  writeDefaultConfig(process.cwd()).pipe(
    Effect.tap((path) => Console.log(`Created ${path}`)),
    Effect.catchTag("ConfigAlreadyExists", failExistingConfig)
  )
).pipe(Command.withDescription("Create a Stateful CI workspace config"));

export const restoreCommand = Command.make("restore", {}, () =>
  restoreProgram(process.env)
).pipe(
  Command.withDescription(
    "Ask the backend what workspace snapshot is safe to restore"
  )
);

export const saveCommand = Command.make("save", {}, () =>
  saveProgram(process.env)
).pipe(
  Command.withDescription(
    "Scan configured workspace paths and ask the backend to commit metadata"
  )
);

export const deployCommand = Command.make("deploy", {}, () =>
  deployProgram(process.env)
).pipe(
  Command.withDescription(
    "Provision and deploy the user-owned Cloudflare backend"
  )
);

export const dashboardCommand = Command.make(
  "dashboard",
  {},
  dashboardProgram
).pipe(Command.withDescription("Open the Stateful CI dashboard"));

export const command = Command.make("stateful-ci").pipe(
  Command.withDescription("Persistent, trust-aware CI workspace snapshots"),
  Command.withSubcommands([
    initCommand,
    deployCommand,
    restoreCommand,
    saveCommand,
    dashboardCommand,
  ])
);

export const runCli = Command.runWith(command, {
  version: clientVersion,
});
