import { Console, Effect } from "effect";

export interface CliFailure {
  readonly _tag: "CliFailure";
  readonly message: string;
}

export const cliFailure = (message: string): CliFailure => ({
  _tag: "CliFailure",
  message,
});

export const failCliFailure = (error: CliFailure) =>
  Console.error(error.message).pipe(Effect.flatMap(() => Effect.fail(error)));
