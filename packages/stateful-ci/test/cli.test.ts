import { once } from "node:events";
import { createServer } from "node:http";

import { NodeServices } from "@effect/platform-node";
import { assert, describe, layer } from "@effect/vitest";
import {
  configFileName,
  RestoreDeniedResponse,
  RestoreRequest,
  SaveDeniedResponse,
  SaveRequest,
} from "@stateful-ci/core";
import { Effect, Exit, FileSystem, Layer, Path, Schema } from "effect";
import { TestConsole } from "effect/testing";

import { restoreProgram, runCli, saveProgram } from "../src/cli";

const githubEnv = {
  GITHUB_ACTOR: "eersnington",
  GITHUB_EVENT_NAME: "push",
  GITHUB_JOB: "test",
  GITHUB_REF: "refs/heads/main",
  GITHUB_REPOSITORY: "eersnington/stateful-ci",
  GITHUB_RUN_ID: "123456789",
  GITHUB_SHA: "abc123",
  GITHUB_WORKFLOW: "ci.yml",
  STATEFUL_CI_OIDC_TOKEN: "oidc.jwt.token",
};

const TestLayer = Layer.mergeAll(NodeServices.layer, TestConsole.layer);

interface Workspace {
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly root: string;
}

interface ProtocolRequest {
  readonly authorization: string | undefined;
  readonly body: unknown;
  readonly method: string | undefined;
  readonly url: string | undefined;
}

const withWorkspace = <A, SetupError, SetupContext, RunError, RunContext>(
  setup: (
    workspace: Workspace
  ) => Effect.Effect<void, SetupError, SetupContext>,
  run: (workspace: Workspace) => Effect.Effect<A, RunError, RunContext>
) =>
  Effect.scoped(
    Effect.gen(function* withWorkspaceEffect() {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({
        prefix: "stateful-ci-",
      });
      const previousCwd = process.cwd();
      const workspace = { fs, path, root } satisfies Workspace;

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          process.chdir(previousCwd);
        })
      );
      yield* Effect.sync(() => {
        process.chdir(root);
      });
      yield* setup(workspace);

      return yield* run(workspace);
    })
  );

const decodeOrDie = <A>(schema: Schema.Decoder<A>, value: unknown) =>
  Effect.gen(function* decodeOrDieEffect() {
    const decoded = Schema.decodeUnknownExit(schema)(value);

    if (Exit.isFailure(decoded)) {
      return yield* Effect.die(decoded.cause);
    }

    return decoded.value;
  });

const withProtocolServer = <A, E, R>(
  handler: (request: ProtocolRequest) => Promise<Response> | Response,
  run: (url: string) => Effect.Effect<A, E, R>
) =>
  Effect.acquireUseRelease(
    Effect.promise(async () => {
      const requests: ProtocolRequest[] = [];
      const server = createServer(async (request, response) => {
        try {
          const protocolRequest = {
            authorization: request.headers.authorization,
            body: await new Response(request).json(),
            method: request.method,
            url: request.url,
          } satisfies ProtocolRequest;
          requests.push(protocolRequest);

          const handlerResponse = await handler(protocolRequest);
          response.writeHead(
            handlerResponse.status,
            Object.fromEntries(handlerResponse.headers.entries())
          );
          response.end(await handlerResponse.text());
        } catch (error) {
          response.writeHead(500, { "content-type": "text/plain" });
          response.end(String(error));
        }
      });

      server.listen(0, "127.0.0.1");
      await once(server, "listening");

      const address = server.address();

      if (address === null || typeof address === "string") {
        throw new Error("Test server did not bind to a TCP address.");
      }

      return { requests, server, url: `http://127.0.0.1:${address.port}` };
    }),
    ({ requests, url }) =>
      run(url).pipe(Effect.map((value) => ({ requests, value }) as const)),
    ({ server }) =>
      Effect.promise(async () => {
        server.close();
        await once(server, "close");
      })
  );

const withClosedProtocolServerUrl = <A, E, R>(
  run: (url: string) => Effect.Effect<A, E, R>
) =>
  Effect.acquireUseRelease(
    Effect.promise(async () => {
      const server = createServer();

      server.listen(0, "127.0.0.1");
      await once(server, "listening");

      const address = server.address();

      if (address === null || typeof address === "string") {
        throw new Error("Test server did not bind to a TCP address.");
      }

      return { server, url: `http://127.0.0.1:${address.port}` };
    }),
    ({ server, url }) =>
      Effect.promise(async () => {
        server.close();
        await once(server, "close");
        return url;
      }).pipe(Effect.flatMap(run)),
    () => Effect.void
  );

layer(TestLayer)("stateful-ci CLI", (it) => {
  describe("init", () => {
    it.effect("writes the default node config", () =>
      withWorkspace(
        ({ fs, path, root }) =>
          fs.makeDirectory(path.join(root, "workspace"), { recursive: true }),
        ({ fs, path, root }) =>
          Effect.gen(function* initWritesDefaultNodeConfigEffect() {
            const workspaceRoot = path.join(root, "workspace");

            yield* Effect.sync(() => {
              process.chdir(workspaceRoot);
            });
            yield* runCli(["init"]);

            const config = yield* fs.readFileString(
              path.join(workspaceRoot, configFileName)
            );
            assert.strictEqual(config, '{"preset":"node"}\n');
          })
      )
    );

    it.effect("does not overwrite an existing config", () =>
      withWorkspace(
        ({ fs, path, root }) =>
          fs.writeFileString(
            path.join(root, configFileName),
            '{"paths":[".turbo"]}\n'
          ),
        ({ fs, path, root }) =>
          Effect.gen(function* initDoesNotOverwriteExistingConfigEffect() {
            const configPath = path.join(root, configFileName);
            const error = yield* Effect.flip(runCli(["init"]));
            const config = yield* fs.readFileString(configPath);

            assert.strictEqual(error._tag, "ConfigAlreadyExists");
            assert.strictEqual(config, '{"paths":[".turbo"]}\n');
          })
      )
    );
  });

  describe("restore", () => {
    const setupRestoreWorkspace = ({ fs }: Workspace) =>
      fs.writeFileString(configFileName, '{"preset":"node"}\n');

    it.effect("sends a valid protocol request and handles denial", () =>
      withWorkspace(setupRestoreWorkspace, () =>
        Effect.gen(function* restoreSendsProtocolRequestEffect() {
          const { requests } = yield* withProtocolServer(
            () =>
              Response.json(
                Schema.encodeUnknownSync(RestoreDeniedResponse)({
                  decision: "denied",
                  reason: "backend_policy_not_configured",
                  save: { allowed: false },
                  trustClass: "unknown",
                })
              ),
            (url) =>
              restoreProgram({
                ...githubEnv,
                STATEFUL_CI_API_TOKEN: "test-token",
                STATEFUL_CI_API_URL: url,
              })
          );
          const [protocolRequest] = requests;

          if (protocolRequest === undefined) {
            return yield* Effect.die("Expected restore to send one request.");
          }

          const request = yield* decodeOrDie(
            RestoreRequest,
            protocolRequest.body
          );

          assert.strictEqual(requests.length, 1);
          assert.strictEqual(protocolRequest.method, "POST");
          assert.strictEqual(protocolRequest.url, "/v1/restore");
          assert.strictEqual(
            protocolRequest.authorization,
            "Bearer test-token"
          );
          assert.deepStrictEqual(request.git, {
            baseRef: null,
            headRef: null,
            headRepo: null,
            ref: "refs/heads/main",
            sha: "abc123",
          });
          assert.deepStrictEqual(request.github, {
            actor: "eersnington",
            event: "push",
            runId: "123456789",
          });
          assert.deepStrictEqual(request.workspace, {
            job: "test",
            repo: "eersnington/stateful-ci",
            workflow: "ci.yml",
          });
        })
      )
    );

    it.effect(
      "records backend workspace when denied restore may still save",
      () =>
        withWorkspace(setupRestoreWorkspace, ({ fs, path, root }) =>
          Effect.gen(function* restoreRecordsSaveAuthorizationEffect() {
            yield* withProtocolServer(
              () =>
                Response.json(
                  Schema.encodeUnknownSync(RestoreDeniedResponse)({
                    decision: "denied",
                    reason: "backend_policy_not_configured",
                    save: { allowed: true, target: "trusted/main/latest" },
                    trustClass: "trusted",
                    workspaceId: "ws_123",
                  })
                ),
              (url) =>
                restoreProgram({
                  ...githubEnv,
                  STATEFUL_CI_API_TOKEN: "test-token",
                  STATEFUL_CI_API_URL: url,
                })
            );

            const session = yield* fs.readFileString(
              path.join(root, ".stateful-ci/restore-session.json")
            );
            assert.strictEqual(
              session,
              '{"baseSnapshotId":null,"runId":"123456789","workspaceId":"ws_123"}\n'
            );
          })
        )
    );

    it.effect("fails before network calls when API URL is missing", () =>
      withWorkspace(setupRestoreWorkspace, () =>
        Effect.gen(function* restoreFailsWithoutApiUrlEffect() {
          const error = yield* Effect.flip(
            restoreProgram({
              ...githubEnv,
              STATEFUL_CI_API_TOKEN: "test-token",
            })
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(error.message, "Missing STATEFUL_CI_API_URL");
        })
      )
    );

    it.effect(
      "reports reachable backend HTTP errors separately from network failures",
      () =>
        withWorkspace(setupRestoreWorkspace, () =>
          Effect.gen(function* restoreReportsHttpErrorsEffect() {
            const error = yield* Effect.flip(
              withProtocolServer(
                () => new Response("forbidden", { status: 403 }),
                (url) =>
                  restoreProgram({
                    ...githubEnv,
                    STATEFUL_CI_API_TOKEN: "test-token",
                    STATEFUL_CI_API_URL: url,
                  })
              )
            );

            assert.strictEqual(error._tag, "CliFailure");
            assert.include(
              error.message,
              "Stateful CI backend returned HTTP 403"
            );
          })
        )
    );

    it.effect("reports unreachable backends as network failures", () =>
      withWorkspace(setupRestoreWorkspace, () =>
        Effect.gen(function* restoreReportsNetworkFailuresEffect() {
          const error = yield* Effect.flip(
            withClosedProtocolServerUrl((url) =>
              restoreProgram({
                ...githubEnv,
                STATEFUL_CI_API_TOKEN: "test-token",
                STATEFUL_CI_API_URL: url,
              })
            )
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(error.message, "Could not reach Stateful CI backend");
        })
      )
    );

    it.effect(
      "rejects protocol-invalid allowed responses before mutation",
      () =>
        withWorkspace(setupRestoreWorkspace, ({ fs, path, root }) =>
          Effect.gen(
            function* restoreRejectsInvalidProtocolBeforeMutationEffect() {
              yield* fs.makeDirectory(path.join(root, ".turbo/cache"), {
                recursive: true,
              });
              yield* fs.writeFileString(
                path.join(root, ".turbo/cache/current.txt"),
                "current"
              );

              const error = yield* Effect.flip(
                withProtocolServer(
                  () =>
                    Response.json({
                      decision: "allowed",
                      downloadPlan: [],
                      manifest: {
                        digest:
                          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        key: "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
                        size: 128,
                        snapshotId: "snap_123",
                      },
                      save: { allowed: true, target: "trusted/main/latest" },
                      snapshot: {
                        id: "snap_123",
                        manifestKey:
                          "manifests/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
                        parent: null,
                      },
                      trustClass: "trusted",
                      workspaceId: "ws_123",
                    }),
                  (url) =>
                    restoreProgram({
                      ...githubEnv,
                      STATEFUL_CI_API_TOKEN: "test-token",
                      STATEFUL_CI_API_URL: url,
                    })
                )
              );

              const current = yield* fs.readFileString(
                path.join(root, ".turbo/cache/current.txt")
              );

              assert.strictEqual(error._tag, "CliFailure");
              assert.include(error.message, "does not match protocol v1");
              assert.strictEqual(current, "current");
            }
          )
        )
    );
  });

  describe("save", () => {
    const setupSaveWorkspace = ({ fs, path, root }: Workspace) =>
      Effect.gen(function* setupSaveWorkspaceEffect() {
        yield* fs.makeDirectory(path.join(root, ".turbo/cache"), {
          recursive: true,
        });
        yield* fs.writeFileString(
          configFileName,
          '{"paths":[".turbo"],"exclude":[".turbo/cache/skip.txt"]}\n'
        );
        yield* fs.writeFileString(
          path.join(root, ".turbo/cache/result.txt"),
          "cached output"
        );
        yield* fs.writeFileString(
          path.join(root, ".turbo/cache/skip.txt"),
          "ignored output"
        );
        yield* fs.writeFileString(path.join(root, ".env"), "SECRET=value");
        yield* fs.symlink(
          "cache/result.txt",
          path.join(root, ".turbo/result-link")
        );
        yield* fs.makeDirectory(path.join(root, ".stateful-ci"), {
          recursive: true,
        });
        yield* fs.writeFileString(
          path.join(root, ".stateful-ci/restore-session.json"),
          '{"baseSnapshotId":null,"runId":"123456789","workspaceId":"ws_123"}\n'
        );
      });

    it.effect("scans configured paths and sends manifest metadata", () =>
      withWorkspace(setupSaveWorkspace, () =>
        Effect.gen(function* saveScansConfiguredPathsEffect() {
          const { requests } = yield* withProtocolServer(
            () =>
              Response.json(
                Schema.encodeUnknownSync(SaveDeniedResponse)({
                  decision: "denied",
                  reason: "backend_policy_not_configured",
                })
              ),
            (url) =>
              saveProgram({
                ...githubEnv,
                STATEFUL_CI_API_TOKEN: "test-token",
                STATEFUL_CI_API_URL: url,
                STATEFUL_CI_OIDC_TOKEN: undefined,
              })
          );
          const [protocolRequest] = requests;

          if (protocolRequest === undefined) {
            return yield* Effect.die("Expected save to send one request.");
          }

          const request = yield* decodeOrDie(SaveRequest, protocolRequest.body);

          assert.strictEqual(requests.length, 1);
          assert.strictEqual(protocolRequest.method, "POST");
          assert.strictEqual(protocolRequest.url, "/v1/save");
          assert.strictEqual(request.baseSnapshotId, null);
          assert.strictEqual(request.manifest.fileCount, 1);
          assert.deepStrictEqual(request.manifest.safety, {
            skippedByBuiltInDenylist: 0,
            skippedByUserExclude: 1,
            skippedUnsupportedType: 0,
          });
          assert.strictEqual(request.runId, "123456789");
          assert.strictEqual(request.workspaceId, "ws_123");
        })
      )
    );
  });
});
