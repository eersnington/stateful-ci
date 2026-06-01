import { once } from "node:events";
import { rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";

import { NodeServices } from "@effect/platform-node";
import { assert, describe, layer } from "@effect/vitest";
import {
  configFileName,
  largeChunkSizeBytes,
  RestoreAllowedResponse,
  RestoreDeniedResponse,
  RestoreRequest,
  CommitSaveRequest,
  CommitSaveResponse,
  PrepareSaveRequest,
  PrepareSaveResponse,
} from "@stateful-ci/core";
import { Effect, Exit, FileSystem, Layer, Path, Schema } from "effect";
import { TestConsole } from "effect/testing";

import {
  deployProgramWithRunner,
  restoreProgram,
  runCli,
  saveProgram,
} from "../src/cli/index";

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
  readonly bodyBytes: Uint8Array;
  readonly headers: Record<string, string>;
  readonly method: string | undefined;
  readonly url: string | undefined;
}

interface DeployStepCall {
  readonly args: readonly string[];
  readonly stdin?: string;
}

const deployConfigPath = new URL(
  "../../../.stateful-ci/deploy/wrangler.toml",
  import.meta.url
);
const deployConfigFsPath = fileURLToPath(deployConfigPath);

const cleanupDeployConfig = () =>
  Effect.tryPromise({
    catch: () => null,
    try: () =>
      rm(new URL("../../../.stateful-ci", import.meta.url), {
        force: true,
        recursive: true,
      }),
  }).pipe(Effect.ignore);

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
          const bodyBytes =
            request.method === "GET"
              ? new Uint8Array()
              : new Uint8Array(await new Response(request).arrayBuffer());
          const contentType = request.headers["content-type"];
          const body =
            bodyBytes.byteLength === 0 ||
            contentType === undefined ||
            !contentType.includes("application/json")
              ? null
              : JSON.parse(new TextDecoder().decode(bodyBytes));
          const protocolRequest = {
            authorization: request.headers.authorization,
            body,
            bodyBytes,
            headers: Object.fromEntries(
              Object.entries(request.headers).flatMap(([key, value]) => {
                if (typeof value === "string") {
                  return [[key, value]];
                }

                return Array.isArray(value) ? [[key, value.join(", ")]] : [];
              })
            ),
            method: request.method,
            url: request.url,
          } satisfies ProtocolRequest;
          requests.push(protocolRequest);

          const handlerResponse = await handler(protocolRequest);
          response.writeHead(
            handlerResponse.status,
            Object.fromEntries(handlerResponse.headers.entries())
          );
          response.end(Buffer.from(await handlerResponse.arrayBuffer()));
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
          assert.strictEqual(protocolRequest.authorization, undefined);
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
      "acquires a GitHub Actions OIDC token when no explicit token is provided",
      () =>
        withWorkspace(setupRestoreWorkspace, () =>
          Effect.gen(function* restoreAcquiresActionsOidcTokenEffect() {
            const { requests } = yield* withProtocolServer(
              (request) =>
                request.url?.startsWith("/oidc") === true
                  ? Response.json({ value: "acquired.jwt.token" })
                  : Response.json(
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
                  ACTIONS_ID_TOKEN_REQUEST_TOKEN: "actions-request-token",
                  ACTIONS_ID_TOKEN_REQUEST_URL: `${url}/oidc?existing=1`,
                  STATEFUL_CI_API_TOKEN: "test-token",
                  STATEFUL_CI_API_URL: url,
                  STATEFUL_CI_OIDC_AUDIENCE: "stateful-ci-test",
                  STATEFUL_CI_OIDC_TOKEN: undefined,
                })
            );
            const [oidcRequest, restoreProtocolRequest] = requests;

            if (
              oidcRequest === undefined ||
              restoreProtocolRequest === undefined
            ) {
              return yield* Effect.die("Expected OIDC and restore requests.");
            }

            const restoreRequest = yield* decodeOrDie(
              RestoreRequest,
              restoreProtocolRequest.body
            );

            assert.strictEqual(requests.length, 2);
            assert.strictEqual(oidcRequest.method, "GET");
            assert.strictEqual(
              oidcRequest.authorization,
              "Bearer actions-request-token"
            );
            assert.strictEqual(
              oidcRequest.url,
              "/oidc?existing=1&audience=stateful-ci-test"
            );
            if (restoreRequest.identity === undefined) {
              return yield* Effect.die("Expected restore identity.");
            }
            assert.strictEqual(
              restoreRequest.identity.token,
              "acquired.jwt.token"
            );
          })
        )
    );

    it.effect("fails before backend calls when OIDC cannot be acquired", () =>
      withWorkspace(setupRestoreWorkspace, () =>
        Effect.gen(function* restoreFailsWithoutOidcAcquisitionEffect() {
          const error = yield* Effect.flip(
            restoreProgram({
              ...githubEnv,
              STATEFUL_CI_API_TOKEN: "test-token",
              STATEFUL_CI_API_URL: "https://stateful-ci.example",
              STATEFUL_CI_OIDC_TOKEN: undefined,
            })
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(
            error.message,
            "Missing GitHub Actions OIDC acquisition"
          );
        })
      )
    );

    it.effect("omits OIDC identity for explicit dev auth", () =>
      withWorkspace(setupRestoreWorkspace, () =>
        Effect.gen(function* restoreOmitsOidcForDevAuthEffect() {
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
                DEV_AUTH_ENABLED: "true",
                STATEFUL_CI_API_TOKEN: "test-token",
                STATEFUL_CI_API_URL: url,
                STATEFUL_CI_DEV_AUTH_ENABLED: "false",
                STATEFUL_CI_OIDC_TOKEN: undefined,
              })
          );
          const [protocolRequest] = requests;

          if (protocolRequest === undefined) {
            return yield* Effect.die("Expected restore request.");
          }

          const request = yield* decodeOrDie(
            RestoreRequest,
            protocolRequest.body
          );

          assert.strictEqual(requests.length, 1);
          assert.strictEqual(
            protocolRequest.authorization,
            "Bearer test-token"
          );
          assert.strictEqual(request.identity, undefined);
        })
      )
    );

    it.effect("reports malformed OIDC acquisition URL as a CLI failure", () =>
      withWorkspace(setupRestoreWorkspace, () =>
        Effect.gen(function* restoreFailsWithMalformedOidcUrlEffect() {
          const error = yield* Effect.flip(
            restoreProgram({
              ...githubEnv,
              ACTIONS_ID_TOKEN_REQUEST_TOKEN: "actions-request-token",
              ACTIONS_ID_TOKEN_REQUEST_URL: "not a url",
              STATEFUL_CI_API_TOKEN: "test-token",
              STATEFUL_CI_API_URL: "https://stateful-ci.example",
              STATEFUL_CI_OIDC_TOKEN: undefined,
            })
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(error.message, "OIDC token endpoint URL was invalid");
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

    it.effect(
      "fails when denied restore allows save without a workspace target",
      () =>
        withWorkspace(setupRestoreWorkspace, ({ fs, path, root }) =>
          Effect.gen(
            function* restoreRejectsSaveWithoutWorkspaceTargetEffect() {
              const error = yield* Effect.flip(
                withProtocolServer(
                  () =>
                    Response.json(
                      Schema.encodeUnknownSync(RestoreDeniedResponse)({
                        decision: "denied",
                        reason: "backend_policy_not_configured",
                        save: { allowed: true, target: "trusted/main/latest" },
                        trustClass: "trusted",
                      })
                    ),
                  (url) =>
                    restoreProgram({
                      ...githubEnv,
                      STATEFUL_CI_API_TOKEN: "test-token",
                      STATEFUL_CI_API_URL: url,
                    })
                )
              );
              const missingSession = yield* Effect.flip(
                fs.readFileString(
                  path.join(root, ".stateful-ci/restore-session.json")
                )
              );

              assert.strictEqual(error._tag, "CliFailure");
              assert.include(error.message, "allowed save");
              assert.include(error.message, "workspaceId");
              assert.strictEqual(missingSession._tag, "PlatformError");
            }
          )
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

    it.effect("fails before network calls when API URL is invalid", () =>
      withWorkspace(setupRestoreWorkspace, () =>
        Effect.gen(function* restoreFailsWithInvalidApiUrlEffect() {
          const error = yield* Effect.flip(
            restoreProgram({
              ...githubEnv,
              STATEFUL_CI_API_TOKEN: "test-token",
              STATEFUL_CI_API_URL: "not a url",
            })
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(error.message, "STATEFUL_CI_API_URL was invalid");
        })
      )
    );

    it.effect("rejects path-prefixed API URLs before network calls", () =>
      withWorkspace(setupRestoreWorkspace, () =>
        Effect.gen(function* restoreRejectsApiUrlPathPrefixEffect() {
          const { requests, value: error } = yield* withProtocolServer(
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
              Effect.flip(
                restoreProgram({
                  ...githubEnv,
                  STATEFUL_CI_API_TOKEN: "test-token",
                  STATEFUL_CI_API_URL: `${url}/stateful-ci`,
                })
              )
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(error.message, "Worker root URL");
          assert.strictEqual(requests.length, 0);
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

    it.effect("fails before mutation when a planned object is missing", () =>
      withWorkspace(setupRestoreWorkspace, ({ fs, path, root }) =>
        Effect.gen(function* restoreFailsBeforeMutationOnMissingObjectEffect() {
          yield* fs.makeDirectory(path.join(root, ".turbo/cache"), {
            recursive: true,
          });
          yield* fs.writeFileString(
            path.join(root, ".turbo/cache/current.txt"),
            "current"
          );

          const error = yield* Effect.flip(
            withProtocolServer(
              (request) =>
                request.method === "GET"
                  ? new Response(null, { status: 404 })
                  : Response.json(
                      Schema.encodeUnknownSync(RestoreAllowedResponse)({
                        decision: "allowed",
                        downloadPlan: [
                          {
                            method: "GET",
                            object: {
                              digest:
                                "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
                              key: "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                              kind: "manifest",
                              size: 8,
                            },
                            route:
                              "/v1/objects/manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                            transport: "worker-route",
                          },
                        ],
                        manifest: {
                          digest:
                            "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
                          key: "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                          size: 8,
                          snapshotId: "snap_123",
                        },
                        save: { allowed: false },
                        snapshot: {
                          id: "snap_123",
                          manifestKey:
                            "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                          parent: null,
                        },
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
            )
          );
          const current = yield* fs.readFileString(
            path.join(root, ".turbo/cache/current.txt")
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(error.message, "returned HTTP 404");
          assert.strictEqual(current, "current");
        })
      )
    );

    it.effect("fails before mutation when a downloaded object is corrupt", () =>
      withWorkspace(setupRestoreWorkspace, ({ fs, path, root }) =>
        Effect.gen(function* restoreFailsBeforeMutationOnCorruptObjectEffect() {
          yield* fs.makeDirectory(path.join(root, ".turbo/cache"), {
            recursive: true,
          });
          yield* fs.writeFileString(
            path.join(root, ".turbo/cache/current.txt"),
            "current"
          );

          const error = yield* Effect.flip(
            withProtocolServer(
              (request) =>
                request.method === "GET"
                  ? new Response("corrupt")
                  : Response.json(
                      Schema.encodeUnknownSync(RestoreAllowedResponse)({
                        decision: "allowed",
                        downloadPlan: [
                          {
                            method: "GET",
                            object: {
                              digest:
                                "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
                              key: "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                              kind: "manifest",
                              size: 8,
                            },
                            route:
                              "/v1/objects/manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                            transport: "worker-route",
                          },
                        ],
                        manifest: {
                          digest:
                            "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
                          key: "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                          size: 8,
                          snapshotId: "snap_123",
                        },
                        save: { allowed: false },
                        snapshot: {
                          id: "snap_123",
                          manifestKey:
                            "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                          parent: null,
                        },
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
            )
          );
          const current = yield* fs.readFileString(
            path.join(root, ".turbo/cache/current.txt")
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(error.message, "size did not match");
          assert.strictEqual(current, "current");
        })
      )
    );

    it.effect(
      "rejects unsafe worker-route download plans before object fetch",
      () =>
        withWorkspace(setupRestoreWorkspace, () =>
          Effect.gen(function* restoreRejectsUnsafeWorkerRouteEffect() {
            const { requests, value: error } = yield* withProtocolServer(
              () =>
                Response.json(
                  Schema.encodeUnknownSync(RestoreAllowedResponse)({
                    decision: "allowed",
                    downloadPlan: [
                      {
                        headers: {},
                        method: "GET",
                        object: {
                          digest:
                            "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
                          key: "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                          kind: "manifest",
                          size: 8,
                        },
                        route: "https://example.test/leak-token",
                        transport: "worker-route",
                      },
                    ],
                    manifest: {
                      digest:
                        "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
                      key: "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                      size: 8,
                      snapshotId: "snap_123",
                    },
                    save: { allowed: false },
                    snapshot: {
                      id: "snap_123",
                      manifestKey:
                        "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                      parent: null,
                    },
                    trustClass: "trusted",
                    workspaceId: "ws_123",
                  })
                ),
              (url) =>
                Effect.flip(
                  restoreProgram({
                    ...githubEnv,
                    STATEFUL_CI_API_TOKEN: "test-token",
                    STATEFUL_CI_API_URL: url,
                  })
                )
            );

            assert.strictEqual(error._tag, "CliFailure");
            assert.strictEqual(requests.length, 1);
            assert.strictEqual(requests[0]?.url, "/v1/restore");
          })
        )
    );

    it.effect("clears stale save authorization when restore fails", () =>
      withWorkspace(setupRestoreWorkspace, ({ fs, path, root }) =>
        Effect.gen(function* restoreClearsStaleSessionOnFailureEffect() {
          const sessionPath = path.join(
            root,
            ".stateful-ci/restore-session.json"
          );

          yield* fs.makeDirectory(path.dirname(sessionPath), {
            recursive: true,
          });
          yield* fs.writeFileString(
            sessionPath,
            '{"baseSnapshotId":"snap_old","runId":"123456789","workspaceId":"ws_old"}\n'
          );

          yield* Effect.flip(
            withProtocolServer(
              (request) =>
                request.method === "GET"
                  ? new Response("corrupt")
                  : Response.json(
                      Schema.encodeUnknownSync(RestoreAllowedResponse)({
                        decision: "allowed",
                        downloadPlan: [
                          {
                            method: "GET",
                            object: {
                              digest:
                                "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
                              key: "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                              kind: "manifest",
                              size: 8,
                            },
                            route:
                              "/v1/objects/manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                            transport: "worker-route",
                          },
                        ],
                        manifest: {
                          digest:
                            "sha256:05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f",
                          key: "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                          size: 8,
                          snapshotId: "snap_123",
                        },
                        save: { allowed: false },
                        snapshot: {
                          id: "snap_123",
                          manifestKey:
                            "manifests/sha256/05b3abf2579a5eb66403cd78be557fd860633a1fe2103c7642030defe32c657f.json",
                          parent: null,
                        },
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
            )
          );
          const missingSession = yield* Effect.flip(
            fs.readFileString(sessionPath)
          );

          assert.strictEqual(missingSession._tag, "PlatformError");
        })
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

    it.effect("prepares, uploads missing objects, and commits", () =>
      withWorkspace(setupSaveWorkspace, ({ fs, path, root }) =>
        Effect.gen(function* saveScansConfiguredPathsEffect() {
          let oidcRequestCount = 0;
          const { requests } = yield* withProtocolServer(
            (request) => {
              if (request.url?.startsWith("/oidc") === true) {
                oidcRequestCount += 1;
                return Response.json({ value: `oidc-${oidcRequestCount}` });
              }

              if (request.url === "/v1/prepare") {
                const prepareRequest = Schema.decodeUnknownSync(
                  PrepareSaveRequest
                )(request.body);
                const [missingObject] = prepareRequest.objects;

                return Response.json(
                  Schema.encodeUnknownSync(PrepareSaveResponse)({
                    baseSnapshotId: null,
                    commitTarget: {
                      namespace:
                        "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=test",
                      refName: "trusted/main/latest",
                    },
                    decision: "allowed",
                    expectedHeadGeneration: 0,
                    missingObjects: [
                      {
                        headers: {
                          "x-stateful-ci-object-digest": missingObject.digest,
                          "x-stateful-ci-object-kind": missingObject.kind,
                          "x-stateful-ci-object-size": String(
                            missingObject.size
                          ),
                        },
                        method: "PUT",
                        object: missingObject,
                        route: `/v1/objects/${missingObject.key}`,
                        transport: "worker-route",
                      },
                    ],
                    trustClass: "trusted",
                    workspaceId: "ws_123",
                  })
                );
              }

              if (request.method === "PUT") {
                return new Response(null, { status: 204 });
              }

              return Response.json(
                Schema.encodeUnknownSync(CommitSaveResponse)({
                  decision: "denied",
                  reason: "backend_policy_not_configured",
                })
              );
            },
            (url) =>
              saveProgram({
                ...githubEnv,
                ACTIONS_ID_TOKEN_REQUEST_TOKEN: "actions-request-token",
                ACTIONS_ID_TOKEN_REQUEST_URL: `${url}/oidc`,
                STATEFUL_CI_API_TOKEN: "test-token",
                STATEFUL_CI_API_URL: url,
                STATEFUL_CI_OIDC_TOKEN: undefined,
              })
          );
          const [
            prepareOidcRequest,
            prepareProtocolRequest,
            uploadRequest,
            commitOidcRequest,
            commitProtocolRequest,
          ] = requests;

          if (
            prepareOidcRequest === undefined ||
            prepareProtocolRequest === undefined ||
            uploadRequest === undefined ||
            commitOidcRequest === undefined ||
            commitProtocolRequest === undefined
          ) {
            return yield* Effect.die(
              "Expected save to acquire OIDC, prepare, upload one object, reacquire OIDC, and commit."
            );
          }

          const prepareRequest = yield* decodeOrDie(
            PrepareSaveRequest,
            prepareProtocolRequest.body
          );
          const commitRequest = yield* decodeOrDie(
            CommitSaveRequest,
            commitProtocolRequest.body
          );
          const [missingObject] = prepareRequest.objects;

          if (missingObject === undefined) {
            return yield* Effect.die("Expected snapshot to include objects.");
          }

          const localObjectBytes = yield* fs.readFile(
            path.join(root, ".stateful-ci/store", missingObject.key)
          );

          assert.strictEqual(requests.length, 5);
          assert.strictEqual(
            prepareOidcRequest.url,
            "/oidc?audience=stateful-ci"
          );
          assert.strictEqual(
            commitOidcRequest.url,
            "/oidc?audience=stateful-ci"
          );
          assert.strictEqual(prepareProtocolRequest.method, "POST");
          assert.strictEqual(prepareProtocolRequest.url, "/v1/prepare");
          assert.strictEqual(uploadRequest.method, "PUT");
          assert.strictEqual(
            uploadRequest.url,
            `/v1/objects/${missingObject.key}`
          );
          assert.strictEqual(commitProtocolRequest.method, "POST");
          assert.strictEqual(commitProtocolRequest.url, "/v1/commit");
          assert.strictEqual(prepareRequest.objects.length > 1, true);
          assert.strictEqual(prepareRequest.github.runId, "123456789");
          assert.strictEqual(
            prepareRequest.idempotencyKey,
            `run-123456789-save-${prepareRequest.manifest.snapshotId}`
          );
          assert.strictEqual(commitRequest.workspaceId, "ws_123");
          assert.strictEqual(commitRequest.expectedHeadGeneration, 0);
          assert.deepStrictEqual(
            [prepareRequest.identity?.token, commitRequest.identity?.token],
            ["oidc-1", "oidc-2"]
          );
          assert.deepStrictEqual(commitRequest.objects, prepareRequest.objects);
          assert.strictEqual(
            uploadRequest.headers["x-stateful-ci-object-digest"],
            missingObject.digest
          );
          assert.strictEqual(
            uploadRequest.headers["x-stateful-ci-object-kind"],
            missingObject.kind
          );
          assert.strictEqual(
            uploadRequest.headers["x-stateful-ci-object-size"],
            String(missingObject.size)
          );
          assert.deepStrictEqual(uploadRequest.bodyBytes, localObjectBytes);
        })
      )
    );

    it.effect("omits OIDC identity for explicit dev auth", () =>
      withWorkspace(setupSaveWorkspace, () =>
        Effect.gen(function* saveOmitsOidcForDevAuthEffect() {
          const { requests } = yield* withProtocolServer(
            (request) => {
              if (request.url === "/v1/prepare") {
                return Response.json(
                  Schema.encodeUnknownSync(PrepareSaveResponse)({
                    baseSnapshotId: null,
                    commitTarget: {
                      namespace:
                        "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=test",
                      refName: "internal/main/latest",
                    },
                    decision: "allowed",
                    expectedHeadGeneration: 0,
                    missingObjects: [],
                    trustClass: "internal",
                    workspaceId: "ws_123",
                  })
                );
              }

              return Response.json(
                Schema.encodeUnknownSync(CommitSaveResponse)({
                  decision: "denied",
                  reason: "backend_policy_not_configured",
                })
              );
            },
            (url) =>
              saveProgram({
                ...githubEnv,
                STATEFUL_CI_API_TOKEN: "test-token",
                STATEFUL_CI_API_URL: url,
                STATEFUL_CI_DEV_AUTH_ENABLED: "true",
                STATEFUL_CI_OIDC_TOKEN: undefined,
              })
          );
          const [prepareProtocolRequest, commitProtocolRequest] = requests;

          if (
            prepareProtocolRequest === undefined ||
            commitProtocolRequest === undefined
          ) {
            return yield* Effect.die("Expected prepare and commit requests.");
          }

          const prepareRequest = yield* decodeOrDie(
            PrepareSaveRequest,
            prepareProtocolRequest.body
          );
          const commitRequest = yield* decodeOrDie(
            CommitSaveRequest,
            commitProtocolRequest.body
          );

          assert.strictEqual(requests.length, 2);
          assert.strictEqual(
            prepareProtocolRequest.authorization,
            "Bearer test-token"
          );
          assert.strictEqual(
            commitProtocolRequest.authorization,
            "Bearer test-token"
          );
          assert.strictEqual(prepareRequest.identity, undefined);
          assert.strictEqual(commitRequest.identity, undefined);
        })
      )
    );

    it.effect("scopes idempotency keys to the produced snapshot", () =>
      withWorkspace(setupSaveWorkspace, () =>
        Effect.gen(function* saveScopesIdempotencyKeysEffect() {
          const preparedRequests: PrepareSaveRequest[] = [];

          yield* withProtocolServer(
            (request) => {
              const prepareRequest = Schema.decodeUnknownSync(
                PrepareSaveRequest
              )(request.body);
              preparedRequests.push(prepareRequest);

              return Response.json(
                Schema.encodeUnknownSync(PrepareSaveResponse)({
                  decision: "denied",
                  reason: "backend_policy_not_configured",
                  trustClass: "trusted",
                  workspaceId: "ws_123",
                })
              );
            },
            (url) =>
              Effect.gen(function* runSameRunDifferentJobsEffect() {
                const env = {
                  ...githubEnv,
                  STATEFUL_CI_API_URL: url,
                };

                yield* saveProgram({ ...env, GITHUB_JOB: "test" });
                yield* saveProgram({ ...env, GITHUB_JOB: "lint" });
              })
          );
          const [testJobRequest, lintJobRequest] = preparedRequests;

          if (testJobRequest === undefined || lintJobRequest === undefined) {
            return yield* Effect.die("Expected two prepare-save requests.");
          }

          assert.strictEqual(testJobRequest.github.runId, "123456789");
          assert.strictEqual(lintJobRequest.github.runId, "123456789");
          assert.strictEqual(testJobRequest.workspace.job, "test");
          assert.strictEqual(lintJobRequest.workspace.job, "lint");
          assert.notStrictEqual(
            testJobRequest.idempotencyKey,
            lintJobRequest.idempotencyKey
          );
          assert.strictEqual(
            testJobRequest.idempotencyKey,
            `run-123456789-save-${testJobRequest.manifest.snapshotId}`
          );
          assert.strictEqual(
            lintJobRequest.idempotencyKey,
            `run-123456789-save-${lintJobRequest.manifest.snapshotId}`
          );
        })
      )
    );

    it.effect(
      "uploads manifest, pack, and chunk objects through prepare plans",
      () =>
        withWorkspace(
          ({ fs, path, root }) =>
            Effect.gen(function* setupChunkedSaveWorkspaceEffect() {
              yield* fs.makeDirectory(path.join(root, ".turbo/cache"), {
                recursive: true,
              });
              yield* fs.makeDirectory(path.join(root, "target"), {
                recursive: true,
              });
              yield* fs.writeFileString(
                configFileName,
                '{"paths":[".turbo","target"]}\n'
              );
              yield* fs.writeFileString(
                path.join(root, ".turbo/cache/result.txt"),
                "cached output"
              );
              yield* Effect.promise(() =>
                writeFile(
                  path.join(root, "target/large.bin"),
                  Buffer.alloc(largeChunkSizeBytes + 1, 4)
                )
              );
              yield* fs.makeDirectory(path.join(root, ".stateful-ci"), {
                recursive: true,
              });
              yield* fs.writeFileString(
                path.join(root, ".stateful-ci/restore-session.json"),
                '{"baseSnapshotId":null,"runId":"123456789","workspaceId":"ws_123"}\n'
              );
            }),
          () =>
            Effect.gen(function* saveUploadsPackAndChunkObjectsEffect() {
              const uploadedKinds: string[] = [];
              const preparedRequests: PrepareSaveRequest[] = [];
              const committedRequests: CommitSaveRequest[] = [];

              yield* withProtocolServer(
                (request) => {
                  if (request.url === "/v1/prepare") {
                    const preparedRequest = Schema.decodeUnknownSync(
                      PrepareSaveRequest
                    )(request.body);
                    preparedRequests.push(preparedRequest);

                    return Response.json(
                      Schema.encodeUnknownSync(PrepareSaveResponse)({
                        baseSnapshotId: null,
                        commitTarget: {
                          namespace:
                            "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=test",
                          refName: "trusted/main/latest",
                        },
                        decision: "allowed",
                        expectedHeadGeneration: 0,
                        missingObjects: preparedRequest.objects.map(
                          (object) => ({
                            headers: {
                              "x-stateful-ci-object-digest": object.digest,
                              "x-stateful-ci-object-kind": object.kind,
                              "x-stateful-ci-object-size": String(object.size),
                            },
                            method: "PUT" as const,
                            object,
                            route: `/v1/objects/${object.key}`,
                            transport: "worker-route" as const,
                          })
                        ),
                        trustClass: "trusted",
                        workspaceId: "ws_123",
                      })
                    );
                  }

                  if (request.method === "PUT") {
                    const key = request.url?.slice("/v1/objects/".length);
                    const object = preparedRequests
                      .at(-1)
                      ?.objects.find((candidate) => candidate.key === key);

                    if (object !== undefined) {
                      uploadedKinds.push(object.kind);
                    }

                    return new Response(null, { status: 204 });
                  }

                  if (request.url === "/v1/commit") {
                    const committedRequest = Schema.decodeUnknownSync(
                      CommitSaveRequest
                    )(request.body);
                    committedRequests.push(committedRequest);

                    return Response.json(
                      Schema.encodeUnknownSync(CommitSaveResponse)({
                        decision: "committed",
                        headGeneration: 1,
                        snapshotId: committedRequest.manifest.snapshotId,
                        workspaceId: "ws_123",
                      })
                    );
                  }

                  return new Response(null, { status: 404 });
                },
                (url) =>
                  saveProgram({
                    ...githubEnv,
                    STATEFUL_CI_API_URL: url,
                  })
              );

              const [preparedRequest] = preparedRequests;
              const [committedRequest] = committedRequests;

              if (
                preparedRequest === undefined ||
                committedRequest === undefined
              ) {
                return yield* Effect.die(
                  "Expected prepare and commit requests."
                );
              }

              assert.deepStrictEqual([...new Set(uploadedKinds)].toSorted(), [
                "chunk",
                "manifest",
                "pack",
              ]);
              assert.deepStrictEqual(
                committedRequest.objects,
                preparedRequest.objects
              );
            })
        )
    );

    it.effect(
      "reports stale generation conflicts without retrying commit",
      () =>
        withWorkspace(setupSaveWorkspace, () =>
          Effect.gen(function* saveReportsGenerationConflictEffect() {
            const { requests } = yield* withProtocolServer(
              (request) => {
                if (request.url === "/v1/prepare") {
                  return Response.json(
                    Schema.encodeUnknownSync(PrepareSaveResponse)({
                      baseSnapshotId: null,
                      commitTarget: {
                        namespace:
                          "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=test",
                        refName: "trusted/main/latest",
                      },
                      decision: "allowed",
                      expectedHeadGeneration: 0,
                      missingObjects: [],
                      trustClass: "trusted",
                      workspaceId: "ws_123",
                    })
                  );
                }

                if (request.url === "/v1/commit") {
                  return Response.json(
                    Schema.encodeUnknownSync(CommitSaveResponse)({
                      actualHeadGeneration: 1,
                      decision: "conflict",
                      reason: "head_generation_mismatch",
                    })
                  );
                }

                return new Response(null, { status: 404 });
              },
              (url) =>
                saveProgram({
                  ...githubEnv,
                  STATEFUL_CI_API_URL: url,
                })
            );
            const logs = yield* TestConsole.logLines;

            assert.strictEqual(
              requests.filter((request) => request.url === "/v1/commit").length,
              1
            );
            assert.strictEqual(
              requests.some((request) => request.method === "PUT"),
              false
            );
            assert.strictEqual(
              logs.at(-1),
              "Save conflicted: expected head changed to generation 1."
            );
          })
        )
    );

    it.effect("accepts idempotent commit replay responses", () =>
      withWorkspace(setupSaveWorkspace, () =>
        Effect.gen(function* saveAcceptsIdempotentCommitReplayEffect() {
          const committedRequests: CommitSaveRequest[] = [];

          yield* withProtocolServer(
            (request) => {
              if (request.url === "/v1/prepare") {
                return Response.json(
                  Schema.encodeUnknownSync(PrepareSaveResponse)({
                    baseSnapshotId: null,
                    commitTarget: {
                      namespace:
                        "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=test",
                      refName: "trusted/main/latest",
                    },
                    decision: "allowed",
                    expectedHeadGeneration: 0,
                    missingObjects: [],
                    trustClass: "trusted",
                    workspaceId: "ws_123",
                  })
                );
              }

              if (request.url === "/v1/commit") {
                const committedRequest = Schema.decodeUnknownSync(
                  CommitSaveRequest
                )(request.body);
                committedRequests.push(committedRequest);

                return Response.json(
                  Schema.encodeUnknownSync(CommitSaveResponse)({
                    decision: "idempotent",
                    headGeneration: 1,
                    snapshotId: committedRequest.manifest.snapshotId,
                    workspaceId: "ws_123",
                  })
                );
              }

              return new Response(null, { status: 404 });
            },
            (url) =>
              saveProgram({
                ...githubEnv,
                STATEFUL_CI_API_URL: url,
              })
          );
          const [committedRequest] = committedRequests;

          if (committedRequest === undefined) {
            return yield* Effect.die("Expected commit request.");
          }

          const logs = yield* TestConsole.logLines;

          assert.strictEqual(
            logs.at(-1),
            `Save already committed: snapshot ${committedRequest.manifest.snapshotId} for workspace ws_123. Head generation: 1.`
          );
        })
      )
    );

    it.effect("leaves fork pull request save policy to the backend", () =>
      withWorkspace(setupSaveWorkspace, () =>
        Effect.gen(function* forkPullRequestSaveUsesBackendPolicyEffect() {
          const { requests } = yield* withProtocolServer(
            (request) => {
              const prepareRequest = Schema.decodeUnknownSync(
                PrepareSaveRequest
              )(request.body);

              assert.deepStrictEqual(prepareRequest.git, {
                baseRef: "main",
                headRef: "feature",
                headRepo: "contributor/stateful-ci",
                ref: "refs/pull/12/merge",
                sha: "abc123",
              });

              return Response.json(
                Schema.encodeUnknownSync(PrepareSaveResponse)({
                  decision: "denied",
                  reason: "external_save_disabled",
                  trustClass: "external",
                  workspaceId: "ws_123",
                })
              );
            },
            (url) =>
              saveProgram({
                ...githubEnv,
                GITHUB_BASE_REF: "main",
                GITHUB_EVENT_NAME: "pull_request",
                GITHUB_HEAD_REF: "feature",
                GITHUB_HEAD_REPOSITORY: "contributor/stateful-ci",
                GITHUB_REF: "refs/pull/12/merge",
                STATEFUL_CI_API_URL: url,
              })
          );

          assert.strictEqual(requests.length, 1);
          assert.strictEqual(requests[0]?.url, "/v1/prepare");
        })
      )
    );

    it.effect("aborts before commit when object upload fails", () =>
      withWorkspace(setupSaveWorkspace, () =>
        Effect.gen(function* saveAbortsBeforeCommitOnUploadFailureEffect() {
          const error = yield* Effect.flip(
            withProtocolServer(
              (request) => {
                if (request.url === "/v1/prepare") {
                  const prepareRequest = Schema.decodeUnknownSync(
                    PrepareSaveRequest
                  )(request.body);
                  const [missingObject] = prepareRequest.objects;

                  return Response.json(
                    Schema.encodeUnknownSync(PrepareSaveResponse)({
                      baseSnapshotId: null,
                      commitTarget: {
                        namespace:
                          "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=test",
                        refName: "trusted/main/latest",
                      },
                      decision: "allowed",
                      expectedHeadGeneration: 0,
                      missingObjects: [
                        {
                          headers: {
                            "x-stateful-ci-object-digest": missingObject.digest,
                            "x-stateful-ci-object-kind": missingObject.kind,
                            "x-stateful-ci-object-size": String(
                              missingObject.size
                            ),
                          },
                          method: "PUT",
                          object: missingObject,
                          route: `/v1/objects/${missingObject.key}`,
                          transport: "worker-route",
                        },
                      ],
                      trustClass: "trusted",
                      workspaceId: "ws_123",
                    })
                  );
                }

                if (request.method === "PUT") {
                  return new Response(null, { status: 500 });
                }

                return Response.json({ unexpected: true });
              },
              (url) =>
                saveProgram({
                  ...githubEnv,
                  STATEFUL_CI_API_TOKEN: "test-token",
                  STATEFUL_CI_API_URL: url,
                })
            )
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(error.message, "returned HTTP 500");
        })
      )
    );

    it.effect("rejects unsafe worker-route upload plans before upload", () =>
      withWorkspace(setupSaveWorkspace, () =>
        Effect.gen(function* saveRejectsUnsafeWorkerRouteEffect() {
          const { requests, value: error } = yield* withProtocolServer(
            (request) => {
              const prepareRequest = Schema.decodeUnknownSync(
                PrepareSaveRequest
              )(request.body);
              const [missingObject] = prepareRequest.objects;

              return Response.json(
                Schema.encodeUnknownSync(PrepareSaveResponse)({
                  baseSnapshotId: null,
                  commitTarget: {
                    namespace:
                      "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=test",
                    refName: "trusted/main/latest",
                  },
                  decision: "allowed",
                  expectedHeadGeneration: 0,
                  missingObjects: [
                    {
                      headers: {
                        "x-stateful-ci-object-digest": missingObject.digest,
                        "x-stateful-ci-object-kind": missingObject.kind,
                        "x-stateful-ci-object-size": String(missingObject.size),
                      },
                      method: "PUT",
                      object: missingObject,
                      route: "https://example.test/leak-token",
                      transport: "worker-route",
                    },
                  ],
                  trustClass: "trusted",
                  workspaceId: "ws_123",
                })
              );
            },
            (url) =>
              Effect.flip(
                saveProgram({
                  ...githubEnv,
                  STATEFUL_CI_API_TOKEN: "test-token",
                  STATEFUL_CI_API_URL: url,
                })
              )
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.strictEqual(requests.length, 1);
          assert.strictEqual(requests[0]?.url, "/v1/prepare");
        })
      )
    );

    it.effect("saves to and restores from a remote test backend", () =>
      withWorkspace(setupSaveWorkspace, ({ fs, path, root }) =>
        Effect.gen(function* saveThenRestoreRemoteSnapshotEffect() {
          const storedObjects = new Map<string, Uint8Array>();
          let committedRequest: CommitSaveRequest | null = null;

          yield* withProtocolServer(
            (request) => {
              if (request.url === "/v1/prepare") {
                const prepareRequest = Schema.decodeUnknownSync(
                  PrepareSaveRequest
                )(request.body);

                return Response.json(
                  Schema.encodeUnknownSync(PrepareSaveResponse)({
                    baseSnapshotId: null,
                    commitTarget: {
                      namespace:
                        "repo=eersnington/stateful-ci/workflow=ci.yml/job=test/config=test",
                      refName: "trusted/main/latest",
                    },
                    decision: "allowed",
                    expectedHeadGeneration: 0,
                    missingObjects: prepareRequest.objects.map((object) => ({
                      headers: {
                        "x-stateful-ci-object-digest": object.digest,
                        "x-stateful-ci-object-kind": object.kind,
                        "x-stateful-ci-object-size": String(object.size),
                        "x-stateful-ci-transfer-token": "test-transfer-token",
                      },
                      method: "PUT" as const,
                      object,
                      route: `/v1/objects/${object.key}`,
                      transport: "worker-route" as const,
                    })),
                    trustClass: "trusted",
                    workspaceId: "ws_123",
                  })
                );
              }

              if (request.method === "PUT") {
                const key = request.url?.slice("/v1/objects/".length);

                if (key !== undefined) {
                  storedObjects.set(key, request.bodyBytes);
                }

                return new Response(null, { status: 204 });
              }

              if (request.url === "/v1/commit") {
                committedRequest = Schema.decodeUnknownSync(CommitSaveRequest)(
                  request.body
                );

                return Response.json(
                  Schema.encodeUnknownSync(CommitSaveResponse)({
                    decision: "committed",
                    headGeneration: 1,
                    snapshotId: committedRequest.manifest.snapshotId,
                    workspaceId: "ws_123",
                  })
                );
              }

              if (request.url === "/v1/restore") {
                if (committedRequest === null) {
                  return Response.json(
                    Schema.encodeUnknownSync(RestoreDeniedResponse)({
                      decision: "denied",
                      reason: "no_compatible_snapshot",
                      save: { allowed: false },
                      trustClass: "trusted",
                    })
                  );
                }

                return Response.json(
                  Schema.encodeUnknownSync(RestoreAllowedResponse)({
                    decision: "allowed",
                    downloadPlan: committedRequest.objects.map((object) => ({
                      headers: {
                        "x-stateful-ci-object-digest": object.digest,
                        "x-stateful-ci-object-kind": object.kind,
                        "x-stateful-ci-object-size": String(object.size),
                        "x-stateful-ci-transfer-token": "test-transfer-token",
                      },
                      method: "GET" as const,
                      object,
                      route: `/v1/objects/${object.key}`,
                      transport: "worker-route" as const,
                    })),
                    manifest: committedRequest.manifest,
                    save: { allowed: true, target: "trusted/main/latest" },
                    snapshot: {
                      id: committedRequest.manifest.snapshotId,
                      manifestKey: committedRequest.manifest.key,
                      parent: null,
                    },
                    trustClass: "trusted",
                    workspaceId: "ws_123",
                  })
                );
              }

              if (request.method === "GET") {
                const key = request.url?.slice("/v1/objects/".length);
                const bytes =
                  key === undefined ? undefined : storedObjects.get(key);

                return bytes === undefined
                  ? new Response(null, { status: 404 })
                  : new Response(bytes);
              }

              return new Response(null, { status: 404 });
            },
            (url) =>
              Effect.gen(function* runRemoteRoundTripEffect() {
                const env = {
                  ...githubEnv,
                  STATEFUL_CI_API_URL: url,
                };

                yield* saveProgram(env);
                yield* fs.writeFileString(
                  path.join(root, ".turbo/cache/result.txt"),
                  "stale output"
                );
                yield* fs.writeFileString(
                  path.join(root, ".turbo/cache/stale.txt"),
                  "stale"
                );

                yield* restoreProgram(env);

                const restored = yield* fs.readFileString(
                  path.join(root, ".turbo/cache/result.txt")
                );
                const stale = yield* Effect.flip(
                  fs.readFileString(path.join(root, ".turbo/cache/stale.txt"))
                );

                assert.strictEqual(restored, "cached output");
                assert.strictEqual(stale._tag, "PlatformError");
              })
          );
        })
      )
    );
  });

  describe("deploy", () => {
    const deployEnv = {
      STATEFUL_CI_ALLOWED_REPOSITORIES: "eersnington/stateful-ci",
      STATEFUL_CI_TRANSFER_SECRET: "test-transfer-secret",
    };

    it.effect("generates deploy config and wires secrets", () =>
      Effect.gen(function* deployGeneratesConfigAndWiresSecretsEffect() {
        yield* Effect.addFinalizer(cleanupDeployConfig);
        const calls: DeployStepCall[] = [];
        const runner = ({ args, stdin }: DeployStepCall) => {
          calls.push(stdin === undefined ? { args } : { args, stdin });

          return Effect.succeed({
            stderr: "",
            stdout:
              args.join(" ") === "wrangler d1 list --json"
                ? JSON.stringify([
                    {
                      name: "stateful-ci-metadata",
                      uuid: "11111111-1111-1111-1111-111111111111",
                    },
                  ])
                : "",
          });
        };

        yield* deployProgramWithRunner(deployEnv, runner);

        const fs = yield* FileSystem.FileSystem;
        const config = yield* fs.readFileString(deployConfigFsPath);

        assert.deepStrictEqual(
          calls.map((call) => call.args),
          [
            ["wrangler", "d1", "create", "stateful-ci-metadata"],
            ["wrangler", "d1", "list", "--json"],
            ["wrangler", "r2", "bucket", "create", "stateful-ci-objects"],
            [
              "wrangler",
              "d1",
              "migrations",
              "apply",
              "stateful-ci-metadata",
              "--remote",
              "--config",
              deployConfigFsPath,
            ],
            [
              "wrangler",
              "secret",
              "put",
              "STATEFUL_CI_TRANSFER_SECRET",
              "--config",
              deployConfigFsPath,
            ],
            ["wrangler", "deploy", "--config", deployConfigFsPath],
          ]
        );
        assert.strictEqual(calls[4]?.stdin, "test-transfer-secret\n");
        assert.include(
          config,
          'ALLOWED_REPOSITORIES = "eersnington/stateful-ci"'
        );
        assert.include(config, 'OIDC_AUDIENCE = "stateful-ci"');
        assert.include(config, 'bucket_name = "stateful-ci-objects"');
        assert.include(
          config,
          'database_id = "11111111-1111-1111-1111-111111111111"'
        );
        assert.notInclude(config, "STATEFUL_CI_TRANSFER_SECRET");
      })
    );

    it.effect("reuses existing R2 buckets confirmed by JSON list", () =>
      Effect.gen(function* deployReusesExistingR2BucketEffect() {
        yield* Effect.addFinalizer(cleanupDeployConfig);
        const calls: DeployStepCall[] = [];
        const runner = ({ args, stdin }: DeployStepCall) => {
          calls.push(stdin === undefined ? { args } : { args, stdin });
          const command = args.join(" ");

          if (command === "wrangler r2 bucket create stateful-ci-objects") {
            return Effect.fail({
              _tag: "CliFailure" as const,
              message: "create failed",
            });
          }

          if (command === "wrangler d1 list --json") {
            return Effect.succeed({
              stderr: "",
              stdout: JSON.stringify([
                {
                  name: "stateful-ci-metadata",
                  uuid: "11111111-1111-1111-1111-111111111111",
                },
              ]),
            });
          }

          if (command === "wrangler r2 bucket list --json") {
            return Effect.succeed({
              stderr: "",
              stdout: JSON.stringify([{ name: "stateful-ci-objects" }]),
            });
          }

          return Effect.succeed({
            stderr: "",
            stdout: "",
          });
        };

        yield* deployProgramWithRunner(deployEnv, runner);

        assert.deepStrictEqual(calls.map((call) => call.args).slice(0, 5), [
          ["wrangler", "d1", "create", "stateful-ci-metadata"],
          ["wrangler", "d1", "list", "--json"],
          ["wrangler", "r2", "bucket", "create", "stateful-ci-objects"],
          ["wrangler", "r2", "bucket", "list", "--json"],
          [
            "wrangler",
            "d1",
            "migrations",
            "apply",
            "stateful-ci-metadata",
            "--remote",
            "--config",
            deployConfigFsPath,
          ],
        ]);
      })
    );

    it.effect("reuses existing D1 databases confirmed by JSON list", () =>
      Effect.gen(function* deployReusesExistingD1DatabaseEffect() {
        yield* Effect.addFinalizer(cleanupDeployConfig);
        const calls: DeployStepCall[] = [];
        const runner = ({ args, stdin }: DeployStepCall) => {
          calls.push(stdin === undefined ? { args } : { args, stdin });

          if (args.join(" ") === "wrangler d1 create stateful-ci-metadata") {
            return Effect.fail({
              _tag: "CliFailure" as const,
              message: "create failed",
            });
          }

          return Effect.succeed({
            stderr: "",
            stdout:
              args.join(" ") === "wrangler d1 list --json"
                ? JSON.stringify([
                    {
                      name: "stateful-ci-metadata",
                      uuid: "11111111-1111-1111-1111-111111111111",
                    },
                  ])
                : "",
          });
        };

        yield* deployProgramWithRunner(deployEnv, runner);

        assert.deepStrictEqual(calls.map((call) => call.args).slice(0, 4), [
          ["wrangler", "d1", "create", "stateful-ci-metadata"],
          ["wrangler", "d1", "list", "--json"],
          ["wrangler", "r2", "bucket", "create", "stateful-ci-objects"],
          [
            "wrangler",
            "d1",
            "migrations",
            "apply",
            "stateful-ci-metadata",
            "--remote",
            "--config",
            deployConfigFsPath,
          ],
        ]);
      })
    );

    it.effect(
      "fails when R2 create fails and JSON list does not confirm it",
      () =>
        Effect.gen(function* deployRejectsUnconfirmedR2ReuseEffect() {
          yield* Effect.addFinalizer(cleanupDeployConfig);
          const calls: DeployStepCall[] = [];
          const runner = ({ args, stdin }: DeployStepCall) => {
            calls.push(stdin === undefined ? { args } : { args, stdin });
            const command = args.join(" ");

            if (command === "wrangler r2 bucket create stateful-ci-objects") {
              return Effect.fail({
                _tag: "CliFailure" as const,
                message: "create failed",
              });
            }

            if (command === "wrangler d1 list --json") {
              return Effect.succeed({
                stderr: "",
                stdout: JSON.stringify([
                  {
                    name: "stateful-ci-metadata",
                    uuid: "11111111-1111-1111-1111-111111111111",
                  },
                ]),
              });
            }

            if (command === "wrangler r2 bucket list --json") {
              return Effect.succeed({ stderr: "", stdout: JSON.stringify([]) });
            }

            return Effect.succeed({
              stderr: "",
              stdout: "",
            });
          };
          const error = yield* Effect.flip(
            deployProgramWithRunner(deployEnv, runner)
          );

          assert.strictEqual(error._tag, "CliFailure");
          assert.include(error.message, "did not confirm");
          assert.deepStrictEqual(
            calls.map((call) => call.args),
            [
              ["wrangler", "d1", "create", "stateful-ci-metadata"],
              ["wrangler", "d1", "list", "--json"],
              ["wrangler", "r2", "bucket", "create", "stateful-ci-objects"],
              ["wrangler", "r2", "bucket", "list", "--json"],
            ]
          );
        })
    );

    it.effect("fails before provisioning when deploy secrets are missing", () =>
      Effect.gen(function* deployFailsBeforeProvisioningWithoutSecretsEffect() {
        const calls: DeployStepCall[] = [];
        const error = yield* Effect.flip(
          deployProgramWithRunner(
            { STATEFUL_CI_ALLOWED_REPOSITORIES: "eersnington/stateful-ci" },
            (call) => {
              calls.push(call);
              return Effect.succeed({ stderr: "", stdout: "" });
            }
          )
        );

        assert.strictEqual(error._tag, "CliFailure");
        assert.include(error.message, "Missing STATEFUL_CI_TRANSFER_SECRET");
        assert.deepStrictEqual(calls, []);
      })
    );

    it.effect("requires an allowed repository list", () =>
      Effect.gen(function* deployRequiresAllowedRepositoryListEffect() {
        const error = yield* Effect.flip(
          deployProgramWithRunner(
            { STATEFUL_CI_TRANSFER_SECRET: "test-transfer-secret" },
            () => Effect.succeed({ stderr: "", stdout: "" })
          )
        );

        assert.strictEqual(error._tag, "CliFailure");
        assert.include(
          error.message,
          "Missing STATEFUL_CI_ALLOWED_REPOSITORIES"
        );
      })
    );

    it.effect("rejects invalid R2 bucket names before provisioning", () =>
      Effect.gen(function* deployRejectsInvalidBucketNamesEffect() {
        const calls: DeployStepCall[] = [];
        const error = yield* Effect.flip(
          deployProgramWithRunner(
            { ...deployEnv, STATEFUL_CI_R2_BUCKET: "--bad-bucket" },
            (call) => {
              calls.push(call);
              return Effect.succeed({ stderr: "", stdout: "" });
            }
          )
        );

        assert.strictEqual(error._tag, "CliFailure");
        assert.include(error.message, "STATEFUL_CI_R2_BUCKET must use");
        assert.deepStrictEqual(calls, []);
      })
    );

    it.effect("rejects invalid D1 database names before provisioning", () =>
      Effect.gen(function* deployRejectsInvalidDatabaseNamesEffect() {
        const calls: DeployStepCall[] = [];
        const error = yield* Effect.flip(
          deployProgramWithRunner(
            { ...deployEnv, STATEFUL_CI_D1_DATABASE: "bad database" },
            (call) => {
              calls.push(call);
              return Effect.succeed({ stderr: "", stdout: "" });
            }
          )
        );

        assert.strictEqual(error._tag, "CliFailure");
        assert.include(error.message, "STATEFUL_CI_D1_DATABASE must use");
        assert.deepStrictEqual(calls, []);
      })
    );
  });

  describe("dashboard", () => {
    it.effect("is registered as a CLI command", () =>
      Effect.gen(function* dashboardIsRegisteredEffect() {
        yield* runCli(["dashboard"]);
      })
    );
  });
});
