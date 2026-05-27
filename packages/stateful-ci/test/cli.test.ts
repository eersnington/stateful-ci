import { once } from "node:events";
import { createServer } from "node:http";

import { NodeServices } from "@effect/platform-node";
import { assert, describe, layer } from "@effect/vitest";
import {
  configFileName,
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
  readonly bodyBytes: Uint8Array;
  readonly headers: Record<string, string>;
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
            const { requests } = yield* withProtocolServer(
              () =>
                Response.json({
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
                }),
              (url) =>
                Effect.flip(
                  restoreProgram({
                    ...githubEnv,
                    STATEFUL_CI_API_TOKEN: "test-token",
                    STATEFUL_CI_API_URL: url,
                  })
                )
            );

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

              if (request.url === "/v1/save/prepare") {
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
          assert.strictEqual(prepareProtocolRequest.url, "/v1/save/prepare");
          assert.strictEqual(uploadRequest.method, "PUT");
          assert.strictEqual(
            uploadRequest.url,
            `/v1/objects/${missingObject.key}`
          );
          assert.strictEqual(commitProtocolRequest.method, "POST");
          assert.strictEqual(commitProtocolRequest.url, "/v1/save/commit");
          assert.strictEqual(prepareRequest.objects.length > 1, true);
          assert.strictEqual(prepareRequest.github.runId, "123456789");
          assert.strictEqual(
            prepareRequest.idempotencyKey,
            "run-123456789-save"
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

    it.effect("aborts before commit when object upload fails", () =>
      withWorkspace(setupSaveWorkspace, () =>
        Effect.gen(function* saveAbortsBeforeCommitOnUploadFailureEffect() {
          const error = yield* Effect.flip(
            withProtocolServer(
              (request) => {
                if (request.url === "/v1/save/prepare") {
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
          const { requests } = yield* withProtocolServer(
            (request) => {
              const prepareRequest = Schema.decodeUnknownSync(
                PrepareSaveRequest
              )(request.body);
              const [missingObject] = prepareRequest.objects;

              return Response.json({
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
              });
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

          assert.strictEqual(requests.length, 1);
          assert.strictEqual(requests[0]?.url, "/v1/save/prepare");
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
              if (request.url === "/v1/save/prepare") {
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

              if (request.url === "/v1/save/commit") {
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

  describe("dashboard", () => {
    it.effect("is registered as a CLI command", () =>
      Effect.gen(function* dashboardIsRegisteredEffect() {
        yield* runCli(["dashboard"]);
      })
    );
  });
});
