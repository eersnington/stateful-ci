import type { D1Migration } from "cloudflare:test";

interface StatefulCiCloudflareTestEnv {
  readonly STATEFUL_CI_API_TOKEN: string;
  readonly STATEFUL_CI_COORDINATORS: DurableObjectNamespace;
  readonly STATEFUL_CI_METADATA: D1Database;
  readonly STATEFUL_CI_OBJECTS: R2Bucket;
  readonly TEST_MIGRATIONS: D1Migration[];
}

declare global {
  namespace Cloudflare {
    interface Env {
      readonly STATEFUL_CI_API_TOKEN: StatefulCiCloudflareTestEnv["STATEFUL_CI_API_TOKEN"];
      readonly STATEFUL_CI_COORDINATORS: StatefulCiCloudflareTestEnv["STATEFUL_CI_COORDINATORS"];
      readonly STATEFUL_CI_METADATA: StatefulCiCloudflareTestEnv["STATEFUL_CI_METADATA"];
      readonly STATEFUL_CI_OBJECTS: StatefulCiCloudflareTestEnv["STATEFUL_CI_OBJECTS"];
      readonly TEST_MIGRATIONS: StatefulCiCloudflareTestEnv["TEST_MIGRATIONS"];
    }
  }
}
