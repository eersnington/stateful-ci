import { applyD1Migrations } from "cloudflare:test";
import { env } from "cloudflare:workers";

await applyD1Migrations(env.STATEFUL_CI_METADATA, env.TEST_MIGRATIONS);
