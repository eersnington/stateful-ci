import { WorkspaceSnapshotCoordinatorDurableObject } from "./durable-object";
import { handleFetch } from "./handler";
import type { WorkerEnv } from "./handler";

export { WorkspaceSnapshotCoordinatorDurableObject };

export default {
  fetch(request: Request, env: WorkerEnv) {
    return handleFetch(request, env);
  },
} satisfies ExportedHandler<WorkerEnv>;
