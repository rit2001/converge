import { parseEnvironment } from "./env.js";
import { createApiServer } from "./server-runtime.js";

const environment = parseEnvironment(process.env);
const server = await createApiServer(environment);
const shutdown = (): void => {
  void server.close().catch(() => {
    process.exitCode = 1;
  });
};

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
await server.listen();
