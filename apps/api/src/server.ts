import { createPool } from "@converge/database";
import { buildApp } from "./app.js";
import { DevelopmentAuthAdapter } from "./auth.js";
import { parseEnvironment } from "./env.js";

const environment = parseEnvironment(process.env);
const pool = createPool(environment.DATABASE_URL);
const authentication = new DevelopmentAuthAdapter(environment);
const { app } = await buildApp(environment, pool, authentication);

const shutdown = async (): Promise<void> => {
  await app.close();
  await pool.end();
};
process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
await app.listen({ host: environment.HOST, port: environment.API_PORT });
