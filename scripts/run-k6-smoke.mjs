import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const result = spawnSync("k6", ["run", "tests/k6/collaboration.js"], {
  cwd: repositoryRoot,
  env: { ...process.env, CONVERGE_PROFILE: "smoke" },
  stdio: "inherit",
});

if (result.error) {
  process.stderr.write(
    "Unable to start k6. Install a trusted k6 runtime and supply all required CONVERGE_* variables.\n",
  );
  process.exit(1);
}
process.exit(result.status ?? 1);
