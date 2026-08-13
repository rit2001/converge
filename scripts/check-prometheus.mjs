import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const prometheusDirectory = join(repositoryRoot, "ops", "prometheus");
const rulePath = "ops/prometheus/converge-alerts.yml";
const fixturePath = "ops/prometheus/converge-alert-tests.yml";
const metadata = JSON.parse(readFileSync(join(prometheusDirectory, "promtool.json"), "utf8"));
const prometheusImage = `${metadata.image}:v${metadata.version}@${metadata.digest}`;

function execute(command, args) {
  const result = spawnSync(command, args, {
    cwd: repositoryRoot,
    stdio: "inherit",
  });
  if (result.error) {
    process.stderr.write(`Unable to execute Prometheus validation: ${result.error.message}\n`);
    return 1;
  }
  return result.status ?? 1;
}

function runPromtool(commandArgs) {
  const suppliedBinary = process.env.PROMTOOL_BIN;
  if (suppliedBinary !== undefined) {
    if (suppliedBinary.length === 0) {
      process.stderr.write("PROMTOOL_BIN must name an executable when supplied.\n");
      return 1;
    }
    return execute(suppliedBinary, commandArgs);
  }

  return execute("docker", [
    "run",
    "--rm",
    "--network",
    "none",
    "--read-only",
    "--tmpfs",
    "/tmp:rw,noexec,nosuid,size=64m",
    "--mount",
    `type=bind,src=${prometheusDirectory},dst=/workspace/ops/prometheus,readonly`,
    "--workdir",
    "/workspace",
    "--entrypoint",
    "/bin/promtool",
    prometheusImage,
    ...commandArgs,
  ]);
}

for (const commandArgs of [
  ["check", "rules", rulePath],
  ["test", "rules", fixturePath],
]) {
  const status = runPromtool(commandArgs);
  if (status !== 0) process.exit(status);
}
