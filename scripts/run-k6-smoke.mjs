import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const workloadDirectory = join(repositoryRoot, "tests", "k6");
const metadata = JSON.parse(readFileSync(join(workloadDirectory, "runtime.json"), "utf8"));
const pinnedImage = `${metadata.image}:${metadata.version}@${metadata.digest}`;
const summaryPath = process.env.CONVERGE_K6_SUMMARY_PATH;
const workloadVariables = [
  "CONVERGE_BASE_URL",
  "CONVERGE_SOCKET_URL",
  "CONVERGE_BOARD_ID",
  "CONVERGE_AUTH_TOKEN",
  "CONVERGE_ALLOW_REMOTE_TARGET",
  "CONVERGE_VUS",
  "CONVERGE_DURATION",
  "CONVERGE_COMMANDS_PER_CLIENT",
  "CONVERGE_COMMAND_INTERVAL_MS",
  "CONVERGE_MAX_PACKET_BYTES",
  "CONVERGE_CONNECT_TIMEOUT_MS",
  "CONVERGE_ACK_TIMEOUT_MS",
  "CONVERGE_DEBUG_PHASES",
  "CONVERGE_DEBUG_REPEAT",
  "CONVERGE_DEBUG_CONCURRENT",
  "CONVERGE_DEBUG_SEQUENTIAL",
];
const debugOnce = process.env.CONVERGE_K6_DEBUG_ONCE === "true";

if (
  summaryPath !== undefined &&
  (!isAbsolute(summaryPath) || dirname(summaryPath) === summaryPath)
) {
  process.stderr.write("CONVERGE_K6_SUMMARY_PATH must be an absolute file path.\n");
  process.exit(1);
}

function execute(command, args, env = process.env) {
  return spawnSync(command, args, { cwd: repositoryRoot, env, stdio: "inherit" });
}

const native = spawnSync("k6", ["version"], { encoding: "utf8" });
let result;
if (!native.error && native.status === 0) {
  process.stdout.write(`k6 runtime: ${native.stdout.trim()} (local executable)\n`);
  const args = ["run"];
  if (debugOnce) args.push("--vus", "1", "--iterations", "1");
  if (summaryPath !== undefined) args.push("--summary-export", summaryPath);
  args.push("tests/k6/collaboration.js");
  result = execute("k6", args, { ...process.env, CONVERGE_PROFILE: "smoke" });
} else {
  const args = [
    "run",
    "--rm",
    "--read-only",
    "--tmpfs",
    "/tmp:rw,noexec,nosuid,size=64m",
    "--add-host",
    "host.docker.internal:host-gateway",
    "--mount",
    `type=bind,src=${workloadDirectory},dst=/workload,readonly`,
  ];
  for (const name of workloadVariables) {
    const value = process.env[name];
    if (value !== undefined) args.push("--env", `${name}=${value}`);
  }
  args.push("--env", "CONVERGE_PROFILE=smoke");
  if (summaryPath !== undefined) {
    const summaryDirectory = dirname(summaryPath);
    if (!existsSync(summaryDirectory)) {
      process.stderr.write("The k6 summary directory does not exist.\n");
      process.exit(1);
    }
    args.push("--mount", `type=bind,src=${summaryDirectory},dst=/results`);
  }
  args.push(pinnedImage, "run");
  if (debugOnce) args.push("--vus", "1", "--iterations", "1");
  if (summaryPath !== undefined)
    args.push("--summary-export", `/results/${summaryPath.slice(dirname(summaryPath).length + 1)}`);
  args.push("/workload/collaboration.js");
  process.stdout.write(
    `k6 runtime: v${metadata.version} (official container ${metadata.digest})\n`,
  );
  result = execute("docker", args);
}

if (result.error) {
  process.stderr.write(
    "Unable to start the pinned k6 runtime. Supply a trusted local k6 or a working Docker runtime.\n",
  );
  process.exit(1);
}
process.exit(result.status ?? 1);
