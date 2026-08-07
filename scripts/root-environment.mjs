import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseEnv } from "node:util";
import { spawn } from "node:child_process";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function loadRootEnvironment({
  environment = process.env,
  rootDirectory = repositoryRoot,
} = {}) {
  const path = join(rootDirectory, ".env");
  let source;
  try {
    source = readFileSync(path, "utf8");
  } catch (error) {
    if (error && typeof error === "object" && error.code === "ENOENT") return environment;
    throw new Error("Unable to load the repository environment file");
  }

  let parsed;
  try {
    parsed = parseEnv(source);
  } catch {
    throw new Error("The repository environment file is invalid");
  }
  for (const [name, value] of Object.entries(parsed)) {
    if (environment[name] === undefined) environment[name] = value;
  }
  return environment;
}

function run(command, args) {
  const child = spawn(command, args, {
    cwd: repositoryRoot,
    env: loadRootEnvironment(),
    stdio: "inherit",
  });
  child.once("error", (error) => {
    process.stderr.write(`Unable to start the requested command: ${error.message}\n`);
    process.exitCode = 1;
  });
  child.once("exit", (code, signal) => {
    if (signal) process.kill(process.pid, signal);
    else process.exitCode = code ?? 1;
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [command, ...args] = process.argv.slice(2);
  if (!command) throw new Error("A command is required");
  run(command, args);
}
