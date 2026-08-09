import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, test } from "node:test";
import { loadRootEnvironment } from "./root-environment.mjs";

const fixtures = [];

afterEach(() => {
  process.chdir(import.meta.dirname);
  for (const fixture of fixtures.splice(0)) rmSync(fixture, { recursive: true, force: true });
});

function fixture(contents) {
  const rootDirectory = mkdtempSync(join(tmpdir(), "converge-environment-"));
  fixtures.push(rootDirectory);
  writeFileSync(join(rootDirectory, ".env"), contents);
  return rootDirectory;
}

test("loads values from an injected repository-root environment file", () => {
  const environment = {};
  loadRootEnvironment({
    environment,
    rootDirectory: fixture("DATABASE_URL=postgresql://localhost/from-file\nAPI_PORT=4100\n"),
  });
  assert.deepEqual(environment, {
    DATABASE_URL: "postgresql://localhost/from-file",
    API_PORT: "4100",
  });
});

test("preserves explicit process environment values", () => {
  const environment = { DATABASE_URL: "postgresql://localhost/from-process" };
  loadRootEnvironment({
    environment,
    rootDirectory: fixture("DATABASE_URL=postgresql://localhost/from-file\nLOG_LEVEL=warn\n"),
  });
  assert.equal(environment.DATABASE_URL, "postgresql://localhost/from-process");
  assert.equal(environment.LOG_LEVEL, "warn");
});

test("resolves the injected root independently of a package working directory", () => {
  const rootDirectory = fixture("DATABASE_URL=postgresql://localhost/from-root\n");
  const workspaceDirectory = join(rootDirectory, "apps", "api");
  mkdirSync(workspaceDirectory, { recursive: true });
  process.chdir(workspaceDirectory);
  const environment = {};
  loadRootEnvironment({ environment, rootDirectory });
  assert.equal(environment.DATABASE_URL, "postgresql://localhost/from-root");
});

test("allows a missing environment file when process configuration is complete", () => {
  const rootDirectory = mkdtempSync(join(tmpdir(), "converge-environment-missing-"));
  fixtures.push(rootDirectory);
  const environment = { DATABASE_URL: "postgresql://localhost/injected" };
  assert.equal(loadRootEnvironment({ environment, rootDirectory }), environment);
});
