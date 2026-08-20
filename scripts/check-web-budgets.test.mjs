import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import test from "node:test";
import {
  WEB_BUNDLE_ARTIFACT_CLASSIFICATION,
  WEB_BUNDLE_ARTIFACT_VERSION,
  WEB_BUNDLE_BUDGETS,
  assertWebBundleBudgets,
  assertWebPerformanceArtifactPrivacy,
  collectWebBundleArtifact,
  validateWebBundleArtifact,
} from "./check-web-budgets.mjs";

const bytes = (value) => Buffer.from(value.repeat(32));
const manifest = {
  pages: {
    "/layout": ["static/chunks/shared.js", "static/css/shared.css"],
    "/page": ["static/chunks/shared.js", "static/chunks/landing.js"],
    "/studio/page": ["static/chunks/shared.js", "static/chunks/studio.js"],
  },
};
const contents = new Map([
  ["static/chunks/shared.js", bytes("shared")],
  ["static/chunks/landing.js", bytes("landing")],
  ["static/chunks/studio.js", bytes("studio")],
  ["static/css/shared.css", bytes("css")],
]);

const route = (javascriptGzipBytes, cssGzipBytes = 1) => ({
  assets: [
    {
      path: "static/chunks/example.js",
      rawBytes: javascriptGzipBytes,
      gzipBytes: javascriptGzipBytes,
    },
    { path: "static/css/example.css", rawBytes: cssGzipBytes, gzipBytes: cssGzipBytes },
  ],
  javascriptRawBytes: javascriptGzipBytes,
  javascriptGzipBytes,
  cssRawBytes: cssGzipBytes,
  cssGzipBytes,
});

const artifact = (landingBytes, studioBytes, cssBytes = 1) => ({
  schemaVersion: WEB_BUNDLE_ARTIFACT_VERSION,
  classification: WEB_BUNDLE_ARTIFACT_CLASSIFICATION,
  budgets: WEB_BUNDLE_BUDGETS,
  landing: route(landingBytes, cssBytes),
  studio: route(studioBytes, cssBytes),
});

test("collects deterministic route assets once and calculates exact gzip bytes", async () => {
  const result = await collectWebBundleArtifact({
    manifest,
    readAsset: async (path) => contents.get(path),
  });
  assert.deepEqual(
    result.landing.assets.map((asset) => asset.path),
    ["static/chunks/landing.js", "static/chunks/shared.js", "static/css/shared.css"],
  );
  assert.equal(
    result.landing.javascriptGzipBytes,
    gzipSync(contents.get("static/chunks/landing.js")).byteLength +
      gzipSync(contents.get("static/chunks/shared.js")).byteLength,
  );
  assert.doesNotThrow(() => validateWebBundleArtifact(result));
});

test("enforces exact bundle boundaries and rejects malformed or duplicate manifest evidence", async () => {
  assert.doesNotThrow(() =>
    assertWebBundleBudgets(
      artifact(
        WEB_BUNDLE_BUDGETS.landingInitialJavaScriptGzipBytes,
        WEB_BUNDLE_BUDGETS.studioInitialJavaScriptGzipBytes,
        WEB_BUNDLE_BUDGETS.initialRouteCssGzipBytes,
      ),
    ),
  );
  assert.throws(
    () =>
      assertWebBundleBudgets(artifact(WEB_BUNDLE_BUDGETS.landingInitialJavaScriptGzipBytes + 1, 1)),
    /WEB_BUNDLE_LANDING_JS_BUDGET/,
  );
  assert.throws(
    () =>
      assertWebBundleBudgets(artifact(1, WEB_BUNDLE_BUDGETS.studioInitialJavaScriptGzipBytes + 1)),
    /WEB_BUNDLE_STUDIO_JS_BUDGET/,
  );
  assert.throws(
    () => assertWebBundleBudgets(artifact(1, 1, WEB_BUNDLE_BUDGETS.initialRouteCssGzipBytes + 1)),
    /WEB_BUNDLE_CSS_BUDGET/,
  );
  await assert.rejects(
    collectWebBundleArtifact({
      manifest: {
        ...manifest,
        pages: {
          ...manifest.pages,
          "/page": ["static/chunks/landing.js", "static/chunks/landing.js"],
        },
      },
      readAsset: async (path) => contents.get(path),
    }),
    /WEB_BUNDLE_MANIFEST_DUPLICATE/,
  );
  await assert.rejects(
    collectWebBundleArtifact({
      manifest: {
        ...manifest,
        pages: { ...manifest.pages, "/page": ["static/chunks/../landing.js"] },
      },
      readAsset: async (path) => contents.get(path),
    }),
    /WEB_BUNDLE_MANIFEST_ASSET/,
  );
});

test("rejects studio-only code in the landing asset set", async () => {
  await assert.rejects(
    collectWebBundleArtifact({
      manifest,
      readAsset: async (path) =>
        path === "static/chunks/landing.js" ? Buffer.from("react-konva") : contents.get(path),
    }),
    /WEB_BUNDLE_LANDING_STUDIO_CODE/,
  );
});

test("strict schema and privacy checks reject unknown, sensitive, and capacity evidence", () => {
  const valid = artifact(1, 1);
  assert.doesNotThrow(() => validateWebBundleArtifact(valid));
  assert.throws(() => validateWebBundleArtifact({ ...valid, port: 3000 }), /ARTIFACT_SCHEMA/);
  assert.throws(
    () => assertWebPerformanceArtifactPrivacy({ note: "postgresql://credential" }),
    /PRIVATE_EVIDENCE/,
  );
  assert.throws(
    () => assertWebPerformanceArtifactPrivacy({ note: "Production capacity confirmed" }),
    /CAPACITY_CLAIM/,
  );
});
