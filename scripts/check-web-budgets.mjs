import { readFile } from "node:fs/promises";
import { gzipSync } from "node:zlib";
import { fileURLToPath } from "node:url";

export const WEB_BUNDLE_ARTIFACT_CLASSIFICATION = "LOCAL REFERENCE PERFORMANCE";
export const WEB_BUNDLE_ARTIFACT_VERSION = 1;
export const WEB_BUNDLE_BUDGETS = Object.freeze({
  landingInitialJavaScriptGzipBytes: 184_320,
  studioInitialJavaScriptGzipBytes: 460_800,
  initialRouteCssGzipBytes: 81_920,
});

const expectedKeys = (value) => Object.keys(value).sort().join(",");

function strictRecord(value, fields, code) {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    expectedKeys(value) !== [...fields].sort().join(",")
  ) {
    throw new Error(code);
  }
  return value;
}

const isNonNegativeInteger = (value) => Number.isSafeInteger(value) && value >= 0;
const privateEvidence =
  /(?:https?:\/\/|postgres(?:ql)?:\/\/|redis:\/\/|\/Users\/|\/home\/|\\Users\\|\b(?:board|user|session|operation)\s*id\b|\btoken\b|password|BEGIN [A-Z ]+ KEY|[a-f0-9]{8}-[a-f0-9]{4}-[1-5][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12})/i;
const unsupportedClaim =
  /(?:production capacity|concurrent[- ]user capacity|10,000[- ]user|service level agreement|\bSLA\b)/i;
const isAssetPath = (value) =>
  typeof value === "string" &&
  /^static\/.+\.(?:js|css)$/.test(value) &&
  !value.split("/").includes("..");

export function assertWebPerformanceArtifactPrivacy(value) {
  const serialized = JSON.stringify(value);
  if (privateEvidence.test(serialized)) throw new Error("WEB_BUNDLE_PRIVATE_EVIDENCE");
  if (unsupportedClaim.test(serialized)) throw new Error("WEB_BUNDLE_CAPACITY_CLAIM");
}

function validateAsset(value) {
  const asset = strictRecord(value, ["path", "rawBytes", "gzipBytes"], "WEB_BUNDLE_ASSET_SCHEMA");
  if (
    !isAssetPath(asset.path) ||
    !isNonNegativeInteger(asset.rawBytes) ||
    !isNonNegativeInteger(asset.gzipBytes)
  ) {
    throw new Error("WEB_BUNDLE_ASSET_SCHEMA");
  }
}

function validateRoute(value) {
  const route = strictRecord(
    value,
    ["assets", "javascriptRawBytes", "javascriptGzipBytes", "cssRawBytes", "cssGzipBytes"],
    "WEB_BUNDLE_ROUTE_SCHEMA",
  );
  if (!Array.isArray(route.assets) || route.assets.length === 0)
    throw new Error("WEB_BUNDLE_ROUTE_SCHEMA");
  route.assets.forEach(validateAsset);
  const paths = route.assets.map((asset) => asset.path);
  if (new Set(paths).size !== paths.length || paths.join(",") !== [...paths].sort().join(","))
    throw new Error("WEB_BUNDLE_ASSET_ORDER");
  for (const field of [
    "javascriptRawBytes",
    "javascriptGzipBytes",
    "cssRawBytes",
    "cssGzipBytes",
  ]) {
    if (!isNonNegativeInteger(route[field])) throw new Error("WEB_BUNDLE_ROUTE_SCHEMA");
  }
  const sum = (suffix, field) =>
    route.assets
      .filter((asset) => asset.path.endsWith(suffix))
      .reduce((total, asset) => total + asset[field], 0);
  if (
    route.javascriptRawBytes !== sum(".js", "rawBytes") ||
    route.javascriptGzipBytes !== sum(".js", "gzipBytes") ||
    route.cssRawBytes !== sum(".css", "rawBytes") ||
    route.cssGzipBytes !== sum(".css", "gzipBytes")
  ) {
    throw new Error("WEB_BUNDLE_TOTAL_MISMATCH");
  }
}

export function validateWebBundleArtifact(value) {
  const artifact = strictRecord(
    value,
    ["schemaVersion", "classification", "budgets", "landing", "studio"],
    "WEB_BUNDLE_ARTIFACT_SCHEMA",
  );
  if (
    artifact.schemaVersion !== WEB_BUNDLE_ARTIFACT_VERSION ||
    artifact.classification !== WEB_BUNDLE_ARTIFACT_CLASSIFICATION
  ) {
    throw new Error("WEB_BUNDLE_ARTIFACT_SCHEMA");
  }
  const budgets = strictRecord(
    artifact.budgets,
    Object.keys(WEB_BUNDLE_BUDGETS),
    "WEB_BUNDLE_BUDGET_SCHEMA",
  );
  if (Object.entries(WEB_BUNDLE_BUDGETS).some(([key, value]) => budgets[key] !== value))
    throw new Error("WEB_BUNDLE_BUDGET_SCHEMA");
  validateRoute(artifact.landing);
  validateRoute(artifact.studio);
  assertWebPerformanceArtifactPrivacy(artifact);
  return artifact;
}

export function assertWebBundleBudgets(value) {
  const artifact = validateWebBundleArtifact(value);
  if (artifact.landing.javascriptGzipBytes > WEB_BUNDLE_BUDGETS.landingInitialJavaScriptGzipBytes) {
    throw new Error("WEB_BUNDLE_LANDING_JS_BUDGET");
  }
  if (artifact.studio.javascriptGzipBytes > WEB_BUNDLE_BUDGETS.studioInitialJavaScriptGzipBytes) {
    throw new Error("WEB_BUNDLE_STUDIO_JS_BUDGET");
  }
  if (
    Math.max(artifact.landing.cssGzipBytes, artifact.studio.cssGzipBytes) >
    WEB_BUNDLE_BUDGETS.initialRouteCssGzipBytes
  ) {
    throw new Error("WEB_BUNDLE_CSS_BUDGET");
  }
  return artifact;
}

function manifestAssets(manifest, route) {
  const pages = manifest?.pages;
  const layout = pages?.["/layout"];
  const page = pages?.[route];
  if (
    !Array.isArray(layout) ||
    !layout.every((item) => typeof item === "string") ||
    !Array.isArray(page) ||
    !page.every((item) => typeof item === "string")
  ) {
    throw new Error("WEB_BUNDLE_MANIFEST_SCHEMA");
  }
  for (const list of [layout, page]) {
    if (new Set(list).size !== list.length) throw new Error("WEB_BUNDLE_MANIFEST_DUPLICATE");
  }
  const assets = [...new Set([...layout, ...page])].sort();
  if (assets.length === 0 || assets.some((asset) => !isAssetPath(asset)))
    throw new Error("WEB_BUNDLE_MANIFEST_ASSET");
  return assets;
}

async function collectRoute(manifest, route, readAsset) {
  const assets = await Promise.all(
    manifestAssets(manifest, route).map(async (path) => {
      const contents = await readAsset(path);
      if (!(contents instanceof Uint8Array)) throw new Error("WEB_BUNDLE_ASSET_UNREADABLE");
      return { path, rawBytes: contents.byteLength, gzipBytes: gzipSync(contents).byteLength };
    }),
  );
  const sum = (suffix, field) =>
    assets
      .filter((asset) => asset.path.endsWith(suffix))
      .reduce((total, asset) => total + asset[field], 0);
  return {
    assets,
    javascriptRawBytes: sum(".js", "rawBytes"),
    javascriptGzipBytes: sum(".js", "gzipBytes"),
    cssRawBytes: sum(".css", "rawBytes"),
    cssGzipBytes: sum(".css", "gzipBytes"),
  };
}

export async function collectWebBundleArtifact({ manifest, readAsset }) {
  const landing = await collectRoute(manifest, "/page", readAsset);
  const studio = await collectRoute(manifest, "/studio/page", readAsset);
  const landingJavaScript = Buffer.concat(
    await Promise.all(
      landing.assets
        .filter((asset) => asset.path.endsWith(".js"))
        .map((asset) => readAsset(asset.path)),
    ),
  ).toString("utf8");
  if (
    /(?:react-konva|socket\.io-client|presence-store|Canvas editing surface)/i.test(
      landingJavaScript,
    )
  )
    throw new Error("WEB_BUNDLE_LANDING_STUDIO_CODE");
  return assertWebBundleBudgets({
    schemaVersion: WEB_BUNDLE_ARTIFACT_VERSION,
    classification: WEB_BUNDLE_ARTIFACT_CLASSIFICATION,
    budgets: WEB_BUNDLE_BUDGETS,
    landing,
    studio,
  });
}

export async function checkBuiltWebBundles(buildDirectory) {
  const manifest = JSON.parse(
    await readFile(new URL("app-build-manifest.json", `${buildDirectory.href}/`), "utf8"),
  );
  return collectWebBundleArtifact({
    manifest,
    readAsset: (path) => readFile(new URL(path, `${buildDirectory.href}/`)),
  });
}

async function main() {
  const buildDirectory = new URL("../apps/web/.next", import.meta.url);
  const artifact = await checkBuiltWebBundles(buildDirectory);
  process.stdout.write(
    [
      `Landing initial JS: ${artifact.landing.javascriptGzipBytes} gzip bytes`,
      `Studio initial JS: ${artifact.studio.javascriptGzipBytes} gzip bytes`,
      `Initial route CSS: ${Math.max(artifact.landing.cssGzipBytes, artifact.studio.cssGzipBytes)} gzip bytes`,
    ].join("\n") + "\n",
  );
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((error) => {
    const code =
      error instanceof Error && /^WEB_BUNDLE_[A-Z_]+$/.test(error.message)
        ? error.message
        : "WEB_BUNDLE_CHECK_FAILED";
    process.stderr.write(`${code}\n`);
    process.exitCode = 1;
  });
}
