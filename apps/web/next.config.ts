import path from "node:path";
import type { NextConfig } from "next";

const testDistDir = process.env.CONVERGE_NEXT_DIST_DIR;
if (testDistDir !== undefined && !/^\.next-m35c-[a-z0-9-]{1,64}$/.test(testDistDir))
  throw new Error("CONVERGE_NEXT_DIST_DIR must name an owned M3.5C test directory");
const testTsconfig = process.env.CONVERGE_NEXT_TSCONFIG;
if (
  testTsconfig !== undefined &&
  !/^\.next-m35c-[a-z0-9-]{1,64}\/tsconfig\.json$/.test(testTsconfig)
)
  throw new Error("CONVERGE_NEXT_TSCONFIG must be an owned M3.5C test configuration");

const config: NextConfig = {
  ...(testDistDir ? { distDir: testDistDir } : {}),
  ...(testTsconfig ? { typescript: { tsconfigPath: testTsconfig } } : {}),
  allowedDevOrigins: ["127.0.0.1"],
  outputFileTracingRoot: path.resolve(process.cwd(), "../.."),
  reactStrictMode: true,
  transpilePackages: ["@converge/protocol", "@converge/canvas-engine"],
  webpack(configuration) {
    configuration.resolve.alias = { ...configuration.resolve.alias, canvas: false };
    return configuration;
  },
};
export default config;
