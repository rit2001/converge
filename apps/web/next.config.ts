import path from "node:path";
import type { NextConfig } from "next";

const config: NextConfig = {
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
