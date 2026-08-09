import { describe, expect, it } from "vitest";
import { EnvironmentConfigurationError, parseEnvironment } from "./env.js";

const required = {
  DATABASE_URL: "postgresql://converge:local-password@localhost:55432/converge",
};

describe("environment validation", () => {
  it("accepts injected production configuration without an environment file", () => {
    expect(
      parseEnvironment({
        ...required,
        NODE_ENV: "production",
        WEB_ORIGIN: "https://example.test",
      }),
    ).toMatchObject({
      NODE_ENV: "production",
      DATABASE_URL: required.DATABASE_URL,
      WEB_ORIGIN: "https://example.test",
    });
  });

  it("fails clearly before startup when required configuration is absent", () => {
    expect(() => parseEnvironment({ NODE_ENV: "production" })).toThrowError(
      new EnvironmentConfigurationError(["DATABASE_URL"]),
    );
  });

  it("does not include credentials or complete connection strings in configuration errors", () => {
    const secret = "do-not-expose-this-password";
    let message = "";
    try {
      parseEnvironment({ DATABASE_URL: `not-a-url-${secret}` });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    }
    expect(message).toBe("Invalid environment configuration: DATABASE_URL");
    expect(message).not.toContain(secret);
    expect(message).not.toContain("not-a-url");
  });
});
