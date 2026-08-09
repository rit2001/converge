import { describe, expect, it } from "vitest";
import type { AuthAdapter, AuthenticatedPrincipal } from "@converge/api/auth";
import { TestAuthAdapter } from "./index.js";

const principal: AuthenticatedPrincipal = {
  id: "00000000-0000-4000-8000-000000000021",
  displayName: "Test Owner",
};
const adapter = new TestAuthAdapter(new Map([["opaque-owner-token", principal]]));

type HttpRequest = Parameters<AuthAdapter["authenticateHttp"]>[0];
type SocketRequest = Parameters<AuthAdapter["authenticateSocket"]>[0];

function httpRequest(authorization?: string): HttpRequest {
  return {
    headers: authorization === undefined ? {} : { authorization },
  } as HttpRequest;
}

function socketRequest(token?: string): SocketRequest {
  return {
    handshake: { auth: token === undefined ? {} : { token } },
  } as SocketRequest;
}

describe("test authentication", () => {
  it("treats unknown opaque tokens as unauthenticated", async () => {
    await expect(adapter.authenticateHttp(httpRequest("Bearer unknown-token"))).resolves.toBeNull();
    await expect(adapter.authenticateSocket(socketRequest("unknown-token"))).resolves.toBeNull();
  });

  it("resolves the same server-mapped principal for HTTP and Socket.IO", async () => {
    await expect(
      adapter.authenticateHttp(httpRequest("Bearer opaque-owner-token")),
    ).resolves.toEqual(principal);
    await expect(adapter.authenticateSocket(socketRequest("opaque-owner-token"))).resolves.toEqual(
      principal,
    );
  });
});
