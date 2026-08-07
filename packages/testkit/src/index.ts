import type { DurableCommand } from "@converge/protocol";
import {
  AuthenticationError,
  type AuthAdapter,
  type AuthenticatedPrincipal,
} from "@converge/api/auth";
import { io, type Socket } from "socket.io-client";

export const fixtureIds = {
  user: "00000000-0000-4000-8000-000000000001",
  clientA: "10000000-0000-4000-8000-000000000001",
  clientB: "10000000-0000-4000-8000-000000000002",
};

type HttpAuthenticationRequest = Parameters<AuthAdapter["authenticateHttp"]>[0];
type SocketAuthenticationRequest = Parameters<AuthAdapter["authenticateSocket"]>[0];

/** Deterministic opaque-token authentication for injected test applications only. */
export class TestAuthAdapter implements AuthAdapter {
  private readonly principalsByToken: ReadonlyMap<string, AuthenticatedPrincipal>;

  constructor(principalsByToken: ReadonlyMap<string, AuthenticatedPrincipal>) {
    this.principalsByToken = new Map(principalsByToken);
  }

  authenticateHttp(request: HttpAuthenticationRequest): Promise<AuthenticatedPrincipal | null> {
    if (request.headers["x-dev-user-id"] !== undefined)
      throw new AuthenticationError(
        "INVALID_AUTH_INPUT",
        "Caller-controlled development identity is not accepted",
      );
    const authorization = request.headers.authorization;
    if (authorization === undefined) return Promise.resolve(null);
    const match =
      typeof authorization === "string" ? /^Bearer ([^\s]+)$/.exec(authorization) : null;
    if (!match?.[1])
      throw new AuthenticationError("INVALID_AUTH_INPUT", "A valid bearer token is required");
    return Promise.resolve(this.principalsByToken.get(match[1]) ?? null);
  }

  authenticateSocket(socket: SocketAuthenticationRequest): Promise<AuthenticatedPrincipal | null> {
    const auth = socket.handshake.auth;
    if (auth && typeof auth === "object" && Object.hasOwn(auth, "userId"))
      throw new AuthenticationError(
        "INVALID_AUTH_INPUT",
        "Caller-controlled development identity is not accepted",
      );
    if (!auth || typeof auth !== "object" || !Object.hasOwn(auth, "token"))
      return Promise.resolve(null);
    if (typeof auth.token !== "string" || auth.token.length === 0)
      throw new AuthenticationError("INVALID_AUTH_INPUT", "A valid test token is required");
    return Promise.resolve(this.principalsByToken.get(auth.token) ?? null);
  }
}

export function testAuthorizationHeaders(token: string): { authorization: string } {
  return { authorization: `Bearer ${token}` };
}

export function createTestSocket(
  url: string,
  token?: string,
  additionalAuth: Record<string, unknown> = {},
): Socket {
  return io(url, {
    auth: { ...(token === undefined ? {} : { token }), ...additionalAuth },
    autoConnect: false,
    forceNew: true,
    reconnection: false,
    transports: ["websocket"],
  });
}

export function createRectangleCommand(
  boardId: string,
  objectId = crypto.randomUUID(),
  opId = crypto.randomUUID(),
): DurableCommand {
  return {
    schemaVersion: 1,
    opId,
    boardId,
    clientId: fixtureIds.clientA,
    baseSeq: 0,
    type: "object.create",
    targetId: objectId,
    payload: {
      id: objectId,
      kind: "rectangle",
      x: 40,
      y: 40,
      width: 160,
      height: 100,
      rotation: 0,
      fill: "#818cf8",
      text: "",
    },
    clientTimestamp: new Date().toISOString(),
  };
}
