import type { FastifyRequest } from "fastify";
import type { Socket } from "socket.io";
import { z } from "zod";
import type { Environment } from "./env.js";

export interface AuthenticatedPrincipal {
  id: string;
  displayName: string;
}

export type AuthenticationErrorCode = "AUTHENTICATION_REQUIRED" | "INVALID_AUTH_INPUT";

export class AuthenticationError extends Error {
  constructor(
    public readonly code: AuthenticationErrorCode,
    message: string,
  ) {
    super(message);
  }
}

export interface AuthAdapter {
  authenticateHttp(request: FastifyRequest): Promise<AuthenticatedPrincipal | null>;
  authenticateSocket(socket: Socket): Promise<AuthenticatedPrincipal | null>;
}

const userIdSchema = z.string().uuid();

/** Local identity scaffolding. This is explicitly not production authentication. */
export class DevelopmentAuthAdapter implements AuthAdapter {
  private readonly principal: AuthenticatedPrincipal;

  constructor(environment: Environment) {
    if (environment.NODE_ENV === "production")
      throw new Error("Development authentication cannot run in production");
    if (!environment.DEV_AUTH_USER_ID)
      throw new Error("DEV_AUTH_USER_ID is required for development authentication");
    const userId = userIdSchema.safeParse(environment.DEV_AUTH_USER_ID);
    if (!userId.success)
      throw new Error("DEV_AUTH_USER_ID must be a valid UUID for development authentication");
    this.principal = {
      id: userId.data,
      displayName: environment.DEV_AUTH_USER_NAME,
    };
  }

  authenticateHttp(request: FastifyRequest): Promise<AuthenticatedPrincipal> {
    if (request.headers["x-dev-user-id"] !== undefined)
      throw new AuthenticationError(
        "INVALID_AUTH_INPUT",
        "Caller-controlled development identity is not accepted",
      );
    return Promise.resolve(this.principal);
  }

  authenticateSocket(socket: Socket): Promise<AuthenticatedPrincipal> {
    if (
      socket.handshake.auth &&
      typeof socket.handshake.auth === "object" &&
      Object.hasOwn(socket.handshake.auth, "userId")
    )
      throw new AuthenticationError(
        "INVALID_AUTH_INPUT",
        "Caller-controlled development identity is not accepted",
      );
    return Promise.resolve(this.principal);
  }
}
