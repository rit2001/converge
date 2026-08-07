import type { FastifyRequest } from "fastify";
import type { Socket } from "socket.io";
import { z } from "zod";
import type { Environment } from "./env.js";

export interface AuthenticatedUser {
  id: string;
  displayName: string;
}
export interface AuthenticationAdapter {
  authenticateHttp(request: FastifyRequest): Promise<AuthenticatedUser | null>;
  authenticateSocket(socket: Socket): Promise<AuthenticatedUser | null>;
}

const userIdSchema = z.string().uuid();

/** Local identity scaffolding. This is explicitly not production authentication. */
export class DevelopmentAuthenticationAdapter implements AuthenticationAdapter {
  constructor(private readonly environment: Environment) {
    if (environment.NODE_ENV === "production")
      throw new Error("Development authentication cannot run in production");
  }
  authenticateHttp(request: FastifyRequest): Promise<AuthenticatedUser> {
    const header = request.headers["x-dev-user-id"];
    const candidate = typeof header === "string" ? header : this.environment.DEV_AUTH_USER_ID;
    return Promise.resolve({
      id: userIdSchema.parse(candidate),
      displayName: this.environment.DEV_AUTH_USER_NAME,
    });
  }
  authenticateSocket(socket: Socket): Promise<AuthenticatedUser> {
    const parsed = z.object({ userId: userIdSchema.optional() }).safeParse(socket.handshake.auth);
    return Promise.resolve({
      id:
        parsed.success && parsed.data.userId
          ? parsed.data.userId
          : this.environment.DEV_AUTH_USER_ID,
      displayName: this.environment.DEV_AUTH_USER_NAME,
    });
  }
}
