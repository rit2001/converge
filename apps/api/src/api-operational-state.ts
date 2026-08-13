export type ApiOperationalLifecycle =
  | "starting"
  | "ready"
  | "stopping"
  | "stopped"
  | "startup_failed";

export interface ApiOperationalState {
  transition(next: ApiOperationalLifecycle): void;
  setSocketReady(ready: boolean): void;
  isLive(): boolean;
  isHttpReady(): boolean;
  isSocketReady(): boolean;
}

export class InstanceApiOperationalState implements ApiOperationalState {
  private lifecycle: ApiOperationalLifecycle = "starting";
  private socketReady: boolean;

  constructor(socketReadyAtStartup: boolean) {
    this.socketReady = socketReadyAtStartup;
  }

  transition(next: ApiOperationalLifecycle): void {
    if (this.lifecycle === "stopped" || this.lifecycle === next) return;
    const allowed: Readonly<Record<ApiOperationalLifecycle, readonly ApiOperationalLifecycle[]>> = {
      starting: ["ready", "stopping", "startup_failed"],
      ready: ["stopping"],
      startup_failed: ["stopping", "stopped"],
      stopping: ["stopped"],
      stopped: [],
    };
    if (!allowed[this.lifecycle].includes(next)) return;
    this.lifecycle = next;
    if (next === "stopping" || next === "stopped" || next === "startup_failed")
      this.socketReady = false;
  }

  setSocketReady(ready: boolean): void {
    if (
      this.lifecycle === "stopping" ||
      this.lifecycle === "stopped" ||
      this.lifecycle === "startup_failed"
    )
      return;
    this.socketReady = ready;
  }

  isLive(): boolean {
    return this.lifecycle === "starting" || this.lifecycle === "ready";
  }

  isHttpReady(): boolean {
    return this.lifecycle === "ready";
  }

  isSocketReady(): boolean {
    return this.lifecycle === "ready" && this.socketReady;
  }
}
