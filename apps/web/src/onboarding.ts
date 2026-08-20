export const ONBOARDING_KEY = "converge:studio-onboarding:v1";
export type OnboardingState = "unseen" | "dismissed" | "completed";
const valid = new Set<OnboardingState>(["unseen", "dismissed", "completed"]);
export function readOnboarding(storage: Storage | undefined): OnboardingState {
  try {
    if (!storage) return "unseen";
    const value: unknown = JSON.parse(
      storage.getItem(ONBOARDING_KEY) ?? '{"version":1,"state":"unseen"}',
    );
    if (!value || typeof value !== "object" || Object.keys(value).length !== 2) return "unseen";
    const record = value as { version?: unknown; state?: unknown };
    return record.version === 1 &&
      typeof record.state === "string" &&
      valid.has(record.state as OnboardingState)
      ? (record.state as OnboardingState)
      : "unseen";
  } catch {
    return "unseen";
  }
}
export function writeOnboarding(
  storage: Storage | undefined,
  state: Exclude<OnboardingState, "unseen">,
): void {
  try {
    storage?.setItem(ONBOARDING_KEY, JSON.stringify({ version: 1, state }));
  } catch {
    /* page-memory fallback */
  }
}
