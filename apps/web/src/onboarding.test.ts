import { describe, expect, it } from "vitest";
import { ONBOARDING_KEY, readOnboarding, writeOnboarding } from "./onboarding";

describe("studio onboarding", () => {
  it("strictly reads only the v1 bounded state", () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => void values.set(key, value),
    } as Storage;
    expect(readOnboarding(storage)).toBe("unseen");
    values.set(ONBOARDING_KEY, '{"version":1,"state":"dismissed","id":"no"}');
    expect(readOnboarding(storage)).toBe("unseen");
    writeOnboarding(storage, "completed");
    expect(readOnboarding(storage)).toBe("completed");
  });
});
