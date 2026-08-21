"use client";

import * as React from "react";

export type ThemePreference = "system" | "light" | "dark";
export type EffectiveTheme = "light" | "dark";
export const THEME_STORAGE_KEY = "converge:theme:v1";

export function parseThemePreference(value: string | null): ThemePreference {
  if (!value) return "system";
  try {
    const parsed: unknown = JSON.parse(value);
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      Object.keys(parsed).length === 2 &&
      (parsed as { version?: unknown }).version === 1 &&
      ["system", "light", "dark"].includes(
        (parsed as { preference?: unknown }).preference as string,
      )
    )
      return (parsed as { preference: ThemePreference }).preference;
  } catch {
    // Invalid device-local preference is intentionally ignored.
  }
  return "system";
}

export function effectiveTheme(preference: ThemePreference, systemDark: boolean): EffectiveTheme {
  return preference === "system" ? (systemDark ? "dark" : "light") : preference;
}

type ThemeSnapshot = Readonly<{
  preference: ThemePreference;
  effective: EffectiveTheme;
  setPreference(preference: ThemePreference): void;
}>;

const ThemeContext = React.createContext<ThemeSnapshot | null>(null);

export function ThemeProvider({ children }: { children: React.ReactNode }): React.JSX.Element {
  const [preference, setStoredPreference] = React.useState<ThemePreference>(() =>
    typeof document === "undefined"
      ? "system"
      : parseThemePreference(document.documentElement.dataset.themePreference ?? null),
  );
  const [systemDark, setSystemDark] = React.useState(
    () =>
      typeof window !== "undefined" && window.matchMedia("(prefers-color-scheme: dark)").matches,
  );
  React.useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const update = (): void => setSystemDark(media.matches);
    setStoredPreference(parseThemePreference(window.localStorage.getItem(THEME_STORAGE_KEY)));
    update();
    media.addEventListener("change", update);
    const storage = (event: StorageEvent): void => {
      if (event.key === THEME_STORAGE_KEY)
        setStoredPreference(parseThemePreference(event.newValue));
    };
    window.addEventListener("storage", storage);
    return () => {
      media.removeEventListener("change", update);
      window.removeEventListener("storage", storage);
    };
  }, []);
  const effective = effectiveTheme(preference, systemDark);
  React.useEffect(() => {
    const root = document.documentElement;
    root.dataset.theme = effective;
    root.dataset.themePreference = preference;
    root.style.colorScheme = effective;
  }, [effective, preference]);
  const setPreference = React.useCallback((next: ThemePreference): void => {
    setStoredPreference(next);
    try {
      window.localStorage.setItem(
        THEME_STORAGE_KEY,
        JSON.stringify({ version: 1, preference: next }),
      );
    } catch {
      // The in-memory page preference remains usable.
    }
  }, []);
  const snapshot = React.useMemo(
    () => ({ preference, effective, setPreference }),
    [effective, preference, setPreference],
  );
  return <ThemeContext.Provider value={snapshot}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeSnapshot {
  const theme = React.useContext(ThemeContext);
  if (!theme) throw new Error("ThemeProvider is required");
  return theme;
}
