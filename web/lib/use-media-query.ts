"use client";
import { useCallback, useSyncExternalStore } from "react";

/** Tailwind's `lg`: the stand laptop gets the two-column kiosk from here up; below it is a phone or tablet. */
export const STAND_SCREEN = "(min-width: 64rem)";

/** True while the media query matches. The server render assumes it doesn't. */
export function useMediaQuery(query: string): boolean {
  const subscribe = useCallback(
    (onChange: () => void) => {
      const mql = window.matchMedia(query);
      mql.addEventListener("change", onChange);
      return () => mql.removeEventListener("change", onChange);
    },
    [query],
  );
  return useSyncExternalStore(subscribe, () => window.matchMedia(query).matches, () => false);
}
