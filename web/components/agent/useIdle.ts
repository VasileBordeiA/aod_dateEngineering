"use client";
import { useCallback, useEffect, useRef, useState } from "react";

/** True after `ms` without a pointer or key event. `activity` changing (new SSE events) also counts as activity. */
export function useIdle(ms: number, onIdle: () => void, activity: unknown): [boolean, () => void] {
  const [idle, setIdle] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const arm = useCallback(() => {
    clearTimeout(timer.current);
    timer.current = setTimeout(() => {
      setIdle(true);
      onIdle();
    }, ms);
  }, [ms, onIdle]);
  const wake = useCallback(() => {
    setIdle(false);
    arm();
  }, [arm]);
  useEffect(() => {
    const events = ["pointerdown", "keydown", "touchstart"] as const;
    events.forEach((e) => window.addEventListener(e, wake));
    arm();
    return () => {
      events.forEach((e) => window.removeEventListener(e, wake));
      clearTimeout(timer.current);
    };
  }, [arm, wake]);
  useEffect(() => arm(), [activity, arm]);
  return [idle, wake];
}
