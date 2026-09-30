"use client";
import { useEffect, useState } from "react";

export function usePoll<T>(load: () => Promise<T>, everyMs: number): T | null {
  const [value, setValue] = useState<T | null>(null);
  useEffect(() => {
    let alive = true;
    const tick = () => {
      load()
        .then((v) => {
          if (alive) setValue(v);
        })
        .catch(() => undefined);
    };
    tick();
    const id = setInterval(tick, everyMs);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [load, everyMs]);
  return value;
}
