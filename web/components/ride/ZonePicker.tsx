"use client";
import { useMemo, useRef, useState } from "react";
import type { Zone } from "@/lib/api";
import { BOROUGH_COLORS } from "@/lib/palette";
import { filterZones, groupByBorough, POPULAR_ZONE_IDS } from "@/lib/ride";

/** A big button that opens a full-screen, searchable zone list grouped by borough. The search text stays on the phone. */
export function ZonePicker({ label, zones, value, onChange }: { label: string; zones: Zone[] | null; value: Zone | null; onChange: (z: Zone) => void }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const triggerRef = useRef<HTMLButtonElement>(null);
  const popular = useMemo(() => POPULAR_ZONE_IDS.flatMap((id) => zones?.find((z) => z.location_id === id) ?? []), [zones]);
  const groups = useMemo(() => groupByBorough(filterZones(zones ?? [], query)), [zones, query]);
  // Closing (pick, Cancel, Escape) always returns focus to the trigger button that opened this dialog.
  const close = () => {
    setOpen(false);
    setQuery("");
    triggerRef.current?.focus();
  };
  const pick = (z: Zone) => {
    onChange(z);
    close();
  };
  const option = "min-h-11 rounded-lg px-3 text-left text-base hover:bg-panel focus-visible:outline-2 focus-visible:outline-visitor";
  return (
    <>
      <button ref={triggerRef} type="button" aria-label={label} aria-haspopup="dialog" disabled={!zones} onClick={() => setOpen(true)}
        className="flex min-h-16 w-full flex-col justify-center rounded-xl border-2 border-line bg-panel px-4 py-2 text-left disabled:opacity-50">
        <span className="text-sm text-muted">{label}</span>
        <span className="text-lg font-medium">{value ? value.zone : zones ? "Choose a zone" : "Loading zones…"}</span>
        {value && <span className="text-sm text-muted">{value.borough}</span>}
      </button>
      {open && (
        <div role="dialog" aria-modal="true" aria-label={label} onKeyDown={(e) => { if (e.key === "Escape") close(); }}
          className="fixed inset-0 z-50 flex flex-col bg-surface">
          <div className="flex items-center gap-2 border-b border-line p-3">
            <input type="search" aria-label="Search zones" placeholder="Search zones…" value={query} autoFocus
              onChange={(e) => setQuery(e.target.value)} autoComplete="off"
              className="min-h-12 min-w-0 flex-1 rounded-lg border border-line bg-panel px-3 text-base text-ink outline-none focus-visible:border-visitor" />
            <button type="button" onClick={close} className="min-h-12 px-3 text-muted">Cancel</button>
          </div>
          <div className="flex-1 overflow-y-auto px-3 pb-8">
            {!query.trim() && popular.length > 0 && (
              <section aria-label="Popular" className="py-3">
                <h2 className="pb-2 text-sm font-semibold text-muted">Popular</h2>
                <div className="flex flex-wrap gap-2">
                  {popular.map((z) => (
                    <button key={z.location_id} type="button" onClick={() => pick(z)}
                      className="min-h-11 rounded-full border border-visitor px-4 text-sm text-visitor-ink">{z.zone}</button>
                  ))}
                </div>
              </section>
            )}
            {groups.map(([borough, list]) => (
              <section key={borough} aria-label={borough}>
                <h2 className="sticky top-0 flex items-center gap-2 bg-surface py-2 text-sm font-semibold">
                  <span aria-hidden className="size-3 rounded-full" style={{ background: BOROUGH_COLORS[borough] ?? "#9a9890" }} />
                  {borough}
                </h2>
                <ul className="flex flex-col">
                  {list.map((z) => (
                    <li key={z.location_id}><button type="button" onClick={() => pick(z)} className={`${option} w-full`}>{z.zone}</button></li>
                  ))}
                </ul>
              </section>
            ))}
            {groups.length === 0 && <p className="py-8 text-center text-muted">No zone matches that.</p>}
          </div>
        </div>
      )}
    </>
  );
}
