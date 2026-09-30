import type { RideEstimate } from "@/lib/api";
import { estimateLine } from "@/lib/ride";

export function EstimateCard({ ready, estimate }: { ready: boolean; estimate: RideEstimate | null }) {
  return (
    <div className="rounded-xl border border-line bg-panel px-4 py-3 text-base">
      {!ready ? <span className="text-muted">Pick both zones to see an estimate.</span>
        : estimate ? <span data-testid="estimate">{estimateLine(estimate)}</span>
        : <span className="text-muted">Estimating…</span>}
    </div>
  );
}
