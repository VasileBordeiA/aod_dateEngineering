"use client";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useState, useSyncExternalStore } from "react";
import { bookRide, getEstimate, getZones, type RideEstimate, type Zone } from "@/lib/api";
import { loadLastTrace, saveLastTrace, tipAmount, type TipChoice } from "@/lib/ride";
import { EstimateCard } from "./EstimateCard";
import { PassengerStepper } from "./PassengerStepper";
import { TipChips } from "./TipChips";
import { ZonePicker } from "./ZonePicker";

const noSubscribe = () => () => {};
const pairKey = (a: Zone | null, b: Zone | null) => (a && b ? `${a.location_id}-${b.location_id}` : null);

export function RideForm() {
  const router = useRouter();
  const [zones, setZones] = useState<Zone[] | null>(null);
  const [pickup, setPickup] = useState<Zone | null>(null);
  const [dropoff, setDropoff] = useState<Zone | null>(null);
  // The estimate remembers which zone pair it is for, so changing a zone never shows the old pair's estimate.
  const [estimated, setEstimated] = useState<{ key: string; estimate: RideEstimate } | null>(null);
  const [payment, setPayment] = useState<"card" | "cash">("card");
  const [tip, setTip] = useState<TipChoice>({ kind: "pct", pct: 15 });
  const [passengers, setPassengers] = useState(1);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // localStorage exists only in the browser: null on the server, the stored id after hydration.
  const lastTrace = useSyncExternalStore(noSubscribe, loadLastTrace, () => null);

  useEffect(() => {
    let alive = true;
    let retry: ReturnType<typeof setTimeout>;
    const load = () => getZones().then((z) => { if (alive) setZones(z); }).catch(() => { if (alive) retry = setTimeout(load, 3000); });
    load();
    return () => { alive = false; clearTimeout(retry); };
  }, []);

  useEffect(() => {
    const key = pairKey(pickup, dropoff);
    if (!pickup || !dropoff || !key) return;
    let alive = true;
    getEstimate(pickup.location_id, dropoff.location_id)
      .then((e) => { if (alive) setEstimated({ key, estimate: e }); })
      .catch(() => undefined); // the api estimates again on booking, so this is only a preview
    return () => { alive = false; };
  }, [pickup, dropoff]);

  const estimate = estimated && estimated.key === pairKey(pickup, dropoff) ? estimated.estimate : null;
  const fare = estimate?.fare_amount ?? null;
  const book = async () => {
    if (!pickup || !dropoff || busy) return;
    setBusy(true);
    setError(null);
    const result = await bookRide({ pickup_location_id: pickup.location_id, dropoff_location_id: dropoff.location_id,
      payment, tip_amount: tipAmount(tip, fare), passenger_count: passengers });
    if (result.ok) {
      saveLastTrace(result.ride.trace_id);
      router.push(result.ride.trace_url);
      return;
    }
    setBusy(false);
    setError(result.message);
  };

  const toggle = (on: boolean) => `min-h-12 flex-1 rounded-xl border-2 font-medium ${on ? "border-visitor bg-visitor-fill text-visitor-ink" : "border-line"}`;
  return (
    <form className="flex flex-col gap-5" onSubmit={(e) => { e.preventDefault(); void book(); }}>
      {lastTrace && (
        <Link href={`/trace/${lastTrace}`} className="rounded-xl border border-visitor px-4 py-3 text-center text-visitor-ink">
          Continue watching your ride
        </Link>
      )}
      <ZonePicker label="Pickup zone" zones={zones} value={pickup} onChange={setPickup} />
      <ZonePicker label="Drop-off zone" zones={zones} value={dropoff} onChange={setDropoff} />
      <EstimateCard ready={!!pickup && !!dropoff} estimate={estimate} />
      <fieldset className="flex flex-col gap-2">
        <legend className="pb-2 font-medium">Payment</legend>
        <div className="flex gap-2">
          <button type="button" aria-pressed={payment === "card"} onClick={() => setPayment("card")} className={toggle(payment === "card")}>Card</button>
          <button type="button" aria-pressed={payment === "cash"} onClick={() => setPayment("cash")} className={toggle(payment === "cash")}>Cash</button>
        </div>
        {payment === "cash" && <p className="text-sm text-muted">Cash tips aren&apos;t recorded in NYC taxi data</p>}
      </fieldset>
      <TipChips value={tip} onChange={setTip} fare={fare} />
      <PassengerStepper value={passengers} onChange={setPassengers} />
      {error && <p data-testid="ride-error" role="alert" className="rounded-xl bg-[#4a3300] px-4 py-3 text-[#ffe7b0]">{error}</p>}
      <button type="submit" disabled={!pickup || !dropoff || busy}
        className="min-h-14 rounded-xl bg-visitor text-xl font-semibold text-black disabled:opacity-40">
        {busy ? "Booking…" : "Book ride"}
      </button>
    </form>
  );
}
