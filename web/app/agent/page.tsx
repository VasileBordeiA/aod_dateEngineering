import type { Metadata } from "next";
import { AgentKiosk } from "@/components/agent/AgentKiosk";
import { isTraceId } from "@/lib/trace";

export const metadata: Metadata = { title: "Ask the data · NYC Taxi Live" };

type Search = Promise<Record<string, string | string[] | undefined>>;

export default async function AgentPage({ searchParams }: { searchParams: Search }) {
  const sp = await searchParams;
  // /agent?trace_id=…&suggestion=ride_rank comes from the trace viewer's "rank my ride" button (spec 07 §Hand-off).
  const id = typeof sp.trace_id === "string" ? sp.trace_id.toLowerCase() : "";
  const rideTraceId = sp.suggestion === "ride_rank" && isTraceId(id) ? id : undefined;
  return <AgentKiosk rideTraceId={rideTraceId} />;
}
