import type { Metadata } from "next";
import { LiveWall } from "@/components/trace/LiveWall";

export const metadata: Metadata = { title: "Live rides · NYC Taxi Live" };
export const dynamic = "force-dynamic"; // STAND_PUBLIC_URL is read when the page is served, never baked into the image

export default function LivePage() {
  const stand = (process.env.STAND_PUBLIC_URL || "http://192.168.8.10").replace(/\/$/, "");
  return <LiveWall standUrl={stand} />;
}
