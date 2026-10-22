import type { Metadata } from "next";
import { QrCode } from "@/components/QrCode";

export const metadata: Metadata = { title: "Book a ride · NYC Taxi Live" };
export const dynamic = "force-dynamic"; // read STAND_PUBLIC_URL at request time, server side only (spec 08)

export default function QrPage() {
  const stand = (process.env.STAND_PUBLIC_URL || "http://192.168.8.10").replace(/\/$/, "");
  return (
    <main className="grid min-h-dvh place-items-center p-8">
      <figure data-testid="qr-ride" className="flex flex-col items-center gap-4">
        <QrCode value={`${stand}/ride`} title="Open the ride page" />
        <figcaption className="text-center text-3xl font-semibold">Book a ride at <span className="text-visitor-ink">{stand}/ride</span></figcaption>
      </figure>
    </main>
  );
}
