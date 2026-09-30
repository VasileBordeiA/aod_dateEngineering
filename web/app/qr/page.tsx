import type { Metadata } from "next";
import { QrCode } from "@/components/QrCode";
import { wifiQrPayload } from "@/lib/wifi";

export const metadata: Metadata = { title: "Join · NYC Taxi Live" };
export const dynamic = "force-dynamic"; // read the Wi-Fi settings at request time, server side only (spec 08)

export default function QrPage() {
  const ssid = process.env.STAND_WIFI_SSID || "NYC-Taxi-Live";
  const password = process.env.STAND_WIFI_PASSWORD || "";
  const stand = (process.env.STAND_PUBLIC_URL || "http://192.168.8.10").replace(/\/$/, "");
  return (
    <main className="grid min-h-dvh grid-cols-1 place-items-center gap-10 p-8 md:grid-cols-2">
      <figure data-testid="qr-wifi" className="flex flex-col items-center gap-4">
        <QrCode value={wifiQrPayload(ssid, password)} title="Join the stand Wi-Fi" />
        <figcaption className="text-center text-3xl font-semibold">1. Join Wi-Fi <span className="text-visitor-ink">{ssid}</span></figcaption>
      </figure>
      <figure data-testid="qr-ride" className="flex flex-col items-center gap-4">
        <QrCode value={`${stand}/ride`} title="Open the ride page" />
        <figcaption className="text-center text-3xl font-semibold">2. Open <span className="text-visitor-ink">{stand}/ride</span></figcaption>
      </figure>
    </main>
  );
}
