"use client";
import { QRCodeSVG } from "qrcode.react";

/** Generated locally: no network needed (offline-first). Black on white with a quiet zone, so any phone camera reads it. */
export function QrCode({ value, size = 320, title }: { value: string; size?: number; title: string }) {
  return (
    <div className="rounded-2xl bg-white p-4">
      <QRCodeSVG value={value} size={size} marginSize={2} level="M" bgColor="#ffffff" fgColor="#000000" title={title} />
    </div>
  );
}
