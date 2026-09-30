import type { Metadata } from "next";
import { TraceViewer } from "@/components/trace/TraceViewer";

export const metadata: Metadata = { title: "Your ride · NYC Taxi Live" };

export default async function TracePage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return (
    <main className="phone mx-auto min-h-dvh w-full max-w-md px-4 py-6">
      <TraceViewer traceId={id.toLowerCase()} />
    </main>
  );
}
