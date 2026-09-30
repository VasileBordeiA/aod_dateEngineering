import type { Metadata } from "next";
import { RideForm } from "@/components/ride/RideForm";

export const metadata: Metadata = { title: "Book a ride · NYC Taxi Live" };

export default function RidePage() {
  return (
    <main className="phone mx-auto flex min-h-dvh w-full max-w-md flex-col gap-5 px-4 py-6">
      <h1 className="text-2xl font-semibold">Book a pretend taxi ride</h1>
      <p className="text-muted">Pick two NYC zones, send the ride, and watch it travel through our data pipeline.</p>
      <RideForm />
    </main>
  );
}
