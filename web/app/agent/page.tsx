import type { Metadata } from "next";
import { AgentKiosk } from "@/components/agent/AgentKiosk";

export const metadata: Metadata = { title: "Ask the data · NYC Taxi Live" };

export default function AgentPage() {
  return <AgentKiosk />;
}
