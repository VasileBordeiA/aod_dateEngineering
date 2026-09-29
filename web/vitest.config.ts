import { defineConfig } from "vitest/config";

export default defineConfig({
  // Vite 8 (a vitest 5 peer) only resolves tsconfig "paths" (our "@/*" alias) with this on;
  // it defaults to false. Without it every "@/lib/..." import fails to resolve. Not in the task brief's
  // vitest.config.ts — added as the minimal fix (deviation, see task-10-report.md).
  resolve: { tsconfigPaths: true },
  test: { include: ["tests/unit/**/*.test.ts"], environment: "node" },
});
