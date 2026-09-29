import type { NextConfig } from "next";

// Dev only: proxy /api to a locally running api. On the stand, Caddy serves web and api on one origin (spec 08).
const apiOrigin = process.env.API_ORIGIN;

const nextConfig: NextConfig = {
  output: "standalone",
  poweredByHeader: false,
  async rewrites() {
    return apiOrigin ? [{ source: "/api/:path*", destination: `${apiOrigin}/api/:path*` }] : [];
  },
};

export default nextConfig;
