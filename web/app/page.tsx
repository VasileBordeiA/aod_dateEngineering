import Link from "next/link";

const GRAFANA = process.env.NEXT_PUBLIC_GRAFANA_PATH ?? "/grafana/";

const tiles = [
  { href: "/agent", title: "Ask the data", text: "Ask a question in plain English and watch the AI write the SQL." },
  { href: GRAFANA, title: "Live dashboard", text: "What NYC taxis are doing right now, replayed at 60×." },
];

export default function Hub() {
  return (
    <main className="mx-auto flex min-h-dvh max-w-5xl flex-col justify-center gap-10 p-8">
      <h1 className="text-5xl font-semibold">NYC Taxi Live</h1>
      <div className="grid gap-6 md:grid-cols-2">
        {tiles.map((t) => (
          <Link key={t.href} href={t.href}
            className="rounded-2xl border border-visitor bg-visitor-fill p-8 text-visitor-ink transition hover:border-visitor-ink">
            <h2 className="text-3xl font-semibold">{t.title}</h2>
            <p className="mt-3 text-lg">{t.text}</p>
          </Link>
        ))}
      </div>
    </main>
  );
}
