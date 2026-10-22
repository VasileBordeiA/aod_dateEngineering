import { Highlight, type PrismTheme } from "prism-react-renderer";
import type { Attempt } from "@/lib/agent-state";
import { flowSql } from "@/lib/sql-display";

/** SQL highlighting in the pipeline purples; every token clears WCAG AA on the panel (#181b1f). */
const THEME: PrismTheme = {
  plain: { color: "#eeedfe", backgroundColor: "transparent" },
  styles: [
    { types: ["keyword"], style: { color: "#b3abff", fontWeight: "600" } },
    { types: ["function"], style: { color: "#d9d4ff" } },
    { types: ["number", "string", "boolean"], style: { color: "#9fe3c9" } },
    { types: ["operator", "punctuation"], style: { color: "#a3a29b" } },
    { types: ["comment"], style: { color: "#a3a29b", fontStyle: "italic" } },
  ],
};

function Sql({ code }: { code: string }) {
  return (
    <Highlight code={flowSql(code)} language="sql" theme={THEME}>
      {({ tokens, getLineProps, getTokenProps }) => (
        <pre className="font-mono text-[0.875rem] leading-snug whitespace-pre-wrap break-words lg:text-[1.05rem]">
          {tokens.map((line, i) => (
            <div key={i} {...getLineProps({ line })} style={undefined}>
              {line.map((token, k) => <span key={k} {...getTokenProps({ token })} />)}
            </div>
          ))}
        </pre>
      )}
    </Highlight>
  );
}

function Failed({ attempt }: { attempt: Attempt }) {
  return (
    <div className="border-l-4 border-amber pl-4">
      <del className="block font-mono text-sm break-words text-muted decoration-amber decoration-2 lg:text-base">{flowSql(attempt.sql)}</del>
      <p className="mt-1 text-sm text-amber lg:text-base">
        <span aria-hidden>✗ </span>
        {attempt.failed}
      </p>
    </div>
  );
}

export function SqlPanel({ attempts }: { attempts: Attempt[] }) {
  const current = attempts.at(-1);
  // On the stand the panel gives up height before the chart and the answer do; long SQL scrolls inside it.
  return (
    <div data-testid="sql-panel" className="flex flex-col rounded-xl border-2 border-pipeline bg-panel p-4 lg:min-h-[8rem] lg:p-5">
      <div className="mb-3 flex shrink-0 items-center gap-3">
        <h2 className="text-lg font-semibold text-pipeline-ink">SQL</h2>
        <span className="rounded-md border border-pipeline bg-pipeline-fill px-2 text-sm text-pipeline-ink">+ safety LIMIT 200</span>
        {/* Spec 06: the counter only appears once the AI has retried. No "/max": tools and oneshot allow different counts. */}
        {current && current.attempt > 1 && <span className="ml-auto text-base text-muted tabular-nums">attempt {current.attempt}</span>}
      </div>
      {!current && <p className="text-base text-muted lg:text-lg">The SQL the AI writes appears here, always.</p>}
      <div className="flex max-h-[60vh] min-h-0 flex-col gap-4 overflow-y-auto lg:max-h-none">
        {attempts.slice(0, -1).map((a, i) => <Failed key={i} attempt={a} />)}
        {current && (current.failed ? <Failed attempt={current} /> : (
          <div>
            {current.purpose && <p className="mb-1 text-sm text-muted lg:text-base">{current.purpose}</p>}
            <Sql code={current.sql} />
          </div>
        ))}
      </div>
    </div>
  );
}
