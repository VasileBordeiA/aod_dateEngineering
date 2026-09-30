# Diagrams

All diagrams are Mermaid, so they render on GitHub, GitLab, VS Code (Markdown Preview Mermaid), and in Grafana text panels. The relevant specs embed the same diagrams. **When a diagram changes, update it here first.**

Colour code (matches the original architecture image):
- **Purple** is the core pipeline.
- **Green** is visitor-facing.
- **Grey** is the data source.
- **Dashed lines** are trace checkpoints.

1. [System architecture](#1-system-architecture)
2. [Topic & data-flow topology](#2-topic--data-flow-topology)
3. [Agent question: sequence](#3-agent-question--sequence)
4. [Trace your ride: sequence](#4-trace-your-ride--sequence)
5. [Trace checkpoint stages: state machine](#5-trace-checkpoint-stages--state-machine)
6. [ClickHouse schema: ER diagram](#6-clickhouse-schema--er-diagram)
7. [Deployment & network](#7-deployment--network)
8. [LLM fallback decision flow](#8-llm-fallback-decision-flow)
9. [Visitor journey at the stand](#9-visitor-journey-at-the-stand)
10. [Timeline (Gantt)](#10-timeline-gantt)
11. [Sim time vs wall time](#11-sim-time-vs-wall-time)
12. [Stream processor dataflow](#12-stream-processor-dataflow)

---

## 1. System architecture

```mermaid
flowchart TB
    replayer["<b>Dataset replayer</b><br/>NYC taxi trips, replayed 60×"]
    ride["<b>Trace your ride</b><br/>Student QR form"]
    rp["<b>Redpanda</b><br/>Kafka-compatible broker"]
    proc["<b>Stream processor</b><br/>Quix Streams: clean, enrich, aggregate"]
    ch["<b>ClickHouse</b><br/>Raw + aggregate tables"]
    dash["<b>Live dashboard</b><br/>Grafana, 5 s refresh"]
    agent["<b>AI data agent</b><br/>Text-to-SQL, read-only"]
    tv["<b>Trace viewer</b><br/>Stage-by-stage view"]

    replayer --> rp
    ride --> rp
    rp --> proc
    proc --> ch
    ch --> dash
    ch --> agent
    proc -.->|trace checkpoints| tv
    ride -.-> tv
    ch -.-> tv

    classDef source fill:#46463f,stroke:#46463f,color:#f1efe8
    classDef core fill:#3c3489,stroke:#534ab7,color:#eeedfe
    classDef visitor fill:#0b4d3f,stroke:#1d9e75,color:#e1f5ee
    class replayer source
    class rp,proc,ch core
    class ride,dash,agent,tv visitor
```

## 2. Topic & data-flow topology

Producers → topics → consumers → tables. `trips.raw` and `trips.enriched` have 1 partition (ordered event time, ADR-014). The others have 3. All use size-based retention of 1 GiB per partition (ADR-015), except `sim.clock`, which is compacted.

```mermaid
flowchart LR
    subgraph producers[Producers]
        R[replayer]
        A1[api · POST /api/rides]
    end
    subgraph topics[Redpanda topics]
        raw[(trips.raw)]
        enr[(trips.enriched)]
        rej[(trips.rejected)]
        agg[(agg.borough_minute)]
        tr[(trace.checkpoints)]
        clk[(sim.clock)]
    end
    subgraph processor[processor · Quix Streams]
        V[validate] --> E[enrich]
        W[1-min tumbling window<br/>per pu_borough]
        S1[ClickHouse sink<br/>trips]
        S2[ClickHouse sink<br/>rejected]
        S3[ClickHouse sink<br/>aggregates]
    end
    subgraph clickhouse[ClickHouse · db taxi]
        T1[[trips]]
        T2[[trips_rejected]]
        T3[[agg_borough_minute]]
        T4[[trace_events]]
        T5[[zones]]
        T6[[agent_log]]
        VW{{sim_now view}}
    end
    A2[api · trace relay + sim clock]

    R --> raw
    R --> clk
    R -.->|"sampled 0.1%"| tr
    A1 --> raw
    A1 -.-> tr
    raw --> V
    V -- invalid --> rej --> S2 --> T2
    E --> enr
    enr --> W
    E --> S1 --> T1
    W --> agg --> S3 --> T3
    V -.-> tr
    E -.-> tr
    W -.-> tr
    S1 -.->|stored| tr
    tr -->|Kafka engine + MV| T4
    tr --> A2
    clk --> A2
    A2 -. queryable .-> tr
    T1 --> VW
```

## 3. Agent question: sequence

The API streams every step to the browser as Server-Sent Events, so students see the agent "think".

```mermaid
sequenceDiagram
    autonumber
    actor S as Student
    participant UI as web /agent
    participant API as api (FastAPI)
    participant LLM as LLM (Ollama / internal)
    participant G as SQL guard
    participant CH as ClickHouse (agent_ro)

    S->>UI: types question or taps a suggestion
    UI->>API: POST /api/agent/ask (SSE)
    API-->>UI: event: status "Reading the schema…"
    API->>LLM: system prompt + question + tool defs
    LLM->>API: tool_call list_tables()
    API-->>UI: event: tool_call / tool_result
    LLM->>API: tool_call describe_table("trips")
    API->>CH: SELECT from system.columns (curated whitelist)
    API-->>UI: event: tool_call / tool_result
    LLM->>API: tool_call run_query(sql)
    API->>G: validate + inject LIMIT
    alt guard rejects
        G-->>API: reason
        API->>LLM: tool error: reason
    else guard ok
        API-->>UI: event: sql
        API->>CH: execute (readonly, 5 s, LIMIT 200)
        alt ClickHouse error (max 2 repairs)
            CH-->>API: error
            API-->>UI: event: status "Fixing its own query…"
            API->>LLM: tool error: message
        else rows
            CH-->>API: rows
            API-->>UI: event: rows
        end
    end
    LLM->>API: tool_call render_chart(spec)
    API-->>UI: event: chart
    LLM->>API: final answer text
    API-->>UI: event: answer, then done
    API->>CH: INSERT agent_log (ingest_writer)
```

## 4. Trace your ride: sequence

```mermaid
sequenceDiagram
    autonumber
    actor P as Student phone
    participant W as web /ride + /trace/[id]
    participant API as api
    participant RP as Redpanda
    participant PR as processor
    participant CH as ClickHouse

    P->>W: scan QR, pick zones + tip
    W->>API: POST /api/rides
    API->>API: estimate distance/fare, pickup_ts = sim clock
    API->>RP: trips.raw (key = trip_id)
    RP-->>API: delivery ack (partition, offset)
    API->>RP: trace.checkpoints: submitted + ingested
    API-->>W: 201 {trace_id}
    W->>API: WS /api/trace/{trace_id}
    API-->>W: replay history so far
    RP->>PR: consume trips.raw
    PR->>RP: validated (or rejected → end)
    PR->>RP: enriched
    PR->>RP: aggregated
    PR->>CH: batch INSERT taxi.trips
    PR->>RP: stored
    RP->>API: relay consumes trace.checkpoints
    API-->>W: push each checkpoint (WS)
    API->>CH: batched poll: DISTINCT trace_id IN pending ids (trace_ro)
    API->>RP: queryable
    API-->>W: push queryable
    W->>P: "Ask the agent where your ride ranks"
```

## 5. Trace checkpoint stages: state machine

```mermaid
stateDiagram-v2
    direction LR
    [*] --> submitted: POST /api/rides
    submitted --> ingested: broker ack
    ingested --> validated: rules pass
    ingested --> rejected: any rule fails
    validated --> enriched: zone lookup + derived fields
    enriched --> aggregated: added to 1-min window
    aggregated --> stored: ClickHouse insert ok
    stored --> queryable: row visible to agent_ro
    queryable --> [*]
    rejected --> [*]

    note right of rejected
        e.g. TIP_UNREALISTIC
        shown to the student as a
        data-quality lesson
    end note
```

Replay trips sampled for tracing start at `ingested`, emitted by the replayer. They have no `submitted` stage.

## 6. ClickHouse schema: ER diagram

```mermaid
erDiagram
    zones ||--o{ trips : "pu_location_id / do_location_id"
    trips ||--o{ trace_events : "trace_id"
    trips }o--|| agg_borough_minute : "pu_borough + minute of pickup_ts"
    trips_rejected }o--o{ trace_events : "trace_id"

    zones {
        UInt16 location_id PK
        String borough
        String zone
        String service_zone
        Float64 lat
        Float64 lon
    }
    trips {
        String trip_id PK
        String source
        UUID trace_id
        DateTime64 pickup_ts
        DateTime64 dropoff_ts
        UInt16 pu_location_id FK
        UInt16 do_location_id FK
        String pu_borough
        String pu_zone
        Float64 trip_distance
        Float32 duration_min
        UInt8 payment_type
        Bool is_card
        Float64 fare_amount
        Float64 tip_amount
        Float32 tip_pct
        Float64 total_amount
        DateTime64 stored_at
    }
    trips_rejected {
        String trip_id
        String source
        UUID trace_id
        DateTime64 pickup_ts
        Array reasons
        String payload
        DateTime64 rejected_at
    }
    agg_borough_minute {
        DateTime64 window_start PK
        String borough PK
        UInt32 trips
        Float64 revenue
        Float64 avg_fare
        Float64 avg_card_tip_pct
        UInt32 student_trips
    }
    trace_events {
        UUID trace_id
        String stage
        UInt8 seq
        String service
        DateTime64 at
        String detail
    }
    agent_log {
        DateTime64 asked_at
        String question
        String final_sql
        Bool success
        UInt32 latency_ms
        String provider
    }
```

## 7. Deployment & network

```mermaid
flowchart TB
    subgraph laptop["Stand laptop · macOS · 32 GB · static IP 192.168.8.10"]
        subgraph docker["Docker Compose (VM 14 GB)"]
            caddy["caddy :80<br/>(only port exposed to LAN)"]
            web["web · Next.js :3000"]
            api["api · FastAPI :8000"]
            graf["grafana :3000"]
            rep["replayer (+control :8001)"]
            proc["processor"]
            rp["redpanda :9092"]
            con["redpanda-console :8080"]
            ch["clickhouse :8123"]
        end
        ollama["Ollama (native, Metal GPU)<br/>:11434"]
        screens["Big screen: Grafana kiosk<br/>Laptop screen: /agent<br/>Tablet: /trace/live"]
    end
    router["Travel router · SSID 'NYC-Taxi-Live'<br/>DHCP + static lease"]
    phones["Student phones<br/>scan QR → /ride"]
    inet(("Internet<br/>(venue Wi-Fi WAN or phone hotspot)"))
    internal["Company internal AI gateway<br/>OpenAI-compatible, API key"]

    phones -- Wi-Fi --> router
    router -- Ethernet/Wi-Fi --> caddy
    caddy --> web
    caddy -->|/api/*| api
    caddy -->|/grafana/*| graf
    api --> ch
    api --> rp
    api -->|host.docker.internal| ollama
    api -. optional .-> inet -.-> internal
    router -. WAN .-> inet
    rep --> rp --> proc --> ch
    graf --> ch
    screens --- caddy

    classDef core fill:#3c3489,stroke:#534ab7,color:#eeedfe
    classDef visitor fill:#0b4d3f,stroke:#1d9e75,color:#e1f5ee
    classDef ext fill:#46463f,stroke:#46463f,color:#f1efe8
    class rp,proc,ch,rep core
    class web,api,graf,caddy,screens,phones visitor
    class inet,internal,router,ollama ext
```

## 8. LLM fallback decision flow

```mermaid
flowchart TD
    q[Question arrives] --> len{"Length ≤ 300 chars<br/>and rate limit ok?"}
    len -- no --> refuse[Polite refusal event]
    len -- yes --> p1{"Provider 1 healthy?<br/>(LLM_ORDER[0])"}
    p1 -- yes --> run1[Run agent loop]
    p1 -- no --> p2
    run1 -- ok --> answer[Stream answer]
    run1 -- timeout / 5xx / no tool calls --> p2{"Provider 2 configured<br/>and healthy?"}
    p2 -- yes --> run2[Run agent loop] -- ok --> answer
    run2 -- fails --> sugg
    p2 -- no --> sugg{"Is it one of the<br/>5 suggested questions?"}
    sugg -- yes --> cached["Re-run the cached reference SQL<br/>on LIVE data + templated answer"]
    cached --> answer
    sugg -- no --> sorry["'The AI is taking a break,<br/>try a suggested question'"]
    answer --> log[(agent_log)]
    sorry --> log
    refuse --> log
```

The health check is a background task that calls `GET {base_url}/models` every 15 s. A provider is marked unhealthy after 2 consecutive failures and healthy again after 1 success.

## 9. Visitor journey at the stand

```mermaid
flowchart LR
    arrive([Student arrives]) --> pitch["30 s pitch<br/>architecture poster"]
    pitch --> dash["Point at live dashboard<br/>'this is NYC, right now, at 60×'"]
    dash --> ask{"Wants to try?"}
    ask -- yes --> agent["Ask the agent<br/>(suggested button or own question)"]
    agent --> sql["Read the SQL together<br/>'the AI is only as good as the data model'"]
    sql --> ride{"Curious for more?"}
    ride -- yes --> qr["Scan QR → book a ride"]
    qr --> trace["Watch it travel the pipeline"]
    trace --> rank["'Where does my ride rank for tips today?'"]
    rank --> bye
    ride -- no --> bye([Careers flyer + goodbye])
    ask -- no --> bye
```

## 10. Timeline (Gantt)

This assumes the event is on **Fri 2026-10-16**. Shift every date if `EVENT_DATE` differs.

```mermaid
gantt
    title NYC Taxi Live: build plan
    dateFormat YYYY-MM-DD
    excludes weekends
    axisFormat %d %b

    section M0 Foundations
    Repo, compose, data prep, contracts frozen :m0, 2026-09-28, 1d
    section M1 Core pipeline
    Replayer                                   :a1, after m0, 2d
    Processor validate + enrich + sinks        :a2, after m0, 3d
    ClickHouse DDL + users                     :a3, after m0, 1d
    section M2 Dashboard
    Windowed aggregates                        :a4, after a2, 1d
    Grafana dashboards                         :c1, after a3, 3d
    section M3 AI agent
    Agent API, tools, guard, SSE               :b1, after m0, 4d
    Eval harness + model selection             :b2, after b1, 2d
    /agent UI                                  :c2, after c1, 3d
    Core stable 2h (go/no-go)                  :milestone, gng, 2026-10-09, 0d
    section M4 Trace your ride
    Rides API + checkpoints + WS relay         :b3, 2026-10-09, 2d
    /ride, /trace/[id], /trace/live            :c3, 2026-10-09, 3d
    Event day                                  :milestone, ev, 2026-10-16, 0d
```

## 11. Sim time vs wall time

```mermaid
flowchart LR
    subgraph wall["Wall clock (UTC) · *_at columns"]
        w0["10:00:00"] --> w1["10:00:01"] --> w2["10:01:00"]
    end
    subgraph sim["Sim clock (NYC local) · *_ts columns · 60×"]
        s0["Tue 07:00:00"] --> s1["Tue 07:01:00"] --> s2["Tue 08:00:00"]
    end
    w0 -.same instant.- s0
    w1 -.same instant.- s1
    w2 -.same instant.- s2
```

- Business questions ("today", "last hour", "rush hour") use **sim** time via `taxi.sim_now`.
- Pipeline latency ("how long did my ride take to get stored?") uses **wall** time only.

## 12. Stream processor dataflow

```mermaid
flowchart LR
    in[(trips.raw)] --> ann["annotate_reasons<br/>11 rules"]
    ann -->|"no reasons"| enr["enrich<br/>zones + derived fields"]
    ann -->|"1+ reasons"| rej["to_rejected"]
    rej --> rejt[(trips.rejected)]
    rej --> rejs[["sink → taxi.trips_rejected"]]
    enr --> enrt[(trips.enriched)]
    enr --> ts[["sink → taxi.trips"]]
    enrt --> gb["group_by pu_borough"] --> win["tumbling window 1 min<br/>grace 180 s (sim)"] --> red["reduce → final"]
    red --> aggt[(agg.borough_minute)]
    red --> aggs[["sink → taxi.agg_borough_minute"]]
    ann -.->|"traced: validated / rejected"| tr[(trace.checkpoints)]
    enr -.->|"traced: enriched + aggregated"| tr
    ts -.->|"traced: stored"| tr

    classDef core fill:#3c3489,stroke:#534ab7,color:#eeedfe
    class ann,enr,rej,gb,win,red,ts,rejs,aggs core
```
