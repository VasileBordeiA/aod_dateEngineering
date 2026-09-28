# Demo script: what to say at the stand

**Goal of a visit:** in 2–3 minutes, a student leaves understanding that *data engineering is what makes data (and AI) trustworthy*, and that it's fun. Talk with them, not at them. Ask a question within the first 20 seconds.

---

## The 30-second pitch (point at the poster / `/architecture`)
> "Everything on these screens is **live**. We're replaying real New York taxi trips from October 2025, sixty times faster. Every second, about a hundred trips come in.
>
> They flow into **Redpanda**, which is like a conveyor belt for events. A **stream processor** cleans them. It throws out the broken ones: negative fares, trips of zero miles. It adds the neighbourhood names and counts everything minute by minute. The results land in **ClickHouse**, a database built for fast analytics.
>
> On top of that there's this live dashboard, and an **AI you can ask questions**. The twist is that the AI shows you the exact SQL it wrote, so you can check it. Want to try?"

## The 2–3 minute visit
1. **(0:00) Hook.** "What do you study?" Then point at the dashboard: "That's Manhattan's rush hour, happening right now. Well, 60 times faster."
2. **(0:30) Ask the agent.** Hand them the keyboard, or say "tap one of these".
   - For first-timers, **"Which borough has the best tippers?"** is the best opener because of the card caveat.
   - While it runs, narrate the steps on screen: "See, it's first looking at the tables… now it's writing SQL…"
3. **(1:15) Read the SQL together.** "Look, it filtered `is_card`. Why? Because New York only records tips paid by card, and cash tips are invisible. **The AI only got this right because the data model told it.** That's data engineering."
4. **(1:45) Curious?** "Want to see your own data go through the pipeline? Scan this." → the ride form.
5. **(2:15) Watch the trace** on their phone and on `/trace/live`. "Your ride just got validated… enriched… it's in the database now. **1.8 seconds** from your phone to queryable."
6. **(2:45) Close.** "Now ask the AI where your tip ranks today." Hand out the careers flyer.

## Talking points per component
These texts are also the drawer copy on `/architecture` (spec 08). Keep them in sync.

**Dataset replayer**
- It replays **historical** data as if it were live, which is a common trick for testing pipelines.
- About 3.5–4.5 million trips a month, published in order at 60×.
- *Takeaway:* batch data and streaming data are the same data, arriving differently.

**Redpanda (Kafka-compatible broker)**
- A durable, ordered log of events. Producers and consumers don't know about each other.
- If the processor crashes, nothing is lost: it continues from where it stopped (its "offset").
- *Show:* Redpanda Console on the laptop, with a raw JSON message.
- *Takeaway:* decoupling is how real systems survive failure.

**Stream processor (Quix Streams, Python)**
- Three jobs: **clean** (11 data-quality rules), **enrich** (zone names, tip %), and **aggregate** (trips per borough per minute).
- About 2–5% of real NYC trips fail validation: refunds recorded as negative fares, meters left running, trips of 0 miles. We keep them in a separate table with the reason, and never silently delete them.
- *Takeaway:* garbage in, garbage out. Cleaning is most of the job.

**ClickHouse**
- A columnar analytics database. It scans millions of rows in milliseconds.
- Every column has a **comment** explaining what it means. The AI reads those comments.
- *Takeaway:* a good data model is documentation that machines can use.

**Live dashboard (Grafana)**
- It refreshes every 5 seconds. Everything shown is SQL on ClickHouse.
- Point at "Rejected by the pipeline": "This is data quality, measured live."

**AI data agent**
- An LLM with 4 tools: list tables, describe a table, run a **read-only** query, and draw a chart.
- It runs **locally on this laptop** (no cloud needed).
- Guardrails: a read-only database user, a 5-second timeout, a row limit, and a SQL checker.
- *Takeaway:* the AI is only as good as the data model underneath it.

**Trace your ride**
- Each stage reports a checkpoint. We measure how long every hop takes.
- Real companies do this with *distributed tracing* tools like OpenTelemetry. This is our simplified, visual version.

## Lines that land
- "The AI didn't know that cash tips are missing. **The data engineer who wrote the column comment did.**"
- "Your ride and a real New York taxi went through exactly the same code."
- "Nothing gets deleted. Broken data goes to the 'rejected' table with a reason, so someone can fix the source."
- "This whole thing runs on one laptop. At a company, it's the same tools, just bigger."

## When things go "wrong" (turn them into lessons)
| Situation | Say |
|---|---|
| The agent writes wrong SQL or gives an odd answer | "Perfect, let's debug it. Read the SQL: what did it assume? This is exactly why we show the SQL." |
| The agent self-repairs a failed query | "It got an error from the database, read it, and fixed its own query. Like we all do." |
| A student sets a $999 tip and gets rejected | "The pipeline caught you! That rule exists because real data has meter glitches like that." |
| Someone tries to break the AI ("ignore your instructions…") | "Go ahead, try! Even if it obeyed, its database user can only read, never delete." Keep it light |
| A student picks cash and the tip shows $0 | "Welcome to real data. Cash tips aren't recorded in New York, so any 'who tips best' study is biased." |
| The model is slow | "It's running on this laptop's GPU, no cloud. Real systems cache and batch." Press a suggestion |

## Likely student questions
**"What's the difference between data engineering and data science?"**
Data scientists ask questions of data. Data engineers make sure the data exists, is correct, arrives on time, and is modelled so people (and AIs) can use it. No pipeline, no data science.

**"Why not just use Excel or a normal database?"**
This month is about 4 million rows, and it keeps flowing. Excel stops at about 1 million rows and doesn't do "live". A normal database (Postgres) is great for transactions. ClickHouse is built to scan and aggregate millions of rows per second.

**"Why streaming instead of a daily batch?"**
Some decisions can't wait until tomorrow: fraud detection, surge pricing, "where are the taxis right now". Streaming gives you minutes or seconds instead of hours.

**"Is the AI always right?"**
No. That's why it shows its SQL and why the data model matters. On our test set it gets about X out of 32 questions right *(fill in from the latest eval report)*.

**"What should I learn?"**
SQL, really well. Python. One streaming tool (Kafka is the standard). One analytical database. And the fundamentals: data modelling, data quality, testing.

**"Can I see the code?"**
*(Decide before the event whether the repo will be public. If yes, put the URL on the flyer.)*

**"Do you have internships / jobs?"**
*(Fill in: company careers link, contact person, flyer.)*
