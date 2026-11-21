# Flink Visual Lab design direction

Written in the design pass (step 5b of the visual-lab skill). Every lab has its own art direction.

## Subject and audience
- Apache Flink: a stateful stream processor. Its world is event time, watermarks trailing behind
  the newest event, windows, keyed state, checkpoint barriers flowing through an operator DAG,
  and the squirrel mark in warm pinks and ambers.
- Visitors are developers who know batch jobs and want to see a stream processor keep state,
  handle late events and survive with checkpoints, fed from Kafka, Postgres CDC and files.
- The UI's job: one live operator DAG where every record and every checkpoint is visible.

## Candidates considered
- `ui-ux-pro-max` (`"Apache Flink stateful stream processing CDC developer learning lab"
  --design-system`) suggested "Swiss Modernism 2.0": navy #1E3A5F, gold #A16207, EB Garamond.
- Rejected: the same academic result came back for every tool queried; nothing in it is about
  streams.
- Kept: strict grid, 4.5:1 contrast, visible focus, reduced motion.
- The old look (slate #0b1120 page, sky-blue title, rainbow buttons including violets, glowing
  halos, Inter) was the generic dark dashboard.
- Dark was kept from the use scene: the lab sits next to Flink's own dark job dashboard, which
  it links to.

## Direction (one sentence)
"Squirrel at dusk": a warm walnut ground marked with faint event-time window boundaries, the
Flink squirrel's pink for the live path and the watermark operator, acorn amber for checkpoints,
one colour per connector, and Recursive for everything, with its casual axis on the wordmark
and its mono axis for code.

## Tokens
| Role | Value | Notes |
|---|---|---|
| Background `--bg` | #221a16 | walnut; window lines #2a211c every 96px, a window edge #362a23 every 384px |
| Panels `--panel`, `--panel-2` | #2c221d, #362a24 | DAG and header on `--panel-deep` #1a1310; `--border` #4a3a31 |
| Text `--text`, `--muted` | #f3ebe4, #a8988b | muted 6.1:1 on `--bg`, 5.0:1 on `--panel-2` |
| Accent `--accent` | #ff6f8e | the squirrel's pink: live connectors, watermark operator, focus ring (6.5:1 on `--bg`) |
| Node roles | source #7fb2ff, cdc #ffcf70, file #d8ad84, state #5cc8b8, sink #8fd18a, checkpoint #f2a65a, late #ff7d7d | one per connector or operator; all at least 5:1 on panels |
| Status ok / bad / warn | sink green / late red / checkpoint amber | every status is also spelled out (LATE_OUT_OF_ORDER, ON_TIME_WATERMARK, "live") |

## Type
- One family: Recursive Variable (`@fontsource-variable/recursive@5.3.0`, `full.css` for the
  MONO and CASL axes).
- Display: the wordmark at weight 820, CASL 1 (casual), tracking -0.02em: the squirrel's
  playfulness, used once.
- Body: Recursive sans (MONO 0, CASL 0), 1rem / 1.55.
- Mono: the same family at MONO 1 for code, stage names, connector labels and tier badges, so
  data and prose share one voice.
- Scale: h1 1.6rem (1.3rem on phones), panel titles 0.95–1.1rem, body 0.8–0.96rem.
- Cost: the full-axis Latin file is 305 KB (one more for Latin Extended when needed); accepted
  for one family doing three jobs.

## Background and surfaces
- `--page-bg`: two vertical hairline gradients, tumbling windows on a time axis, fixed while the
  page scrolls.
- Radii 5–10px. No glows: an operator that is processing gets its role colour as border and a
  12% tint.
- The header is the one bold element through type alone: the casual heavy wordmark.

## Motion personality
- The live path is the one moving thing: an active edge between operators becomes pink dashes
  travelling in the direction of the data (downward on phones, where the DAG stacks).
- The checkpoint badge turns amber for 1.2 s when a barrier completes. No GSAP: every motion
  answers a live SSE event.
- `prefers-reduced-motion` stops every animation and transition.

## Generic defaults avoided
- Dark slate dashboard with sky-blue title and rainbow buttons (violets included): walnut, pink
  and per-connector tints.
- Coloured left bars on telemetry rows and the lesson card: replaced with a stage dot and full
  borders.
- Glowing halos on active operators: border plus tint.
- All-caps tracked column labels and Title Case headings: sentence case, same facts.
- Inter: replaced with Recursive.

## Audit log
- 2026-11-21 `impeccable detect` (regex scan of `src`): 0 findings after the rewrite (the old CSS
  had left-bar telemetry rows, a left-bar lesson card and an all-caps label style). URL mode
  could not run: no Chromium on the host.
  - Craft floor: themed `::selection` and scrollbars; transitions list their properties.
- 2026-11-21 `web-design-guidelines` on `src/**/*.{jsx,css}`: all actions are `<button>`s,
  dashboards are real links, no clickable divs; `:focus-visible` ring added; the in-flight pill
  is `aria-live="polite"`; "..." replaced; tabular figures on the tables; `theme-color`,
  description and a favicon added (there was none).
- 2026-11-21 `screenshot.sh`: the stock script waits for network idle, which never happens while
  the SSE stream is open; ran a copy that waits for `load`: 2 screenshots (1440 and 390), 0
  problems. Then a scripted run that clicks all six scenarios against the real Flink job and
  shoots the live state: 0 console errors, 0 overflow at 390px. Looking at it: the source name
  WATERMARK_CHECKPOINT ran into the description in the telemetry log, and the sink table's last
  column was cut off at 1440px. Widened the source column (mono, wraps) and gave the sink table
  more of the row; shot again, both fixed.
- 2026-11-21 `test-isolated.sh`: security and design checks pass, `GET /` ok; `GET /api/health`
  fails because this backend has no health route and the lab has no demo catalog to smoke-test.
  The six real trigger endpoints were exercised by the scripted screenshot run instead (rows
  landed in the sink table, LATE_OUT_OF_ORDER flagged, checkpoints completing).
