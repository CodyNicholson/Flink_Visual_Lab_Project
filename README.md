# Apache Flink Visual Lab

Interactive lab for stateful stream processing. A React UI triggers real actions in Kafka,
PostgreSQL (logical-replication WAL CDC) and the local file system. An embedded Flink job inside
the Spring Boot backend processes them, and each operator streams `FlinkTraceEvent` telemetry
over SSE so the UI can animate the DAG.

## Run

```bash
docker compose up --build
```

| URL | What |
| --- | --- |
| http://localhost:3000 | React visualizer (minified build served by nginx, which proxies `/api`) |
| http://localhost:8082 (also `/flink-job/` on the frontend) | Flink dashboard of the lab job (embedded MiniCluster, read-only) |
| http://localhost:8081 (also `/flink-cluster/` on the frontend) | Standalone Flink cluster UI (idle; for exploring the UI, read-only) |
| http://localhost:8080/api/flink/state | Backend state endpoint |

If a host port is already taken, override it, for example:

```bash
FRONTEND_HOST_PORT=3100 BACKEND_HOST_PORT=8090 KAFKA_HOST_PORT=9192 docker compose up --build
```

For hot reload, run `npm run dev` in `frontend/` (Vite proxies `/api` to `http://localhost:8080`).

### Deploying through the Cloudflare tunnel

`./pipeline-run.sh` pulls the latest code, removes **all** Docker containers on the host, starts
the Cloudflare tunnel (needs `TUNNEL_TOKEN` set globally, e.g. in `/etc/environment`), and runs
`docker compose up -d --build`. The frontend container is named `rixus-frontend` on the
`tunnel-net` network to match the tunnel's route. The frontend's nginx also serves both Flink
dashboards at `/flink-job/` and `/flink-cluster/`, so they work through the tunnel. Job submit and
cancel are disabled in both dashboards because they're publicly reachable.

Other overrides: `POSTGRES_HOST_PORT`, `FLINK_CLUSTER_HOST_PORT`, `FLINK_EMBEDDED_UI_HOST_PORT`.

## Scenarios

1. **Kafka event**: `KafkaSource` → event-time watermarks → `keyBy(customerId)` → `ValueState` running total → sinks.
2. **Postgres CDC**: SQL `UPDATE` → WAL read through the `flink_cdc_lab_slot` replication slot (`test_decoding`) → `-U`/`+U` changelog → the customer tier in keyed state is updated.
3. **File drop**: a `.jsonl` file is written to `/data/flink-fs/inbox` → `FileSource.monitorContinuously` → the output is appended to `/data/flink-fs/outbox/flink-processed-sink.jsonl`.
4. **Stateful join + JDBC sink**: the order is enriched with the live CDC tier and upserted into `enriched_orders_sink`.
5. **Late event**: an event backdated by 60s lands behind the watermark and is tagged `LATE_OUT_OF_ORDER`. Checkpoints (every 10s, stored in `/data/flink-fs/checkpoints`) flash the checkpoint badge.

## Notes

- The Flink job runs embedded in the backend so its operators can push telemetry to SSE clients
  in the same JVM. The standalone JobManager/TaskManager containers do not run it.
- Watermarks are bounded-out-of-orderness (2s) with a floor that trails the wall clock by 10s.
  Without the floor, an idle input (for example CDC) would hold the joined operator's watermark at
  `Long.MIN_VALUE` and nothing would ever be late.
- Files in the inbox are re-read when the job restarts, which adds them to the running totals again.
- Reset everything: `docker compose down -v`. This drops the shared volume. Postgres has no
  volume, so it re-initializes on the next start.
