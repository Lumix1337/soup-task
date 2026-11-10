# Soup Take-Home Note: Mini Experiment Tracker

**Core Questions:**
1. *Does what the user sees always match what is stored?* — **Yes**, proven via per-run gapless `seq` catch-up (`Last-Event-ID`) and `step`-ordered client state (`results/consistency_raw.txt`).
2. *Do they see only what's theirs?* — **Yes**, proven across REST, live SSE, cache headers, logs, error payloads, and timing measurements (`results/isolation_raw.txt`).

---

## 1. Architecture Design Summary (Full 1-Page Spec in [`docs/DESIGN.md`](docs/DESIGN.md))

- **Event Format & Ordering (`step` vs. `seq`):** Each metric has two integers: `step` (`PRIMARY KEY (run_id, step)`, logical training step used for UI sorting) and `seq` (`UNIQUE (run_id, seq)`, arrival/delivery sequence allocated from `runs.last_seq` under `SELECT ... FROM runs WHERE id = $1 AND project_id = $2 FOR UPDATE`). Decoupling `seq` from `step` ensures that if `step=10` arrives before a disconnect (`seq=1`) and delayed `step=5` arrives during the disconnect (`seq=2`), reconnecting with `Last-Event-ID: 1` fetches `seq > 1` and delivers `step=5`.
- **Duplicate Batches & Atomicity:** All points in a batch are validated pre-write (`422` on any invalid point or intra-batch step conflict). `(run_id, batch_id, payload_hash)` in `ingested_batches` makes exact batch retries idempotent (`200 OK`, `inserted: 0`), while conflicting payloads return `409 Conflict`. Duplicate `(run_id, step)` rows with identical values do **not** increment `runs.last_seq`, keeping `seq = 1..N` strictly gapless.
- **Disconnect Catch-Up:** `GET /runs/{id}/stream` registers the subscriber's bounded `asyncio.Queue` **before** querying PostgreSQL for `seq > Last-Event-ID`. After streaming DB backlog rows up to `high_water_seq`, the handler drains the live queue and discards any event with `seq <= high_water_seq` (zero gap, zero boundary duplicate).
- **Project Isolation:** External IDs are `UUIDv4`. API keys (`soup_sk_...`) are stored as `SHA-256` hashes. Query-string keys (`?api_key=`) are rejected with `400 Bad Request`. Cross-project and non-existent run probes execute the same parameterized index lookup and return byte-identical `404 {"detail": "Run not found"}` with `Cache-Control: private, no-store`.

---

## 2. Consistency Proof & What the Test Would NOT Catch

**Automated Test (`tests/test_consistency_e2e.py`, raw output in [`results/consistency_raw.txt`](results/consistency_raw.txt)):**
- Launches PostgreSQL, Uvicorn, and the real frontend script (`app/static/app.js`) in a Node.js DOM harness connected via live SSE.
- Sends steps `[0,1,2]` (`seq 1..3`), out-of-order future steps `[6,7,8]` (`seq 4..6`), and a duplicate retry of `[0,1,2]`.
- Forcibly drops the SSE connection mid-stream (`POST /_test/disconnect/{run_id}`).
- While offline, ingests **late steps `[3,4,5]`** (`seq 7..9`) and overlapping steps `[4,5,9,10]` (`seq 10..11`), then waits for automatic reconnect (`Last-Event-ID: 6`) and sends live steps `[11,12]` (`seq 12..13`).
- **Result:** Final UI state (`window.__trackerState.orderedMetrics` and rendered DOM `<tr>` rows) matches `SELECT step, seq, loss, lr, grad_norm FROM metrics ORDER BY step ASC` across all 13 steps (`0..12`) with `reconnectCount = 1` and `seqGapsDetected = 0`.

**What This Test Would NOT Catch:**
1. **Multi-worker pub/sub partitioning:** `PubSubBroker` is in-memory per process. With `--workers > 1` behind a load balancer without Redis/Postgres `LISTEN/NOTIFY`, a batch ingested on Worker A will not push live SSE events to a client connected to Worker B until that client reconnects.
2. **Browser memory/DOM degradation on long runs:** Storing 500,000+ steps in `window.__trackerState` and re-rendering full `<tbody>` HTML on every frame would cause browser tab OOM/layout thrashing without downsampling (LTTB) and virtualized tables.
3. **Storage-engine crash before `fsync`:** If the host kernel panics or `synchronous_commit=off` is used, an acknowledged HTTP 200 could be lost on disk recovery while already rendered in the UI.
4. **IEEE-754 float serialization drift:** Extreme subnormal floats or `1e-13` rounding differences across architectures could trigger false `409 Conflict` on retries if clients re-serialize floats differently.
5. **CSS/layout visual regressions:** Our Node DOM harness verifies exact `<tr>` and `<svg><polyline>` markup generation, but does not rasterize pixels to catch overlapping CSS elements.

---

## 3. Isolation Proof & What Else Could Leak

**Automated Test (`tests/test_isolation.py`, raw output in [`results/isolation_raw.txt`](results/isolation_raw.txt)):**
- Proves Project A cannot read (`GET /runs`, `/runs/{B}`, `/runs/{B}/metrics`), write (`POST /runs/{B}/metrics`), rotate keys (`POST /projects/{B}/keys/rotate`), or subscribe (`GET /runs/{B}/stream`) to Project B (`404 Not Found`), nor poison caches (`Cache-Control: private, no-store`, `Vary: X-API-Key, Authorization`).

**What Else Could Leak (and How We Checked):**
- **Logs:** Tested that `?api_key=` is blocked with `400` (preventing plaintext key logging in access logs) and verified via `/_test/stats` that `_RedactingLogFilter` strips `soup_sk_...` and `Bearer` tokens (`0` raw keys in logs).
- **Error Messages:** Verified that querying Project B's existing run ID, a random UUIDv4, or a malformed string (`not-a-uuid-12345`) returns byte-for-byte identical status (`404`), headers, and body (`{"detail": "Run not found"}`).
- **Timing Side Channels:** Measured 100 probes from Project A to Project B's existing run vs. 100 probes to random UUIDs (`results/isolation_raw.txt`): existing foreign UUID `p50 = 6.440 ms` vs. non-existent UUID `p50 = 6.245 ms` (median delta `0.195 ms`, well within loopback jitter). *Remaining leak vector:* on `POST /runs/{id}/metrics`, if Project B is actively writing heavy batches and holding the `FOR UPDATE` row lock on `run_B`, a concurrent write probe by Project A would never block because `WHERE id = $1 AND project_id = $2` matches 0 rows, **but** global DB connection pool exhaustion by Project B could still be observed by Project A as elevated latency (noisy-neighbor timing leak).

---

## 4. Silent Failures Verification ([`results/silent_failures_raw.txt`](results/silent_failures_raw.txt))

| Silent Failure Mode | Prevention Mechanism | How We Checked (`tests/test_silent_failures.py`) |
| :--- | :--- | :--- |
| **1. Events lost on reconnect** | Catch-up queries `seq > Last-Event-ID` (arrival order) after subscribing to live queue; UI sorts by `step`. | Ingested steps `10..12` (`seq 1..3`), disconnected SSE, ingested late steps `1..3` + new `13..14` (`seq 4..8`), reconnected with `Last-Event-ID: 3` -> verified `seq 1..8` received with 0 losses. |
| **2. Duplicates** | `(run_id, batch_id, payload_hash)` table + `PRIMARY KEY (run_id, step)` under `FOR UPDATE` run lock. | Fired 5 concurrent identical batch POSTs (`asyncio.gather`) + overlapping batch -> verified exactly 4 unique rows in DB and gapless `seq 1..4`. |
| **3. Slow client queue overflow** | Bounded `asyncio.Queue(maxsize=64)` + `put_nowait`. On `QueueFull`, clears unsent queue, sends `event: overflow`, closes stream. | Connected client with `queue_size=3`, paused reading while blasting 9 metrics -> verified `event: overflow` received, `overflow_drops >= 1`, and reconnect recovered `seq 1..10`. |
| **4. Partial writes** | Pre-write batch validation (`422`) + single DB transaction (`ROLLBACK` on any `409` step conflict). | Sent batch with 4 valid + 1 invalid point (`step=-1` -> `422`) and batch with 3 new + 1 conflicting step (`409`) -> verified `0` orphan rows written. |
| **5. Old API key after rotation** | `revoked_at = NOW()` in DB + `PubSubBroker.revoke_key()` pushes `"REVOKED"` to active queues. | Opened SSE stream with old key, rotated key -> verified REST returns `401`, open SSE stream emits `api_key_revoked` and receives `0` post-rotation metrics. |

---

## 5. Verdict: 50 Concurrent Runs at 10 Events/Second ([`results/benchmark_raw.txt`](results/benchmark_raw.txt))

We benchmarked two traffic shapes for **50 concurrent runs × 10 events/sec = 500 events/sec** with 10 live SSE listeners:
- **Profile A (Client batches 10 events / 1s flush = 50 HTTP POST/s, 500 events/s):** Achieved **555.2 events/sec** (`55.5 HTTP POST/sec` in `5.404s`), `3000/3000` stored, `0` errors, `0` overflows (Ingestion: `p50 = 295.48 ms`, `p95 = 573.34 ms`, `p99 = 700.78 ms`; Live SSE lag: `p50 = 247.21 ms`, `p95 = 583.5 ms`).
- **Profile B (Unbatched 1 event per HTTP POST = 500 HTTP POST/s, 500 events/s):** Achieved **125.0 events/sec** (`125.0 HTTP POST/sec`, taking `24.009s` to drain 3,000 individual commits; `3000/3000` stored, `0` errors, but `p50 = 242.45 ms`, `p95 = 1181.38 ms`, `p99 = 1806.06 ms`).

**Verdict:**
- **SHIP** if the SDK/client batches metrics (e.g., 10 events flushed once per second = 50 RPS).
- **DON'T SHIP** if 50 clients send every single event as an unbatched HTTP POST (500 separate ACID transactions/sec with synchronous disk `fsync` per request).
- **What to change first:** (1) Switch from synchronous `libpq`-in-threadpool to native async `psycopg`/`asyncpg` with server-side group-commit coalescing (or `COPY` / `unnest()` bulk inserts), and (2) replace the single-process in-memory `PubSubBroker` with Redis Streams or PostgreSQL `LISTEN/NOTIFY` so multiple Uvicorn workers can scale horizontally.

---

## 6. Live Discussion Questions (Written Answers)

1. **A client was offline for 30 seconds: what does it see and why?**
   During the 30s outage, the browser's `fetch` stream reader rejects or times out, sets the status badge to `Reconnecting...`, and retries `/runs/{id}/stream` with header `Last-Event-ID: <lastSeq>`. Upon reconnection, the server subscribes the client to the live queue, queries PostgreSQL for all rows `WHERE run_id = $1 AND seq > $2 ORDER BY seq ASC` (the ~300 events written during those 30s, including any out-of-order steps), streams them immediately, and seamlessly resumes live delivery. The UI deduplicates by `step`, sorts `orderedMetrics` by `step ASC`, and updates the chart/table to match the DB without page reload.
2. **Where is your design weakest?**
   Single-node coupling in two places: (a) `PubSubBroker` lives in process memory, so running multiple API replicas without a shared pub/sub bus would cause SSE subscribers on Replica B to miss live pushes ingested on Replica A; and (b) every HTTP POST executes an independent PostgreSQL transaction (`BEGIN ... SELECT FOR UPDATE ... INSERT ... COMMIT`), making unbatched 1-event requests bottleneck on per-transaction commit overhead (~130 TPS ceiling on a single node).
3. **How would you know in production that an event was lost?**
   At two levels: (a) **Server -> UI delivery loss:** because `runs.last_seq` allocates a strictly contiguous sequence `1, 2, ..., N` per run (duplicates never increment `last_seq`), the client checks `metric.seq > state.lastSeq + 1` on every SSE frame (`state.seqGapsDetected`) and can immediately alert or trigger a resync. (b) **Client SDK -> Server ingestion loss:** each client SDK should attach a monotonic `client_batch_seq = 1..M` per run; if the server sees `client_batch_seq` jump from `4` to `6`, it increments a Prometheus counter `ingestion_batch_gaps_total`.

---

## 7. What Surprised Us & What Still Concerns Us

What surprised us most was a subtle bug we caught while testing slow-client queue overflow (`tests/test_silent_failures.py`): initially, when `asyncio.Queue(maxsize=3)` filled up, our signal helper popped one item via `queue.get_nowait()` to make room for the `"OVERFLOW"` sentinel. Because `asyncio.Queue` is FIFO, `get_nowait()` dropped the *oldest* unsent event (`seq=2`) at the head of the queue while leaving `seq=3, 4` ahead of `"OVERFLOW"`—causing the SSE stream to deliver `seq=3, 4`, advance the client's `Last-Event-ID` to `4`, and permanently lose `seq=2` on reconnect! Clearing all unsent queue items before pushing `"OVERFLOW"` fixed this completely. We were also surprised by how stark the gap was between batched ingestion (550+ events/sec in 5.4s) and unbatched 1-event HTTP requests (130 events/sec in 22s) due to per-transaction commit overhead. What still concerns us is single-process memory/CPU isolation under multi-tenant load: while data isolation is strict, one noisy tenant blasting 500 unbatched requests/sec can saturate the shared database connection pool and increase tail latency (`p99`) for other tenants unless per-project rate limiting and connection quotas are enforced.

---

## 8. AI Tools Usage Summary (Full Log in [`docs/AI_LOG.md`](docs/AI_LOG.md))

AI was used to scaffold the PostgreSQL schema, ASGI routing boilerplate, and SVG chart rendering. We **accepted** the per-run `SELECT ... FOR UPDATE` sequence allocation and subscribe-before-query SSE handoff. We **rejected and rewrote** four flawed AI suggestions: (1) catching up by `step > last_step` (which loses late out-of-order steps), (2) passing `?api_key=` in SSE URLs (which leaks secrets into access logs), (3) using PostgreSQL `SERIAL` with `ON CONFLICT DO NOTHING` (which creates sequence gaps on duplicate retries), and (4) popping the head of a full FIFO queue on slow-client overflow (which skipped an undelivered `seq` before closing the stream).
