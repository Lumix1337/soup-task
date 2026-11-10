# Architecture Design: Mini Experiment Tracker (Soup Take-Home)

This document defines the data model, ordering invariants, idempotency rules, live stream catch-up protocol, and multi-tenant isolation boundaries designed **before** implementation.

---

## 1. Event Format and Ordering (`step` vs. `seq`)

Clients send metric batches via `POST /runs/{run_id}/metrics` over an unreliable network:

```json
{
  "batch_id": "b-0192f3a0",
  "metrics": [
    {"step": 10, "loss": 1.421, "lr": 0.0001, "grad_norm": 0.83, "client_ts": "2026-11-10T10:00:00Z"}
  ]
}
```

We decouple **domain ordering** from **arrival/delivery ordering** using two distinct integers per metric row:
- **`step` (`INTEGER >= 0`)**: The logical training step (`PRIMARY KEY (run_id, step)`). Used to sort metrics for display, charts, and final state comparison (`ORDER BY step ASC`).
- **`seq` (`BIGINT >= 1`)**: A strictly monotonic, gapless per-run sequence number (`UNIQUE (run_id, seq)`), incremented on the parent `runs.last_seq` counter inside a row-locked transaction (`SELECT last_seq FROM runs WHERE id = :run_id AND project_id = :project_id FOR UPDATE`).

**Why two numbers?** If `step=10` arrives first (`seq=1`) and `step=5` arrives late (`seq=2`) while a browser client is disconnected at `Last-Event-ID: 1`, catching up by `step > 10` would permanently lose `step=5`. Catching up by `seq > 1` guarantees delivery of `seq=2 (step=5)`, which the client then inserts in sorted `step` order. Locking only the target `runs` row serializes writes within a single run (preventing PostgreSQL MVCC out-of-order commit visibility gaps) while allowing 50+ concurrent runs to ingest in parallel with zero cross-run lock contention.

---

## 2. Duplicate Batches and Atomic Ingestion

Networks retry timed-out requests, and clients may resend overlapping steps. Ingestion is **all-or-nothing** per batch:
1. **Pre-write Validation:** Every point in `metrics` is validated (`step >= 0`, finite `loss`/`lr`/`grad_norm`, no conflicting values for the same `step` within the batch). Any invalid point rejects the entire batch with `422 Unprocessable Entity` before touching the database.
2. **Batch-Level Idempotency (`ingested_batches`):** We store `(run_id, batch_id, payload_hash)`.
   - Re-sending the same `(run_id, batch_id)` with an identical canonical SHA-256 `payload_hash` is a **no-op** (`200 OK`, `inserted: 0`).
   - Re-sending the same `batch_id` with a *different* payload returns `409 Conflict` and rolls back.
3. **Point-Level Idempotency (`PRIMARY KEY (run_id, step)`):**
   - If `(run_id, step)` already exists with identical metric values (e.g., overlapping retry batch), it is skipped (`duplicates += 1`), and **`runs.last_seq` is NOT incremented** (preserving gapless `seq = 1..N`).
   - If `(run_id, step)` exists with *different* metric values, the transaction rolls back with `409 Conflict` to prevent silent data corruption.

---

## 3. Catch-Up After Disconnect (`Last-Event-ID` + Race-Free Handoff)

Live updates stream over `GET /runs/{run_id}/stream` (Server-Sent Events) using `id: <seq>`:
1. **Subscribe Before Query:** When a client connects (or reconnects with header `Last-Event-ID: <last_seq>`), the server **first** registers a bounded in-memory subscriber queue `asyncio.Queue(maxsize=64)` on `run_id`, and **second** queries the database for `WHERE run_id = :id AND seq > :last_seq ORDER BY seq ASC`.
2. **Deduplicated Handoff:** The server streams all rows from the database query, recording `high_water_seq`. When switching to the live `asyncio.Queue`, any queued event with `event.seq <= high_water_seq` is discarded; events with `event.seq > high_water_seq` are streamed and advance `high_water_seq`. This eliminates both the "missed event gap" and "duplicate boundary event" between DB snapshot and live pub/sub.
3. **Slow Client Backpressure:** Publishing to subscriber queues uses non-blocking `put_nowait`. If a slow client fills its bounded queue (`QueueFull`), the server emits `event: overflow` and closes that stream without blocking ingestion or growing memory. The client automatically reconnects with `Last-Event-ID` and catches up from PostgreSQL.

---

## 4. Project Isolation and Security Boundaries

1. **Credentials & Hashing:** API keys are generated with 256 bits of entropy (`secrets.token_urlsafe(32)`). Only `SHA-256(raw_key)` is stored in `api_keys(key_hash, project_id, revoked_at)`. Passing `?api_key=` in URL query parameters is rejected with `400 Bad Request` so secrets never leak into HTTP access logs.
2. **Browser SSE Auth:** Native `EventSource` cannot send custom headers; the UI streams SSE via `fetch('/runs/{id}/stream', { headers: { 'X-API-Key': key, 'Last-Event-ID': seq } })` + `ReadableStream`.
3. **Tenant Scoping & Indistinguishable 404s:** Every run/metric query filters by `WHERE id = :run_id AND project_id = :auth_project_id`. All external IDs are random `UUIDv4`. Accessing another project's run or a non-existent UUID executes the same indexed lookup and returns an identical `404 {"detail": "Run not found"}` with `Cache-Control: private, no-store` and `Vary: X-API-Key, Authorization`.
4. **Immediate Key Rotation:** `POST /projects/{id}/keys/rotate` atomically sets `revoked_at = NOW()` on the old key, issues a new key, and broadcasts a `"REVOKED"` signal to the SSE broker, immediately terminating any already-open SSE streams authenticated with the old key.
