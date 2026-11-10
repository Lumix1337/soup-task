"""Requirement 5: Load Benchmark — 50 Concurrent Runs at 10 Events/Second.

Evaluates system performance under two real-world profiles for 50 concurrent runs
generating 10 events/sec each (500 events/sec total), with active live SSE
subscribers attached:
- Profile A (Batched 1s flush): 50 runs × 1 batch/sec (10 events/batch) = 50 HTTP POST/s, 500 events/s.
- Profile B (Unbatched per-event): 50 runs × 10 requests/sec (1 event/batch) = 500 HTTP POST/s, 500 events/s.

Measures:
- Ingestion latency distribution (`p50`, `p95`, `p99`, `max` in ms)
- End-to-end SSE live delivery latency (`p50`, `p95`, `p99` in ms)
- Error rate, dropped slow clients, and database row completeness (expected vs. stored).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import statistics
import sys
import time
from typing import Dict, List

import httpx

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from tests.conftest import running_test_server  # noqa: E402


def _percentile(sorted_vals: List[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, int(round((pct / 100.0) * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


async def run_profile(
    name: str,
    num_runs: int,
    events_per_sec_per_run: int,
    batch_size: int,
    duration_secs: int,
    num_sse_listeners: int = 10,
) -> Dict[str, object]:
    batches_per_sec_per_run = events_per_sec_per_run // batch_size
    total_batches_per_run = batches_per_sec_per_run * duration_secs
    expected_events_per_run = events_per_sec_per_run * duration_secs
    expected_total_events = num_runs * expected_events_per_run
    interval_sec = 1.0 / batches_per_sec_per_run

    async with running_test_server(pool_size=32) as server:
        limits = httpx.Limits(max_connections=200, max_keepalive_connections=100)
        async with httpx.AsyncClient(
            base_url=server.base_url, timeout=15.0, limits=limits
        ) as client:
            proj = (await client.post("/projects", json={"name": f"bench-{name}"})).json()
            headers = {"X-API-Key": proj["api_key"]}

            run_ids: List[str] = []
            for i in range(num_runs):
                r = await client.post("/runs", json={"name": f"run-{i:02d}"}, headers=headers)
                run_ids.append(r.json()["run_id"])

            sse_latencies_ms: List[float] = []
            sse_received_counts: Dict[str, int] = {rid: 0 for rid in run_ids[:num_sse_listeners]}
            stop_sse = asyncio.Event()

            async def sse_worker(rid: str) -> None:
                async with client.stream(
                    "GET", f"/runs/{rid}/stream?queue_size=512", headers=headers
                ) as resp:
                    buf = ""
                    async for chunk in resp.aiter_text():
                        buf += chunk
                        while "\n\n" in buf:
                            block, buf = buf.split("\n\n", 1)
                            if "event: metric" in block:
                                for ln in block.splitlines():
                                    if ln.startswith("data:"):
                                        payload = json.loads(ln[5:].strip())
                                        sent_t = payload.get("custom_metrics", {}).get("sent_mono")
                                        if isinstance(sent_t, (int, float)):
                                            sse_latencies_ms.append(
                                                (time.perf_counter() - float(sent_t)) * 1000.0
                                            )
                                        sse_received_counts[rid] += 1
                        if sse_received_counts[rid] >= expected_events_per_run or stop_sse.is_set():
                            break

            sse_tasks = [
                asyncio.create_task(sse_worker(rid)) for rid in run_ids[:num_sse_listeners]
            ]
            await asyncio.sleep(0.15)

            post_latencies_ms: List[float] = []
            errors = 0

            async def producer_worker(run_idx: int, rid: str) -> None:
                nonlocal errors
                step_counter = 0
                start_mono = time.perf_counter()
                for b_idx in range(total_batches_per_run):
                    target_time = start_mono + b_idx * interval_sec
                    now = time.perf_counter()
                    if target_time > now:
                        await asyncio.sleep(target_time - now)

                    sent_mono = time.perf_counter()
                    metrics_batch = []
                    for _ in range(batch_size):
                        metrics_batch.append(
                            {
                                "step": step_counter,
                                "loss": round(2.5 / (1.0 + 0.01 * step_counter), 6),
                                "lr": 1e-4,
                                "grad_norm": 0.42,
                                "custom_metrics": {"sent_mono": sent_mono},
                            }
                        )
                        step_counter += 1

                    t0 = time.perf_counter()
                    try:
                        resp = await client.post(
                            f"/runs/{rid}/metrics",
                            json={"batch_id": f"r{run_idx}-b{b_idx}", "metrics": metrics_batch},
                            headers=headers,
                        )
                        t1 = time.perf_counter()
                        post_latencies_ms.append((t1 - t0) * 1000.0)
                        if resp.status_code != 200:
                            errors += 1
                    except Exception:
                        errors += 1

            wall_t0 = time.perf_counter()
            await asyncio.gather(
                *[producer_worker(idx, rid) for idx, rid in enumerate(run_ids)]
            )
            wall_elapsed = time.perf_counter() - wall_t0

            # Allow SSE listeners up to 2s to drain final events
            try:
                await asyncio.wait_for(asyncio.gather(*sse_tasks), timeout=2.0)
            except asyncio.TimeoutError:
                stop_sse.set()
                for t in sse_tasks:
                    t.cancel()

            # Verify stored DB count
            db_count_row = await server.db.fetch_one("SELECT COUNT(*) AS cnt FROM metrics")
            stored_events = int(db_count_row["cnt"]) if db_count_row else 0
            stats = (await client.get("/_test/stats")).json()

            s_post = sorted(post_latencies_ms)
            s_sse = sorted(sse_latencies_ms)

            return {
                "profile": name,
                "num_runs": num_runs,
                "events_per_sec_target": num_runs * events_per_sec_per_run,
                "batch_size": batch_size,
                "duration_secs": duration_secs,
                "wall_elapsed_secs": round(wall_elapsed, 3),
                "achieved_events_per_sec": round(stored_events / wall_elapsed, 1),
                "achieved_http_rps": round(len(post_latencies_ms) / wall_elapsed, 1),
                "expected_events": expected_total_events,
                "stored_events": stored_events,
                "http_errors": errors,
                "overflow_drops": stats["overflow_drops"],
                "post_p50_ms": round(_percentile(s_post, 50), 2),
                "post_p95_ms": round(_percentile(s_post, 95), 2),
                "post_p99_ms": round(_percentile(s_post, 99), 2),
                "post_max_ms": round(s_post[-1] if s_post else 0.0, 2),
                "sse_p50_ms": round(_percentile(s_sse, 50), 2),
                "sse_p95_ms": round(_percentile(s_sse, 95), 2),
                "sse_p99_ms": round(_percentile(s_sse, 99), 2),
                "sse_delivered_samples": len(s_sse),
            }


async def main() -> None:
    print("======================================================================")
    print("SOUP EXPERIMENT TRACKER — LOAD BENCHMARK (50 CONCURRENT RUNS @ 10 EPS)")
    print("======================================================================")

    res_batched = await run_profile(
        name="Profile A: Batched (10 events/batch, 1 batch/s per run -> 500 eps / 50 RPS)",
        num_runs=50,
        events_per_sec_per_run=10,
        batch_size=10,
        duration_secs=6,
        num_sse_listeners=10,
    )

    res_unbatched = await run_profile(
        name="Profile B: Unbatched (1 event/batch, 10 batches/s per run -> 500 eps / 500 RPS)",
        num_runs=50,
        events_per_sec_per_run=10,
        batch_size=1,
        duration_secs=6,
        num_sse_listeners=10,
    )

    for r in (res_batched, res_unbatched):
        print(f"\n--- {r['profile']} ---")
        print(f"Target Load            : {r['num_runs']} runs × 10 eps = {r['events_per_sec_target']} events/sec")
        print(f"Wall Clock Duration    : {r['wall_elapsed_secs']} s (configured {r['duration_secs']} s)")
        print(f"Achieved Throughput    : {r['achieved_events_per_sec']} events/sec ({r['achieved_http_rps']} HTTP POST/sec)")
        print(f"Completeness (DB Rows) : {r['stored_events']} / {r['expected_events']} stored (HTTP errors: {r['http_errors']})")
        print(f"Slow Client Overflows  : {r['overflow_drops']}")
        print(
            f"Ingestion HTTP Latency : p50 = {r['post_p50_ms']} ms | p95 = {r['post_p95_ms']} ms | "
            f"p99 = {r['post_p99_ms']} ms | max = {r['post_max_ms']} ms"
        )
        print(
            f"Live SSE Delivery Lag  : p50 = {r['sse_p50_ms']} ms | p95 = {r['sse_p95_ms']} ms | "
            f"p99 = {r['sse_p99_ms']} ms ({r['sse_delivered_samples']} live events sampled)"
        )


if __name__ == "__main__":
    asyncio.run(main())
