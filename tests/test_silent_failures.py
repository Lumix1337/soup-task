"""Requirement 4: Automated Verification of 5 Silent Failure Modes.

Tests and proves protection against:
1. Events lost on reconnect (including late/out-of-order steps arriving while offline).
2. Duplicates (concurrent batch retries, overlapping steps, conflicting payload detection).
3. Queue overflow for a slow client (bounded queue `maxsize`, non-blocking ingestion,
   explicit `event: overflow` signal, and 100% recovery via `Last-Event-ID`).
4. Partial writes (all-or-nothing batch validation and mid-transaction rollback on conflict).
5. Old API key keeping working after rotation (both subsequent REST requests and
   already-open live SSE streams).
"""

from __future__ import annotations

import asyncio
import json
from typing import List, Tuple
import unittest

import httpx

from tests.conftest import running_test_server


def _parse_sse_events(raw_text: str) -> List[Tuple[str, str | None, dict]]:
    events: List[Tuple[str, str | None, dict]] = []
    for block in raw_text.split("\n\n"):
        lines = [ln for ln in block.splitlines() if ln and not ln.startswith(":")]
        if not lines:
            continue
        ev_type = "message"
        ev_id = None
        data_parts = []
        for ln in lines:
            if ln.startswith("event:"):
                ev_type = ln[6:].strip()
            elif ln.startswith("id:"):
                ev_id = ln[3:].strip()
            elif ln.startswith("data:"):
                data_parts.append(ln[5:].strip())
        if data_parts:
            events.append((ev_type, ev_id, json.loads("\n".join(data_parts))))
    return events


class TestSilentFailures(unittest.IsolatedAsyncioTestCase):
    async def test_all_five_silent_failure_scenarios(self) -> None:
        async with running_test_server() as server:
            async with httpx.AsyncClient(base_url=server.base_url, timeout=10.0) as client:
                print("\n=== SILENT FAILURES VERIFICATION SUITE ===")

                # =====================================================================
                # 1. Events Lost on Reconnect (with Late/Out-of-Order Steps)
                # =====================================================================
                proj1 = (await client.post("/projects", json={"name": "sf-reconnect"})).json()
                headers1 = {"X-API-Key": proj1["api_key"]}
                run1 = (
                    await client.post("/runs", json={"name": "run-reconnect"}, headers=headers1)
                ).json()["run_id"]

                # Ingest initial high steps [10, 11, 12] -> seq 1, 2, 3
                r_init = await client.post(
                    f"/runs/{run1}/metrics",
                    json={
                        "batch_id": "b-init",
                        "metrics": [
                            {"step": 10, "loss": 1.0},
                            {"step": 11, "loss": 0.9},
                            {"step": 12, "loss": 0.8},
                        ],
                    },
                    headers=headers1,
                )
                self.assertEqual(r_init.json()["last_seq"], 3)

                # Client connects, receives seq 1..3, then disconnects
                received_first_leg = []
                async with client.stream(
                    "GET", f"/runs/{run1}/stream", headers=headers1
                ) as sse_resp:
                    buf = ""
                    async for chunk in sse_resp.aiter_text():
                        buf += chunk
                        if buf.count("event: metric") >= 3:
                            break
                    for ev_type, _, data in _parse_sse_events(buf):
                        if ev_type == "metric":
                            received_first_leg.append(data)

                self.assertEqual([m["seq"] for m in received_first_leg], [1, 2, 3])
                last_seen_seq = received_first_leg[-1]["seq"]

                # While client is offline, LATE steps [1, 2, 3] and NEW steps [13, 14] arrive
                await client.post(
                    f"/runs/{run1}/metrics",
                    json={
                        "batch_id": "b-late",
                        "metrics": [
                            {"step": 1, "loss": 2.1},
                            {"step": 2, "loss": 2.0},
                            {"step": 3, "loss": 1.9},
                            {"step": 13, "loss": 0.7},
                            {"step": 14, "loss": 0.6},
                        ],
                    },
                    headers=headers1,
                )

                # Client reconnects with Last-Event-ID: 3
                received_second_leg = []
                async with client.stream(
                    "GET",
                    f"/runs/{run1}/stream",
                    headers={**headers1, "Last-Event-ID": str(last_seen_seq)},
                ) as sse_resp2:
                    buf2 = ""
                    async for chunk in sse_resp2.aiter_text():
                        buf2 += chunk
                        if buf2.count("event: metric") >= 5:
                            break
                    for ev_type, _, data in _parse_sse_events(buf2):
                        if ev_type == "metric":
                            received_second_leg.append(data)

                all_received = received_first_leg + received_second_leg
                self.assertEqual([m["seq"] for m in all_received], [1, 2, 3, 4, 5, 6, 7, 8])
                self.assertEqual(
                    sorted(m["step"] for m in all_received), [1, 2, 3, 10, 11, 12, 13, 14]
                )
                print(
                    "[PASS] 1. Events lost on reconnect : 0 lost (seq 1..3 then catch-up seq 4..8 including late steps 1..3)"
                )

                # =====================================================================
                # 2. Duplicates (Concurrent Batch Retries & Overlapping Steps)
                # =====================================================================
                run2 = (
                    await client.post("/runs", json={"name": "run-dups"}, headers=headers1)
                ).json()["run_id"]

                dup_payload = {
                    "batch_id": "batch-concurrent-dup",
                    "metrics": [
                        {"step": 0, "loss": 2.5},
                        {"step": 1, "loss": 2.2},
                        {"step": 2, "loss": 1.9},
                    ],
                }
                # Fire 5 concurrent identical batch requests
                concurrent_resps = await asyncio.gather(
                    *[
                        client.post(f"/runs/{run2}/metrics", json=dup_payload, headers=headers1)
                        for _ in range(5)
                    ]
                )
                for r in concurrent_resps:
                    self.assertEqual(r.status_code, 200)
                total_inserted = sum(r.json()["inserted"] for r in concurrent_resps)
                self.assertEqual(total_inserted, 3, "Concurrent retry inserted duplicate rows!")

                # Send overlapping steps [1, 2, 3] under a new batch_id
                r_overlap = await client.post(
                    f"/runs/{run2}/metrics",
                    json={
                        "batch_id": "batch-overlap-2",
                        "metrics": [
                            {"step": 1, "loss": 2.2},
                            {"step": 2, "loss": 1.9},
                            {"step": 3, "loss": 1.6},
                        ],
                    },
                    headers=headers1,
                )
                self.assertEqual(r_overlap.status_code, 200)
                self.assertEqual(r_overlap.json()["inserted"], 1)
                self.assertEqual(r_overlap.json()["duplicates"], 2)
                self.assertEqual(r_overlap.json()["last_seq"], 4)

                db_metrics_2 = (
                    await client.get(f"/runs/{run2}/metrics", headers=headers1)
                ).json()["metrics"]
                self.assertEqual([m["step"] for m in db_metrics_2], [0, 1, 2, 3])
                self.assertEqual([m["seq"] for m in db_metrics_2], [1, 2, 3, 4])
                print(
                    "[PASS] 2. Duplicates               : 5x concurrent + overlapping batches -> 4 unique rows, gapless seq 1..4"
                )

                # =====================================================================
                # 3. Queue Overflow for a Slow Client
                # =====================================================================
                run3 = (
                    await client.post("/runs", json={"name": "run-slow-client"}, headers=headers1)
                ).json()["run_id"]

                # Seed 1 initial metric (seq=1) so initial SSE response flushes immediately
                await client.post(
                    f"/runs/{run3}/metrics",
                    json={"batch_id": "blast-seed", "metrics": [{"step": 0, "loss": 2.0}]},
                    headers=headers1,
                )

                # Connect a slow client with bounded queue_size=3
                overflow_seen = False
                partial_seqs = []
                async with client.stream(
                    "GET", f"/runs/{run3}/stream?queue_size=3", headers=headers1
                ) as slow_stream:
                    aiter = slow_stream.aiter_text()
                    first_buf = ""
                    async for ch in aiter:
                        first_buf += ch
                        if "event: metric" in first_buf:
                            break
                    for ev_type, _, data in _parse_sse_events(first_buf):
                        if ev_type == "metric":
                            partial_seqs.append(data["seq"])
                    self.assertEqual(partial_seqs, [1])

                    # Do NOT read from `slow_stream` while producer blasts a burst of 9 metrics
                    # in a single batch (> queue_size=3), overflowing the subscriber's queue
                    r_blast = await client.post(
                        f"/runs/{run3}/metrics",
                        json={
                            "batch_id": "blast-burst",
                            "metrics": [
                                {"step": s, "loss": round(2.0 / (s + 1), 6)}
                                for s in range(1, 10)
                            ],
                        },
                        headers=headers1,
                    )
                    self.assertEqual(r_blast.status_code, 200)
                    self.assertEqual(r_blast.json()["inserted"], 9)

                    # Now drain the slow stream: it must receive `event: overflow` and close
                    rem_buf = ""
                    async for chunk in aiter:
                        rem_buf += chunk
                    for ev_type, _, data in _parse_sse_events(rem_buf):
                        if ev_type == "metric":
                            partial_seqs.append(data["seq"])
                        elif ev_type == "overflow":
                            overflow_seen = True

                self.assertTrue(overflow_seen, "Slow client did not receive event: overflow!")
                stats = (await client.get("/_test/stats")).json()
                self.assertGreaterEqual(stats["overflow_drops"], 1)

                # Reconnect from last received seq and verify 100% catch-up of all 10 metrics
                resume_from = partial_seqs[-1] if partial_seqs else 0
                recovered_seqs = list(partial_seqs)
                async with client.stream(
                    "GET",
                    f"/runs/{run3}/stream",
                    headers={**headers1, "Last-Event-ID": str(resume_from)},
                ) as recovery_stream:
                    rec_buf = ""
                    async for chunk in recovery_stream.aiter_text():
                        rec_buf += chunk
                        if rec_buf.count("event: metric") >= (10 - len(partial_seqs)):
                            break
                    for ev_type, _, data in _parse_sse_events(rec_buf):
                        if ev_type == "metric":
                            recovered_seqs.append(data["seq"])

                self.assertEqual(recovered_seqs, list(range(1, 11)))
                print(
                    f"[PASS] 3. Slow client overflow     : Queue(maxsize=3) overflowed cleanly -> event: overflow -> recovered seq 1..10"
                )

                # =====================================================================
                # 4. Partial Writes (Atomic All-or-Nothing Rollback)
                # =====================================================================
                run4 = (
                    await client.post("/runs", json={"name": "run-atomic"}, headers=headers1)
                ).json()["run_id"]

                # Seed step 0 (seq=1)
                await client.post(
                    f"/runs/{run4}/metrics",
                    json={"batch_id": "seed-0", "metrics": [{"step": 0, "loss": 3.0}]},
                    headers=headers1,
                )

                # Case 4a: 4 valid points + 1 malformed point (negative step) at the end
                r_bad_point = await client.post(
                    f"/runs/{run4}/metrics",
                    json={
                        "batch_id": "partial-invalid",
                        "metrics": [
                            {"step": 1, "loss": 2.8},
                            {"step": 2, "loss": 2.6},
                            {"step": 3, "loss": 2.4},
                            {"step": 4, "loss": 2.2},
                            {"step": -1, "loss": 2.0},
                        ],
                    },
                    headers=headers1,
                )
                self.assertEqual(r_bad_point.status_code, 422)

                # Case 4b: 3 valid new points (steps 1, 2, 3) + 1 conflicting point (step 0 with different loss)
                r_conflict_point = await client.post(
                    f"/runs/{run4}/metrics",
                    json={
                        "batch_id": "partial-conflict",
                        "metrics": [
                            {"step": 1, "loss": 2.8},
                            {"step": 2, "loss": 2.6},
                            {"step": 3, "loss": 2.4},
                            {"step": 0, "loss": 999.999},
                        ],
                    },
                    headers=headers1,
                )
                self.assertEqual(r_conflict_point.status_code, 409)

                # Verify neither batch wrote ANY rows and `last_seq` remained 1
                check4 = (await client.get(f"/runs/{run4}/metrics", headers=headers1)).json()
                self.assertEqual(check4["last_seq"], 1)
                self.assertEqual(len(check4["metrics"]), 1)
                self.assertEqual(check4["metrics"][0]["step"], 0)
                self.assertAlmostEqual(check4["metrics"][0]["loss"], 3.0)
                print(
                    "[PASS] 4. Partial writes rollback  : 0 orphan rows persisted on 422 validation & 409 mid-batch conflict"
                )

                # =====================================================================
                # 5. Old API Key After Rotation (REST + Already-Open SSE Stream)
                # =====================================================================
                proj5 = (await client.post("/projects", json={"name": "sf-rotation"})).json()
                old_key = proj5["api_key"]
                proj5_id = proj5["project_id"]
                old_headers = {"X-API-Key": old_key}

                run5 = (
                    await client.post("/runs", json={"name": "run-key-rot"}, headers=old_headers)
                ).json()["run_id"]

                # Open live SSE stream with `old_key` BEFORE rotation
                async with client.stream(
                    "GET", f"/runs/{run5}/stream", headers=old_headers
                ) as open_sse:
                    sse_iter = open_sse.aiter_text()
                    banner = await sse_iter.__anext__()
                    self.assertIn(": connected", banner)

                    # Send 1 metric while old_key is still valid
                    await client.post(
                        f"/runs/{run5}/metrics",
                        json={"batch_id": "pre-rot", "metrics": [{"step": 0, "loss": 1.5}]},
                        headers=old_headers,
                    )
                    pre_chunk = await sse_iter.__anext__()
                    self.assertIn("event: metric", pre_chunk)

                    # Rotate the project API key
                    rot_resp = await client.post(
                        f"/projects/{proj5_id}/keys/rotate", headers=old_headers
                    )
                    self.assertEqual(rot_resp.status_code, 200)
                    rot_data = rot_resp.json()
                    new_key = rot_data["api_key"]
                    new_headers = {"X-API-Key": new_key}
                    self.assertGreaterEqual(rot_data["closed_streams"], 1)

                    # Old key must immediately fail REST read, write, and stream
                    self.assertEqual(
                        (await client.get("/runs", headers=old_headers)).status_code, 401
                    )
                    self.assertEqual(
                        (
                            await client.post(
                                f"/runs/{run5}/metrics",
                                json={"metrics": [{"step": 1, "loss": 1.1}]},
                                headers=old_headers,
                            )
                        ).status_code,
                        401,
                    )
                    self.assertEqual(
                        (await client.get(f"/runs/{run5}/stream", headers=old_headers)).status_code,
                        401,
                    )

                    # Ingest a new metric using `new_key`
                    r_new = await client.post(
                        f"/runs/{run5}/metrics",
                        json={"batch_id": "post-rot", "metrics": [{"step": 1, "loss": 1.1}]},
                        headers=new_headers,
                    )
                    self.assertEqual(r_new.status_code, 200)

                    # Drain the already-open old SSE stream: it must receive `api_key_revoked`
                    # and NEVER receive step=1
                    post_rot_buf = ""
                    async for ch in sse_iter:
                        post_rot_buf += ch

                    post_events = _parse_sse_events(post_rot_buf)
                    revoked_errors = [
                        d
                        for (t, _, d) in post_events
                        if t == "error" and d.get("reason") == "api_key_revoked"
                    ]
                    leaked_metrics = [d for (t, _, d) in post_events if t == "metric"]
                    self.assertEqual(len(revoked_errors), 1)
                    self.assertEqual(
                        len(leaked_metrics),
                        0,
                        f"Revoked SSE stream leaked post-rotation metrics: {leaked_metrics}",
                    )

                print(
                    "[PASS] 5. Old API key after rotate : REST -> 401; open SSE stream terminated with api_key_revoked (0 leaked events)"
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
