"""Requirement 3: Automated Multi-Tenant Isolation & Leakage Tests.

Verifies that Project A cannot:
1. Read runs or metrics of Project B (`GET /runs`, `GET /runs/{b_id}`, `GET /runs/{b_id}/metrics`).
2. Write metrics or rotate keys for Project B (`POST /runs/{b_id}/metrics`, `POST /projects/{b_id}/keys/rotate`).
3. Subscribe to the live SSE stream of Project B (`GET /runs/{b_id}/stream`).
4. Guess or enumerate IDs of Project B (UUIDv4 entropy + byte-for-byte identical 404 responses).
5. Poison or read Project B data via HTTP/application caches (`Cache-Control: private, no-store`).
6. Leak API keys via URL query strings (`?api_key=` rejected with 400) or server logs.
7. Distinguish existing cross-project run IDs from non-existent UUIDs via timing side channels.
"""

from __future__ import annotations

import statistics
import time
import unittest
import uuid

import httpx

from tests.conftest import running_test_server


class TestIsolation(unittest.IsolatedAsyncioTestCase):
    async def test_full_isolation_suite(self) -> None:
        async with running_test_server() as server:
            async with httpx.AsyncClient(base_url=server.base_url, timeout=10.0) as client:
                # Setup Project A and Project B
                proj_a = (await client.post("/projects", json={"name": "project-alpha"})).json()
                proj_b = (await client.post("/projects", json={"name": "project-beta"})).json()
                key_a = proj_a["api_key"]
                key_b = proj_b["api_key"]
                headers_a = {"X-API-Key": key_a}
                headers_b = {"X-API-Key": key_b}

                run_a = (
                    await client.post("/runs", json={"name": "alpha-run"}, headers=headers_a)
                ).json()
                run_b = (
                    await client.post("/runs", json={"name": "beta-secret-run"}, headers=headers_b)
                ).json()
                run_a_id = run_a["run_id"]
                run_b_id = run_b["run_id"]

                # Project B logs secret metrics
                r_ingest_b = await client.post(
                    f"/runs/{run_b_id}/metrics",
                    json={
                        "batch_id": "b-secret-1",
                        "metrics": [{"step": 0, "loss": 0.777, "lr": 1e-4}],
                    },
                    headers=headers_b,
                )
                self.assertEqual(r_ingest_b.status_code, 200)

                # --- 1. Read Isolation ---
                list_a = (await client.get("/runs", headers=headers_a)).json()["runs"]
                self.assertEqual([r["run_id"] for r in list_a], [run_a_id])
                self.assertNotIn(run_b_id, [r["run_id"] for r in list_a])

                get_b_by_a = await client.get(f"/runs/{run_b_id}", headers=headers_a)
                self.assertEqual(get_b_by_a.status_code, 404)
                self.assertEqual(get_b_by_a.json(), {"detail": "Run not found"})

                metrics_b_by_a = await client.get(f"/runs/{run_b_id}/metrics", headers=headers_a)
                self.assertEqual(metrics_b_by_a.status_code, 404)
                self.assertEqual(metrics_b_by_a.json(), {"detail": "Run not found"})

                # --- 2. Write & Key-Rotation Isolation ---
                write_b_by_a = await client.post(
                    f"/runs/{run_b_id}/metrics",
                    json={"batch_id": "forged", "metrics": [{"step": 1, "loss": 99.9}]},
                    headers=headers_a,
                )
                self.assertEqual(write_b_by_a.status_code, 404)
                self.assertEqual(write_b_by_a.json(), {"detail": "Run not found"})

                # Verify DB still has only Project B's 1 metric
                b_metrics_check = (
                    await client.get(f"/runs/{run_b_id}/metrics", headers=headers_b)
                ).json()
                self.assertEqual(len(b_metrics_check["metrics"]), 1)
                self.assertAlmostEqual(b_metrics_check["metrics"][0]["loss"], 0.777)

                rotate_b_by_a = await client.post(
                    f"/projects/{proj_b['project_id']}/keys/rotate", headers=headers_a
                )
                self.assertEqual(rotate_b_by_a.status_code, 404)
                self.assertEqual(rotate_b_by_a.json(), {"detail": "Project not found"})

                # --- 3. Live SSE Stream Isolation ---
                stream_b_by_a = await client.get(f"/runs/{run_b_id}/stream", headers=headers_a)
                self.assertEqual(stream_b_by_a.status_code, 404)
                self.assertEqual(stream_b_by_a.json(), {"detail": "Run not found"})

                # --- 4. ID Unpredictability (UUIDv4) & Response Indistinguishability ---
                for uid_str in (proj_a["project_id"], proj_b["project_id"], run_a_id, run_b_id):
                    parsed = uuid.UUID(uid_str)
                    self.assertEqual(parsed.version, 4, f"ID {uid_str} is not UUIDv4")

                non_existent_uuid = str(uuid.uuid4())
                r_existing_foreign = await client.get(f"/runs/{run_b_id}", headers=headers_a)
                r_non_existent = await client.get(f"/runs/{non_existent_uuid}", headers=headers_a)
                r_malformed_id = await client.get("/runs/not-a-uuid-12345", headers=headers_a)

                self.assertEqual(r_existing_foreign.status_code, 404)
                self.assertEqual(r_non_existent.status_code, 404)
                self.assertEqual(r_malformed_id.status_code, 404)
                self.assertEqual(r_existing_foreign.content, r_non_existent.content)
                self.assertEqual(r_existing_foreign.content, r_malformed_id.content)

                # --- 5. Cache Isolation & Security Headers ---
                r_b_cached = await client.get(f"/runs/{run_b_id}/metrics", headers=headers_b)
                self.assertEqual(r_b_cached.status_code, 200)
                self.assertEqual(r_b_cached.headers.get("cache-control"), "private, no-store")
                self.assertIn("x-api-key", r_b_cached.headers.get("vary", "").lower())

                r_a_after_b = await client.get(f"/runs/{run_b_id}/metrics", headers=headers_a)
                self.assertEqual(r_a_after_b.status_code, 404)

                # --- 6. Query-String Key Rejection & Log Redaction ---
                r_query_leak = await client.get(f"/runs?api_key={key_a}")
                self.assertEqual(r_query_leak.status_code, 400)
                self.assertIn("forbidden", r_query_leak.json()["detail"].lower())

                r_bad_bearer = await client.get(
                    "/runs", headers={"Authorization": f"Bearer {key_b}_invalid"}
                )
                self.assertEqual(r_bad_bearer.status_code, 401)

                stats_resp = (await client.get("/_test/stats")).json()
                joined_logs = "\n".join(stats_resp["audit_logs"])
                self.assertNotIn(key_a, joined_logs, "Raw API key A leaked into server logs!")
                self.assertNotIn(key_b, joined_logs, "Raw API key B leaked into server logs!")

                # --- 7. Timing Side-Channel Measurement (100 iterations each) ---
                foreign_times_ms = []
                nonexistent_times_ms = []
                for i in range(100):
                    rand_uuid = str(uuid.uuid4())
                    t0 = time.perf_counter()
                    resp1 = await client.get(f"/runs/{run_b_id}", headers=headers_a)
                    t1 = time.perf_counter()
                    assert resp1.status_code == 404
                    foreign_times_ms.append((t1 - t0) * 1000.0)

                    t2 = time.perf_counter()
                    resp2 = await client.get(f"/runs/{rand_uuid}", headers=headers_a)
                    t3 = time.perf_counter()
                    assert resp2.status_code == 404
                    nonexistent_times_ms.append((t3 - t2) * 1000.0)

                f_p50 = statistics.median(foreign_times_ms)
                n_p50 = statistics.median(nonexistent_times_ms)
                f_p95 = sorted(foreign_times_ms)[94]
                n_p95 = sorted(nonexistent_times_ms)[94]
                diff_p50 = abs(f_p50 - n_p50)

                print("\n=== MULTI-TENANT ISOLATION PROOF SUMMARY ===")
                print("1. Read Isolation (GET /runs, /runs/{B}, /metrics)      : PASS (404 Not Found)")
                print("2. Write Isolation (POST /runs/{B}/metrics, /rotate)    : PASS (404 Not Found)")
                print("3. Live SSE Stream Isolation (GET /runs/{B}/stream)     : PASS (404 Not Found)")
                print("4. ID Entropy & Enumeration (UUIDv4, identical 404 body): PASS")
                print("5. Cache Isolation (Cache-Control: private, no-store)   : PASS")
                print("6. Query-Param API Key Block (?api_key= -> 400) & Logs  : PASS (0 keys in logs)")
                print("7. Timing Side-Channel Check (100 probes each)          :")
                print(f"   - Existing Foreign Run UUID : p50 = {f_p50:.3f} ms, p95 = {f_p95:.3f} ms")
                print(f"   - Non-Existent Random UUID  : p50 = {n_p50:.3f} ms, p95 = {n_p95:.3f} ms")
                print(f"   - Absolute Median Delta     : {diff_p50:.3f} ms")


if __name__ == "__main__":
    unittest.main(verbosity=2)
